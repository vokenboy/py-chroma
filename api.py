from fastapi import FastAPI, HTTPException, Response
from fastapi import Query
from pydantic import BaseModel
import chromadb
import uuid

SERVERS = [
    {"name": "DBVS1", "host": "localhost", "port": 8000},
    {"name": "DBVS2", "host": "localhost", "port": 8001},
]

TENANT = "tenant_user:user12"

FRAGMENTS = {
    "DBVS1": {
        "internal": {"database": "db11", "year_range": [1, 2]},
        "external": {"database": "db12", "year_range": [3, 4]},
    },
    "DBVS2": {
        "internal": {"database": "db21", "year_range": [1, 2]},
        "external": {"database": "db22", "year_range": [3, 4]},
    },
}

app = FastAPI()


class DBVS1Metadata(BaseModel):
    timestamp: str | None = None
    final_score: float
    study_year: int

    class Config:
        extra = "forbid"


class DBVS2Metadata(BaseModel):
    student_id: int | None = None
    name: str
    surname: str
    email: str
    study_year: int

    class Config:
        extra = "forbid"


class Student(BaseModel):
    document: str
    metadata: dict


class StudentUpdate(BaseModel):
    document: str | None = None
    metadata: dict


def get_server_by_name(name: str):
    for s in SERVERS:
        if s["name"].lower() == name.lower():
            return s
    raise HTTPException(status_code=400, detail=f"Server '{name}' not found.")


def get_client(server_name: str, db_name: str):
    server = get_server_by_name(server_name)
    return chromadb.HttpClient(
        tenant=TENANT,
        database=db_name,
        host=server["host"],
        port=server["port"],
    )


def resolve_fragment(server_name: str, study_year: int):
    fragments = FRAGMENTS[server_name]
    for frag_type, frag_info in fragments.items():
        if frag_info["year_range"][0] <= study_year <= frag_info["year_range"][1]:
            return frag_info["database"]
    raise HTTPException(
        status_code=400,
        detail=f"No fragment found for study_year {study_year} on {server_name}",
    )


def detect_metadata_type(metadata: dict):
    if "final_score" in metadata:
        DBVS1Metadata(**metadata)
        return "DBVS1"
    elif all(k in metadata for k in ["name", "surname", "email", "study_year"]):
        DBVS2Metadata(**metadata)
        return "DBVS2"
    else:
        raise HTTPException(status_code=400, detail="Invalid metadata structure.")


from datetime import datetime

from datetime import datetime

@app.post("/student")
def insert_student(student: Student):
    meta_in = dict(student.metadata or {})

    if not isinstance(meta_in.get("study_year"), int):
        try:
            meta_in["study_year"] = int(meta_in.get("study_year"))
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="study_year must be an integer")

    study_year = meta_in.get("study_year")
    if study_year not in [1, 2, 3, 4]:
        raise HTTPException(status_code=400, detail="study_year must be 1, 2, 3, or 4")

    if "final_score" not in meta_in:
        raise HTTPException(status_code=400, detail="Missing required field: final_score for DBVS1 metadata")

    dbvs1_meta = {
        "final_score": meta_in.get("final_score"),
        "study_year": study_year,
    }
    if "timestamp" in meta_in:
        dbvs1_meta["timestamp"] = meta_in["timestamp"]
    else:
        dbvs1_meta["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    DBVS1Metadata(**dbvs1_meta)

    missing_dbvs2 = [k for k in ["name", "surname", "email"] if k not in meta_in]
    if missing_dbvs2:
        raise HTTPException(status_code=400, detail=f"Missing required fields for DBVS2 metadata: {', '.join(missing_dbvs2)}")

    db_dbvs1 = resolve_fragment("DBVS1", study_year)
    db_dbvs2 = resolve_fragment("DBVS2", study_year)

    def _allocate_next_student_id():
        max_id = 0
        for server_name, frags in FRAGMENTS.items():
            for frag_info in frags.values():
                dbname = frag_info["database"]
                try:
                    tmp_client = get_client(server_name, dbname)
                    tmp_collection = tmp_client.get_or_create_collection("students")
                    data = tmp_collection.get(limit=None)
                except Exception:
                    continue
                for rid in data.get("ids", []) or []:
                    try:
                        val = int(rid)
                        if val > max_id:
                            max_id = val
                    except (ValueError, TypeError):
                        continue
        return max_id + 1

    try:
        student_id_int = _allocate_next_student_id()
        student_id = str(student_id_int)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to allocate new student id: {str(e)}")

    dbvs2_meta = {
        "student_id": student_id_int,
        "name": meta_in.get("name"),
        "surname": meta_in.get("surname"),
        "email": meta_in.get("email"),
        "study_year": study_year,
    }
    DBVS2Metadata(**dbvs2_meta)

    collection_dbvs1 = None
    try:
        client_dbvs1 = get_client("DBVS1", db_dbvs1)
        collection_dbvs1 = client_dbvs1.get_or_create_collection("students")
        collection_dbvs1.add(
            documents=[student.document],
            metadatas=[dbvs1_meta],
            ids=[student_id],
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert into DBVS1 failed: {str(e)}")

    try:
        client_dbvs2 = get_client("DBVS2", db_dbvs2)
        collection_dbvs2 = client_dbvs2.get_or_create_collection("students")
        collection_dbvs2.add(
            documents=[student.document],
            metadatas=[dbvs2_meta],
            ids=[student_id],
        )
    except Exception as e:
        try:
            if collection_dbvs1 is not None:
                collection_dbvs1.delete(ids=[student_id])
        except Exception as rollback_err:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"Insert into DBVS2 failed: {str(e)}; "
                    f"Rollback of DBVS1 also failed: {str(rollback_err)}"
                ),
            )
        raise HTTPException(status_code=500, detail=f"Insert into DBVS2 failed (rolled back DBVS1): {str(e)}")

    return {"message": "Student inserted successfully across DBVS1 and DBVS2", "student_id": student_id_int}


class _FailingAddCollection:
    def __init__(self, base):
        self._base = base

    def add(self, ids, documents, metadatas):
        raise RuntimeError("Simulated DBVS2 failure on add")

    def __getattr__(self, name):
        return getattr(self._base, name)


class _ClientProxy:
    def __init__(self, base, fail_on_add: bool = False):
        self._base = base
        self._fail_on_add = fail_on_add

    def get_or_create_collection(self, name):
        col = self._base.get_or_create_collection(name)
        if self._fail_on_add:
            return _FailingAddCollection(col)
        return col

    def get_collection(self, name):
        col = self._base.get_collection(name)
        if self._fail_on_add:
            return _FailingAddCollection(col)
        return col

    def __getattr__(self, name):
        return getattr(self._base, name)


@app.post("/_test/transaction/dbvs2_fail")
def test_insert_student_dbvs2_fail(student: Student):
    original_get_client = get_client

    def patched_get_client(server_name: str, db_name: str):
        base_client = original_get_client(server_name, db_name)
        return _ClientProxy(base_client, fail_on_add=(server_name == "DBVS2"))

    try:
        globals()["get_client"] = patched_get_client
        return insert_student(student)
    finally:
        globals()["get_client"] = original_get_client


@app.put("/student/{student_id}")
def update_student(student_id: str, update: StudentUpdate):
    metadata = update.metadata
    new_year = metadata.get("study_year")

    if new_year is not None:
        try:
            new_year = int(new_year)
        except (ValueError, TypeError):
            raise HTTPException(status_code=400)

        if new_year not in [1, 2, 3, 4]:
            raise HTTPException(status_code=400)

    moved = False

    for server_name, frags in FRAGMENTS.items():
        for frag_type, frag_info in frags.items():
            db_name = frag_info["database"]
            client = get_client(server_name, db_name)
            collection = client.get_or_create_collection("students")
            data = collection.get(limit=None)
            ids = data.get("ids", [])
            if student_id in ids:
                idx = ids.index(student_id)
                existing_metadata = metas[idx]
                document = docs[idx]
                existing_metadata.update(metadata)
                if update.document:
                    document = update.document

                new_db = resolve_fragment(server_name, new_year) if new_year else db_name

                if new_db == db_name:
                    collection.update(
                        ids=[student_id],
                        metadatas=[existing_metadata],
                        documents=[document],
                    )
                else:
                    new_client = get_client(server_name, new_db)
                    new_collection = new_client.get_or_create_collection("students")
                    new_collection.add(
                        documents=[document],
                        metadatas=[existing_metadata],
                        ids=[student_id],
                    )
                    collection.delete(ids=[student_id])

                moved = True
                break
        if moved:
            break

    if not moved:
        raise HTTPException(status_code=404, detail="Student not found in any database.")

    return Response(status_code=200)

@app.delete("/student/{student_id}")
def delete_student(student_id: str):
    deleted = False

    for server_name, frags in FRAGMENTS.items():
        for frag_type, frag_info in frags.items():
            db_name = frag_info["database"]
            client = get_client(server_name, db_name)
            collection = client.get_or_create_collection("students")

            data = collection.get(limit=None)
            ids = data.get("ids", [])

            if student_id in ids:
                try:
                    collection.delete(ids=[student_id])
                    deleted = True
                    break
                except Exception as e:
                    raise HTTPException(
                        status_code=500,
                        detail=f"Failed to delete student {student_id}: {str(e)}"
                    )
        if deleted:
            break

    if not deleted:
        raise HTTPException(
            status_code=404,
            detail=f"Student with ID '{student_id}' not found in any database."
        )

    return {"message": f"Student with ID '{student_id}' successfully deleted."}



@app.get("/support_ticket/{ticket_id}")
def find_related_document_to_policy(
    ticket_id: str,
    top_k: int = Query(5, description="Number of closest policy documents to return")
):
    source_server = "DBVS1"
    source_db = None
    source_doc = None
    found = False

    for frag_type, frag_info in FRAGMENTS[source_server].items():
        db_name = frag_info["database"]
        client = get_client(source_server, db_name)

        try:
            collection = client.get_collection("support_tickets")
        except Exception:
            continue

        data = collection.get(limit=None)
        ids = data.get("ids", [])
        docs = data.get("documents", [])

        if ticket_id in ids:
            idx = ids.index(ticket_id)
            source_doc = docs[idx]
            source_db = db_name
            found = True
            break

    if not found:
        raise HTTPException(status_code=404, detail=f"Ticket '{ticket_id}' not found in DBVS1.")

    target_server = "DBVS2"
    if source_db == "db11":
        target_db = "db21"
    elif source_db == "db12":
        target_db = "db22"
    else:
        raise HTTPException(status_code=400, detail=f"Invalid mapping for source DB '{source_db}'.")

    try:
        target_client = get_client(target_server, target_db)
        try:
            target_collection = target_client.get_collection("documents")
        except Exception:
            raise HTTPException(status_code=404, detail=f"'documents' collection not found in {target_db}.")

        query_result = target_collection.query(
            query_texts=[source_doc],
            n_results=top_k
        )

        docs_found = query_result.get("documents", [[]])[0]
        metas_found = query_result.get("metadatas", [[]])[0]
        ids_found = query_result.get("ids", [[]])[0]
        distances = query_result.get("distances", [[]])[0]

        documents = []
        for i in range(len(docs_found)):
            documents.append({
                "id": ids_found[i],
                "document": docs_found[i],
                "metadata": metas_found[i],
                "distance": distances[i]
            })

        return {
            "documents": documents
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Vector similarity query failed: {str(e)}")



@app.get("/students")
def get_all_students():
    aggregated = {"DBVS1": {}, "DBVS2": {}}

    for server_name, frags in FRAGMENTS.items():
        for frag_type, frag_info in frags.items():
            db_name = frag_info["database"]
            try:
                client = get_client(server_name, db_name)
                try:
                    collection = client.get_collection("students")
                except Exception:
                    continue

                data = collection.get(limit=None)
                ids = data.get("ids", [])
                docs = data.get("documents", [])
                metas = data.get("metadatas", [])

                for i, row_id in enumerate(ids):
                    meta = metas[i] if i < len(metas) else {}
                    doc = docs[i] if i < len(docs) else None

                    merge_id = row_id

                    if not merge_id:
                        continue

                    if merge_id not in aggregated[server_name]:
                        aggregated[server_name][merge_id] = {
                            "document": doc,
                            "metadata": meta if isinstance(meta, dict) else {},
                        }
            except HTTPException:
                raise
            except Exception:
                continue

    all_merge_ids = set(aggregated["DBVS1"].keys()) | set(aggregated["DBVS2"].keys())
    students = []
    for mid in all_merge_ids:
        dbvs1_entry = aggregated["DBVS1"].get(mid, {})
        dbvs2_entry = aggregated["DBVS2"].get(mid, {})

        document = dbvs2_entry.get("document") or dbvs1_entry.get("document")

        merged = {}
        if isinstance(dbvs2_entry.get("metadata"), dict):
            merged.update(dbvs2_entry.get("metadata") or {})
        if isinstance(dbvs1_entry.get("metadata"), dict):
            merged.update(dbvs1_entry.get("metadata") or {})

        fields = {
            "student_id": None,
            "name": None,
            "surname": None,
            "email": None,
            "final_score": None,
            "timestamp": None,
            "study_year": None,
        }

        for k in list(fields.keys()):
            if k in merged:
                fields[k] = merged.get(k)

        if fields["student_id"] is None:
            fields["student_id"] = mid

        students.append({
            "id": mid,
            "document": document,
            "metadata": fields,
        })

    return {"students": students}


@app.post("/student/{student_id}/upgrade")
def upgrade_student_year(student_id: int):
    sid = str(student_id)

    def locate_student(server_name: str):
        found = None
        for frag_type, frag_info in FRAGMENTS[server_name].items():
            db_name = frag_info["database"]
            client = get_client(server_name, db_name)
            try:
                collection = client.get_collection("students")
            except Exception:
                continue

            data = collection.get(limit=None)
            ids = data.get("ids", [])
            docs = data.get("documents", [])
            metas = data.get("metadatas", [])

            if sid in ids:
                idx = ids.index(sid)
                doc = docs[idx]
                meta = metas[idx] if idx < len(metas) and isinstance(metas[idx], dict) else {}
                return {
                    "db": db_name,
                    "doc": doc,
                    "meta": meta,
                }
        return None

    # Locate across both vertical fragments
    s1 = locate_student("DBVS1")
    s2 = locate_student("DBVS2")

    if not s1 or not s2:
        raise HTTPException(status_code=404, detail=f"Student '{sid}' not found in all vertical fragments")

    def to_int_year(v):
        try:
            return int(v)
        except Exception:
            return None

    y1 = to_int_year((s1["meta"] or {}).get("study_year"))
    y2 = to_int_year((s2["meta"] or {}).get("study_year"))

    if y1 is None or y2 is None:
        raise HTTPException(status_code=400, detail="study_year missing or invalid in metadata")

    if y1 != y2:
        raise HTTPException(status_code=409, detail="Inconsistent study_year across vertical fragments")

    current_year = y1
    if current_year >= 4:
        raise HTTPException(status_code=400, detail="Student is already at maximum study year (4)")

    new_year = current_year + 1

    # Helper to apply update/move atomically within a server (with internal compensation)
    def apply_on_server(server_name: str, doc: str, meta: dict):
        old_db = resolve_fragment(server_name, current_year)
        new_db = resolve_fragment(server_name, new_year)

        # Ensure clean metadata copy
        old_meta = dict(meta or {})
        new_meta = dict(old_meta)
        new_meta["study_year"] = new_year

        old_client = get_client(server_name, old_db)
        old_col = old_client.get_or_create_collection("students")

        if new_db == old_db:
            # In-place update
            try:
                old_col.update(ids=[sid], metadatas=[new_meta], documents=[doc])
                return {"action": "update", "server": server_name, "db": old_db, "prev_meta": old_meta}
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to update {server_name}:{old_db}: {e}")
        else:
            # Move across horizontal fragments (add to new, then delete old). Compensate if delete fails
            new_client = get_client(server_name, new_db)
            new_col = new_client.get_or_create_collection("students")
            try:
                new_col.add(ids=[sid], documents=[doc], metadatas=[new_meta])
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to add to {server_name}:{new_db}: {e}")
            try:
                old_col.delete(ids=[sid])
            except Exception as e:
                # Compensation: remove from new to revert
                try:
                    new_col.delete(ids=[sid])
                except Exception:
                    pass
                raise HTTPException(status_code=500, detail=f"Failed to delete from {server_name}:{old_db}: {e}")

            return {"action": "move", "server": server_name, "from": old_db, "to": new_db, "doc": doc, "prev_meta": old_meta}

    # Apply on DBVS1 first, then DBVS2. If second fails, rollback first.
    result1 = apply_on_server("DBVS1", s1["doc"], s1["meta"])
    try:
        result2 = apply_on_server("DBVS2", s2["doc"], s2["meta"])
    except HTTPException as err:
        # Rollback DBVS1
        try:
            if result1["action"] == "update":
                db = result1["db"]
                client = get_client("DBVS1", db)
                col = client.get_or_create_collection("students")
                col.update(ids=[sid], metadatas=[result1["prev_meta"]], documents=[s1["doc"]])
            elif result1["action"] == "move":
                # Move back: add to original, delete from new
                from_db = result1["from"]
                to_db = result1["to"]
                from_client = get_client("DBVS1", from_db)
                to_client = get_client("DBVS1", to_db)
                from_col = from_client.get_or_create_collection("students")
                to_col = to_client.get_or_create_collection("students")
                from_col.add(ids=[sid], documents=[result1["doc"]], metadatas=[result1["prev_meta"]])
                try:
                    to_col.delete(ids=[sid])
                except Exception:
                    pass
        except Exception:
            # If rollback fails, still return the original error to signal inconsistency
            pass
        raise err

    return {
        "message": "Student upgraded successfully",
        "student_id": sid,
        "previous_year": current_year,
        "new_year": new_year,
    }
