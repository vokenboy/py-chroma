from fastapi import FastAPI, HTTPException, Response
from fastapi import Query
from datetime import datetime
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


class CourseReviewCreate(BaseModel):
    text: str


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

    # Helps to add into fragments
    db_dbvs1 = resolve_fragment("DBVS1", study_year)
    db_dbvs2 = resolve_fragment("DBVS2", study_year)

    #Find max student_id across all fragments
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
    # Insert into dbvs1
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

    # Insert into dbvs2
    try:
        client_dbvs2 = get_client("DBVS2", db_dbvs2)
        collection_dbvs2 = client_dbvs2.get_or_create_collection("students")
        # Force error
        # raise RuntimeError("Forced DBVS2 failure for transactional rollback testing")
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
                    f"Insert into DBVS2:{db_dbvs2} failed: {str(e)}; "
                    f"Rollback of DBVS1 also failed: {str(rollback_err)}"
                ),
            )
        raise HTTPException(
            status_code=500,
            detail=f"Insert into DBVS2:{db_dbvs2} failed (rolled back DBVS1): {str(e)}",
        )

    return {"message": "Student inserted successfully across DBVS1 and DBVS2", "student_id": student_id_int}


@app.post("/course/{course_id}/review")
def add_course_review(course_id: int, payload: CourseReviewCreate):
    located_fragment = None
    course_id_str = str(course_id)
    for frag_type, frag_info in FRAGMENTS["DBVS2"].items():
        db_name = frag_info["database"]
        try:
            client = get_client("DBVS2", db_name)
            collection = client.get_collection("courses")
        except Exception:
            continue

        try:
            data = collection.get(ids=[course_id_str])
            ids = data.get("ids", []) or []
            if course_id_str in ids:
                located_fragment = frag_type  
                break
        except Exception:
            continue

    if not located_fragment:
        raise HTTPException(status_code=404, detail=f"Course '{course_id}' not found in DBVS2")

    target_dbvs1_db = FRAGMENTS["DBVS1"][located_fragment]["database"]

    # Prepare review payload
    review_id = str(uuid.uuid4())
    review_doc = payload.text
    review_meta = {
        "course_id": course_id,
        "timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    try:
        client_dbvs1 = get_client("DBVS1", target_dbvs1_db)
        col = client_dbvs1.get_or_create_collection("course_review")
        col.add(ids=[review_id], documents=[review_doc], metadatas=[review_meta])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to insert course review into DBVS1:{target_dbvs1_db}: {e}")

    return {
        "message": "Course review inserted",
        "course_id": course_id,
        "dbvs1_database": target_dbvs1_db,
        "review_id": review_id,
    }


@app.delete("/course/{course_id}")
def delete_course(course_id: str):
    source_db2 = None
    deleted_course = False
    course_id_str = str(course_id)

    saved_doc = None
    saved_meta = {}

    for frag_info in FRAGMENTS["DBVS2"].values():
        db_name = frag_info["database"]
        client = get_client("DBVS2", db_name)
        try:
            collection = client.get_collection("courses")
        except Exception:
            continue

        data = collection.get(limit=None)
        ids = data.get("ids", [])
        docs = data.get("documents", [])
        metas = data.get("metadatas", [])
        if course_id_str in ids:
            try:
                idx = ids.index(course_id_str)
                saved_doc = docs[idx] if idx < len(docs) else None
                saved_meta = metas[idx] if idx < len(metas) and isinstance(metas[idx], dict) else {}
                collection.delete(ids=[course_id_str])
                deleted_course = True
                source_db2 = db_name
                break
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Failed to delete course {course_id_str} from {db_name}: {str(e)}")

    if not deleted_course:
        raise HTTPException(status_code=404, detail=f"Course with ID '{course_id_str}' not found in DBVS2.")

    if source_db2 == "db21":
        peer_db1 = "db11"
    elif source_db2 == "db22":
        peer_db1 = "db12"
    else:
        raise HTTPException(status_code=400, detail=f"Unknown DBVS2 database '{source_db2}'.")

    exam_deleted = False
    saved_exam_doc = None
    saved_exam_meta = {}
    try:
        exam_client = get_client("DBVS2", source_db2)
    except Exception:
        exam_client = None

    if exam_client is not None:
        try:
            exam_collection = exam_client.get_collection("exams")
        except Exception:
            exam_collection = None

        if exam_collection is not None:
            try:
                exam_data = exam_collection.get(ids=[course_id_str])
                exam_ids = exam_data.get("ids", []) or []
                if course_id_str in exam_ids:
                    idx = exam_ids.index(course_id_str)
                    docs = exam_data.get("documents", []) or []
                    metas = exam_data.get("metadatas", []) or []
                    saved_exam_doc = docs[idx] if idx < len(docs) else None
                    saved_exam_meta = metas[idx] if idx < len(metas) and isinstance(metas[idx], dict) else {}
                    exam_collection.delete(ids=[course_id_str])
                    exam_deleted = True
            except Exception as exam_err:
                restore_error = None
                try:
                    restore_collection = exam_client.get_or_create_collection("courses")
                    restore_collection.add(
                        ids=[course_id_str],
                        documents=[saved_doc if isinstance(saved_doc, str) else (saved_doc or "")],
                        metadatas=[saved_meta if isinstance(saved_meta, dict) else {}],
                    )
                except Exception as re:
                    restore_error = str(re)
                if restore_error:
                    raise HTTPException(
                        status_code=500,
                        detail=(
                            f"Failed to delete related exam {course_id_str} in {source_db2}: {str(exam_err)}; "
                            f"also failed to restore course {course_id_str} in {source_db2}: {restore_error}"
                        ),
                    )
                raise HTTPException(
                    status_code=500,
                    detail=(
                        f"Failed to delete related exam {course_id_str} in {source_db2}: {str(exam_err)}; "
                        f"course deletion rolled back in {source_db2}"
                    ),
                )

    deleted_reviews = 0
    try:
        client_db1 = get_client("DBVS1", peer_db1)
        try:
            review_collection = client_db1.get_collection("course_review")
        except Exception:
            try:
                review_collection = client_db1.get_collection("course_reviews")
            except Exception:
                review_collection = None

        ids_to_delete = []
        if review_collection is not None:
            data = review_collection.get(limit=None)
            ids = data.get("ids", [])
            metas = data.get("metadatas", [])

            for i, rid in enumerate(ids):
                meta = metas[i] if i < len(metas) else {}
                course_meta_id = None
                if isinstance(meta, dict) and "course_id" in meta:
                    course_meta_id = meta.get("course_id")
                if course_meta_id is None:
                    continue

                try:
                    if str(course_meta_id) == course_id_str:
                        ids_to_delete.append(rid)
                except Exception:
                    continue

        if ids_to_delete:
            # Uncomment to simulate failure during review deletion and test rollback logic.
            raise RuntimeError("Forced DBVS1 review deletion failure for rollback testing")
            review_collection.delete(ids=ids_to_delete)
            deleted_reviews = len(ids_to_delete)
    except HTTPException:
        raise
    except Exception as e:
        restore_course_error = None
        restore_exam_error = None
        restore_client = None
        try:
            restore_client = get_client("DBVS2", source_db2)
        except Exception as re_client:
            restore_course_error = str(re_client)
            if exam_deleted:
                restore_exam_error = str(re_client)

        if restore_client is not None:
            try:
                restore_collection = restore_client.get_or_create_collection("courses")
                restore_collection.add(
                    ids=[course_id_str],
                    documents=[saved_doc if isinstance(saved_doc, str) else (saved_doc or "")],
                    metadatas=[saved_meta if isinstance(saved_meta, dict) else {}],
                )
            except Exception as re_course:
                restore_course_error = str(re_course)

            if exam_deleted:
                try:
                    restore_exam_collection = restore_client.get_or_create_collection("exams")
                    restore_exam_collection.add(
                        ids=[course_id_str],
                        documents=[saved_exam_doc if isinstance(saved_exam_doc, str) else (saved_exam_doc or "")],
                        metadatas=[saved_exam_meta if isinstance(saved_exam_meta, dict) else {}],
                    )
                except Exception as re_exam:
                    restore_exam_error = str(re_exam)

        failure_parts = []
        if restore_course_error:
            failure_parts.append(f"course {course_id_str} in {source_db2}: {restore_course_error}")
        if exam_deleted and restore_exam_error:
            failure_parts.append(f"exam {course_id_str} in {source_db2}: {restore_exam_error}")

        if failure_parts:
            raise HTTPException(
                status_code=500,
                detail=(
                    f"Failed to delete related course_review items in {peer_db1}: {str(e)}; "
                    f"also failed to restore {' and '.join(failure_parts)}"
                ),
            )
        raise HTTPException(
            status_code=500,
            detail=(
                f"Failed to delete related course_review items in {peer_db1}: {str(e)}; "
                f"course deletion rolled back in {source_db2}"
            ),
        )

    return {
        "message": "Course deleted successfully",
        "course_id": course_id_str,
        "source_db": source_db2,
        "deleted_course_reviews": deleted_reviews,
        "peer_db": peer_db1,
    }


class MoveCourseRequest(BaseModel):
    target_db: str


@app.post("/course/{course_id}/move")
def move_course(course_id: str, req: MoveCourseRequest):
    target_db2 = req.target_db.strip().lower()
    if target_db2 not in ("db21", "db22"):
        raise HTTPException(status_code=400, detail="target_db must be 'db21' or 'db22'")

    course_id_str = str(course_id)

    # Find course in DBVS2
    source_db2 = None
    src_course_doc = None
    src_course_meta = None
    src_course_collection = None
    for frag_info in FRAGMENTS["DBVS2"].values():
        db_name = frag_info["database"]
        client = get_client("DBVS2", db_name)
        try:
            col = client.get_collection("courses")
        except Exception:
            continue
        data = col.get(limit=None)
        ids = data.get("ids", [])
        if course_id_str in ids:
            idx = ids.index(course_id_str)
            docs = data.get("documents", [])
            metas = data.get("metadatas", [])
            src_course_doc = docs[idx] if idx < len(docs) else None
            src_course_meta = metas[idx] if idx < len(metas) else {}
            source_db2 = db_name
            src_course_collection = col
            break

    if source_db2 is None:
        raise HTTPException(status_code=404, detail=f"Course {course_id_str} not found in DBVS2")

    if source_db2 == target_db2:
        return {"message": "Course already in target fragment", "course_id": course_id_str, "db": target_db2}

    # Extract linked ids from course metadata
    exam_id = src_course_meta.get("exam_id")
    program_id = src_course_meta.get("program_id")

    # Helper to get one row by id from a collection
    def _get_row(client, collection_name, rid):
        try:
            col = client.get_collection(collection_name)
        except Exception:
            return None, None, None
        data = col.get(limit=None)
        ids = data.get("ids", [])
        if str(rid) in ids:
            idx = ids.index(str(rid))
            docs = data.get("documents", [])
            metas = data.get("metadatas", [])
            return col, docs[idx] if idx < len(docs) else None, metas[idx] if idx < len(metas) else {}
        return col, None, None

    # Save source items (for rollback)
    src_client2 = get_client("DBVS2", source_db2)
    tgt_client2 = get_client("DBVS2", target_db2)

    # Exams
    src_exam_col, src_exam_doc, src_exam_meta = (None, None, None)
    if exam_id is not None:
        src_exam_col, src_exam_doc, src_exam_meta = _get_row(src_client2, "exams", exam_id)

    # Programs
    src_prog_col, src_prog_doc, src_prog_meta = (None, None, None)
    if program_id is not None:
        src_prog_col, src_prog_doc, src_prog_meta = _get_row(src_client2, "programs", program_id)

    # Perform moves in DBVS2: course -> exams -> programs
    moved = {"course": False, "exam": False, "program": False}
    try:
        # Move course
        tgt_course_col = tgt_client2.get_or_create_collection("courses")
        try:
            tgt_course_col.add(ids=[course_id_str], documents=[src_course_doc or ""], metadatas=[src_course_meta or {}])
        except Exception:
            # id may exist; try update
            tgt_course_col.update(ids=[course_id_str], documents=[src_course_doc or ""], metadatas=[src_course_meta or {}])
        src_course_collection.delete(ids=[course_id_str])
        moved["course"] = True

        # Move exam
        if exam_id is not None and src_exam_col is not None and src_exam_doc is not None and src_exam_meta is not None:
            tgt_exam_col = tgt_client2.get_or_create_collection("exams")
            try:
                tgt_exam_col.add(ids=[str(exam_id)], documents=[src_exam_doc or ""], metadatas=[src_exam_meta or {}])
            except Exception:
                tgt_exam_col.update(ids=[str(exam_id)], documents=[src_exam_doc or ""], metadatas=[src_exam_meta or {}])
            src_exam_col.delete(ids=[str(exam_id)])
            moved["exam"] = True

        # Move program
        if program_id is not None and src_prog_col is not None and src_prog_doc is not None and src_prog_meta is not None:
            tgt_prog_col = tgt_client2.get_or_create_collection("programs")
            try:
                tgt_prog_col.add(ids=[str(program_id)], documents=[src_prog_doc or ""], metadatas=[src_prog_meta or {}])
            except Exception:
                tgt_prog_col.update(ids=[str(program_id)], documents=[src_prog_doc or ""], metadatas=[src_prog_meta or {}])
            src_prog_col.delete(ids=[str(program_id)])
            moved["program"] = True
    except Exception as e:
        # Rollback within DBVS2
        try:
            if moved.get("program"):
                # Move program back
                try:
                    src_prog_col.add(ids=[str(program_id)], documents=[src_prog_doc or ""], metadatas=[src_prog_meta or {}])
                except Exception:
                    src_prog_col.update(ids=[str(program_id)], documents=[src_prog_doc or ""], metadatas=[src_prog_meta or {}])
                try:
                    tgt_client2.get_or_create_collection("programs").delete(ids=[str(program_id)])
                except Exception:
                    pass
            if moved.get("exam"):
                try:
                    src_exam_col.add(ids=[str(exam_id)], documents=[src_exam_doc or ""], metadatas=[src_exam_meta or {}])
                except Exception:
                    src_exam_col.update(ids=[str(exam_id)], documents=[src_exam_doc or ""], metadatas=[src_exam_meta or {}])
                try:
                    tgt_client2.get_or_create_collection("exams").delete(ids=[str(exam_id)])
                except Exception:
                    pass
            if moved.get("course"):
                try:
                    src_course_collection.add(ids=[course_id_str], documents=[src_course_doc or ""], metadatas=[src_course_meta or {}])
                except Exception:
                    src_course_collection.update(ids=[course_id_str], documents=[src_course_doc or ""], metadatas=[src_course_meta or {}])
                try:
                    tgt_course_col.delete(ids=[course_id_str])
                except Exception:
                    pass
        except Exception as rb_err:
            raise HTTPException(status_code=500, detail=f"Failed moving course within DBVS2: {str(e)}; rollback failed: {str(rb_err)}")
        raise HTTPException(status_code=500, detail=f"Failed moving course within DBVS2: {str(e)}")

    # Now move course_reviews across DBVS1
    src_db1 = "db11" if source_db2 == "db21" else "db12"
    tgt_db1 = "db11" if target_db2 == "db21" else "db12"
    moved_review_ids = []
    try:
        src_client1 = get_client("DBVS1", src_db1)
        tgt_client1 = get_client("DBVS1", tgt_db1)
        try:
            src_rev_col = src_client1.get_collection("course_review")
        except Exception:
            src_rev_col = None
        if src_rev_col is not None:
            data = src_rev_col.get(limit=None)
            ids = data.get("ids", [])
            docs = data.get("documents", [])
            metas = data.get("metadatas", [])
            tgt_rev_col = tgt_client1.get_or_create_collection("course_review")
            # Move matching by metadata.course_id == course_id
            for i, rid in enumerate(ids):
                meta = metas[i] if i < len(metas) else {}
                if isinstance(meta, dict) and str(meta.get("course_id")) == course_id_str:
                    doc = docs[i] if i < len(docs) else None
                    try:
                        tgt_rev_col.add(ids=[rid], documents=[doc or ""], metadatas=[meta or {}])
                    except Exception:
                        tgt_rev_col.update(ids=[rid], documents=[doc or ""], metadatas=[meta or {}])
                    moved_review_ids.append(rid)
            # After adds/updates, delete from source
            if moved_review_ids:
                src_rev_col.delete(ids=moved_review_ids)
    except Exception as e:
        # Rollback entire DBVS2 move if review move fails
        try:
            # Move program back
            if program_id is not None and src_prog_col is not None and src_prog_doc is not None and src_prog_meta is not None:
                try:
                    src_prog_col.add(ids=[str(program_id)], documents=[src_prog_doc or ""], metadatas=[src_prog_meta or {}])
                except Exception:
                    src_prog_col.update(ids=[str(program_id)], documents=[src_prog_doc or ""], metadatas=[src_prog_meta or {}])
                try:
                    tgt_client2.get_or_create_collection("programs").delete(ids=[str(program_id)])
                except Exception:
                    pass
            # Move exam back
            if exam_id is not None and src_exam_col is not None and src_exam_doc is not None and src_exam_meta is not None:
                try:
                    src_exam_col.add(ids=[str(exam_id)], documents=[src_exam_doc or ""], metadatas=[src_exam_meta or {}])
                except Exception:
                    src_exam_col.update(ids=[str(exam_id)], documents=[src_exam_doc or ""], metadatas=[src_exam_meta or {}])
                try:
                    tgt_client2.get_or_create_collection("exams").delete(ids=[str(exam_id)])
                except Exception:
                    pass
            # Move course back
            try:
                src_course_collection.add(ids=[course_id_str], documents=[src_course_doc or ""], metadatas=[src_course_meta or {}])
            except Exception:
                src_course_collection.update(ids=[course_id_str], documents=[src_course_doc or ""], metadatas=[src_course_meta or {}])
            try:
                tgt_client2.get_or_create_collection("courses").delete(ids=[course_id_str])
            except Exception:
                pass
            # Cleanup any partially added reviews in target
            if moved_review_ids:
                try:
                    tgt_client1.get_or_create_collection("course_review").delete(ids=moved_review_ids)
                except Exception:
                    pass
        except Exception as rb_err:
            raise HTTPException(status_code=500, detail=f"Failed moving course_reviews: {str(e)}; rollback failed: {str(rb_err)}")
        raise HTTPException(status_code=500, detail=f"Failed moving course_reviews: {str(e)}")

    return {
        "message": "Course moved successfully",
        "course_id": course_id_str,
        "from_dbvs2": source_db2,
        "to_dbvs2": target_db2,
        "moved_reviews": len(moved_review_ids),
        "from_dbvs1": src_db1,
        "to_dbvs1": tgt_db1,
    }


@app.post("/course/{course_id}/upgrade")
def upgrade_course(course_id: str):
    """
    Body-less convenience endpoint that moves course from db21 -> db22,
    including linked exam/program and related course_review entries.
    If already in db22, returns a no-op message.
    """
    course_id_str = str(course_id)
    current_db2 = None
    for frag_info in FRAGMENTS["DBVS2"].values():
        db_name = frag_info["database"]
        client = get_client("DBVS2", db_name)
        try:
            col = client.get_collection("courses")
        except Exception:
            continue
        data = col.get(limit=None)
        if course_id_str in (data.get("ids", []) or []):
            current_db2 = db_name
            break

    if current_db2 is None:
        raise HTTPException(status_code=404, detail=f"Course {course_id_str} not found in DBVS2")

    if current_db2 == "db22":
        return {"message": "Course already at upper level (db22)", "course_id": course_id_str, "db": current_db2}

    # Move from db21 -> db22
    return move_course(course_id, MoveCourseRequest(target_db="db22"))


@app.delete("/student/{student_id}")
def delete_student(student_id: int):
    sid = str(student_id)

    def locate_student(server_name: str):
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
                return {"db": db_name, "doc": doc, "meta": meta}
        return None

    # Require presence in both vertical fragments to ensure sync
    s1 = locate_student("DBVS1")
    s2 = locate_student("DBVS2")

    if not s1 or not s2:
        raise HTTPException(status_code=404, detail=f"Student '{sid}' not found in all vertical fragments")

    # Delete in DBVS1 then DBVS2; rollback DBVS1 if DBVS2 fails
    try:
        c1 = get_client("DBVS1", s1["db"])\
            .get_or_create_collection("students")
        c1.delete(ids=[sid])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to delete from DBVS1:{s1['db']}: {e}")

    try:
        c2 = get_client("DBVS2", s2["db"])\
            .get_or_create_collection("students")
        c2.delete(ids=[sid])
    except Exception as e:
        # Rollback DBVS1 deletion
        try:
            c1.add(ids=[sid], documents=[s1["doc"]], metadatas=[s1["meta"]])
        except Exception:
            pass
        raise HTTPException(status_code=500, detail=f"Failed to delete from DBVS2:{s2['db']}: {e}")

    return {"message": "Student deleted successfully", "student_id": sid}



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

        # Collect documents from both vertical fragments
        external_document = dbvs1_entry.get("document")
        internal_document = dbvs2_entry.get("document") 

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
            "review": external_document,
            "motivational_letter": internal_document,
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
