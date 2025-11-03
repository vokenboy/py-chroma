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
    student_id: str | None = None
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
    student_id = str(uuid.uuid4())
    metadata = student.metadata

    if not isinstance(metadata.get("study_year"), int):
        try:
            metadata["study_year"] = int(metadata.get("study_year"))
        except (ValueError, TypeError):
            raise HTTPException(status_code=400, detail="study_year must be an integer")

    target_server = detect_metadata_type(metadata)
    study_year = metadata.get("study_year")
    target_db = resolve_fragment(target_server, study_year)

    if target_server == "DBVS2":
        metadata["student_id"] = student_id

    if target_server == "DBVS1" and "timestamp" not in metadata:
        metadata["timestamp"] = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        client = get_client(target_server, target_db)
        collection = client.get_or_create_collection("students")
        collection.add(
            documents=[student.document],
            metadatas=[metadata],
            ids=[student_id],
        )
        return {"message": "Student inserted successfully", "student_id": student_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {str(e)}")


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
            metas = data.get("metadatas", [])
            docs = data.get("documents", [])

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



