from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import chromadb

# ===================================
# CONFIGURATION
# ===================================
SERVERS = [
    {"name": "DBVS1", "host": "localhost", "port": 8000},
    {"name": "DBVS2", "host": "localhost", "port": 8001},
]

TENANT = "tenant_user:user12"

# Fragment rules for both servers
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

app = FastAPI(title="Smart Student Routing API", version="4.2")


# ===================================
# HELPERS
# ===================================
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


def classify_target_server(metadata: dict) -> str:
    """Choose which server to insert into based on metadata content."""
    meta_keys = metadata.keys()
    if any(k in meta_keys for k in ["student_id", "name", "surname", "email"]):
        return "DBVS2"  # personal info → internal system
    if any(k in meta_keys for k in ["final_score", "timestamp"]):
        return "DBVS1"  # academic data → external system
    raise HTTPException(status_code=400, detail="Metadata does not match known server rules.")


def resolve_fragment(server_name: str, study_year: int):
    """Find the fragment on that server for the student's study_year."""
    fragments = FRAGMENTS[server_name]
    for frag_type, frag_info in fragments.items():
        if frag_info["year_range"][0] <= study_year <= frag_info["year_range"][1]:
            return frag_info["database"]
    raise HTTPException(status_code=400, detail=f"No fragment found for study_year {study_year} on {server_name}")


# ===================================
# MODELS
# ===================================
class Student(BaseModel):
    id: str
    document: str
    metadata: dict


# ===================================
# ENDPOINTS
# ===================================

@app.post("/insert")
def insert_student(student: Student):
    """Insert student into the correct server and DB based on metadata + study_year."""
    study_year = int(student.metadata.get("study_year"))
    target_server = classify_target_server(student.metadata)
    target_db = resolve_fragment(target_server, study_year)

    print(f"INSERT student {student.id} (year {study_year}) → {target_server}/{target_db}")

    try:
        client = get_client(target_server, target_db)
        collection = client.get_or_create_collection("students")
        collection.add(
            documents=[student.document],
            metadatas=[student.metadata],
            ids=[student.id],
        )
        return {
            "status": "success",
            "inserted_in": f"{target_server}/{target_db}",
            "student_id": student.id,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Insert failed: {str(e)}")


@app.get("/select-year-range")
def select_students(start_year: int, end_year: int):
    """Select students from all servers for a year range."""
    results = []
    total = 0

    try:
        for server_name, config in FRAGMENTS.items():
            for frag_type, frag in config.items():
                db_name = frag["database"]
                low, high = frag["year_range"]

                if high < start_year or low > end_year:
                    continue

                client = get_client(server_name, db_name)
                collection = client.get_or_create_collection("students")
                data = collection.get()

                docs = data.get("documents", [])
                metas = data.get("metadatas", [])
                ids = data.get("ids", [])

                for i, meta in enumerate(metas):
                    year = int(meta.get("study_year", 0))
                    if start_year <= year <= end_year:
                        results.append({
                            "id": ids[i],
                            "document": docs[i],
                            "metadata": meta,
                            "source": f"{server_name}/{db_name}"
                        })
                        total += 1

        return {
            "status": "success",
            "range": f"{start_year}-{end_year}",
            "count": total,
            "records": results
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Select failed: {str(e)}")


@app.get("/")
def root():
    return {
        "message": "Smart Student Routing API running",
        "routing_rules": {
            "DBVS1": "final_score / timestamp → academic (external)",
            "DBVS2": "student_id / name / surname → personal (internal)",
        },
        "fragmentation": {
            "1–2 years": "internal (db11 / db21)",
            "3–4 years": "external (db12 / db22)",
        },
        "example": {
            "DBVS1 example": {
                "document": "Follow-up application letter",
                "metadata": {
                    "final_score": 7.13,
                    "timestamp": "2025-01-15T11:20:00Z",
                    "study_year": 3
                }
            },
            "DBVS2 example": {
                "document": "First-year computer engineering student...",
                "metadata": {
                    "student_id": "b17a24e3-4e86-4b71-bd8c-5e7c83a908b9",
                    "name": "Ella",
                    "surname": "Harris",
                    "email": "ella.harris@example.com",
                    "study_year": 1
                }
            }
        }
    }