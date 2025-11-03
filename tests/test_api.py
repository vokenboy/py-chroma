import pytest
from fastapi.testclient import TestClient

# Import the app and function to patch
import api as api_module


class InMemoryCollection:
    def __init__(self, name, backing):
        self.name = name
        # backing: dict id -> {"document": str, "metadata": dict}
        self._store = backing

    def add(self, ids, documents, metadatas):
        for i, rid in enumerate(ids):
            doc = documents[i] if i < len(documents) else None
            meta = metadatas[i] if i < len(metadatas) else {}
            self._store[str(rid)] = {"document": doc, "metadata": dict(meta or {})}

    def get(self, limit=None, ids=None):
        out_ids, docs, metas = [], [], []
        if ids is not None:
            for rid in ids:
                rid = str(rid)
                if rid in self._store:
                    out_ids.append(rid)
                    docs.append(self._store[rid]["document"])
                    metas.append(self._store[rid]["metadata"])
        else:
            for rid, row in self._store.items():
                out_ids.append(rid)
                docs.append(row["document"])
                metas.append(row["metadata"])
                if isinstance(limit, int) and limit is not None and len(out_ids) >= limit:
                    break
        return {"ids": out_ids, "documents": docs, "metadatas": metas}

    def delete(self, ids):
        for rid in ids:
            self._store.pop(str(rid), None)

    def update(self, ids, documents=None, metadatas=None):
        for i, rid in enumerate(ids):
            rid = str(rid)
            if rid not in self._store:
                # Simulate error similar to real client
                raise RuntimeError(f"id {rid} not found")
            if documents is not None and i < len(documents):
                self._store[rid]["document"] = documents[i]
            if metadatas is not None and i < len(metadatas):
                self._store[rid]["metadata"] = dict(metadatas[i] or {})

    # Minimal similarity that returns first n docs deterministically
    def query(self, query_texts, n_results=5):
        ids = list(self._store.keys())[:n_results]
        docs = [self._store[i]["document"] for i in ids]
        metas = [self._store[i]["metadata"] for i in ids]
        dists = [0.0 for _ in ids]
        return {
            "ids": [ids],
            "documents": [docs],
            "metadatas": [metas],
            "distances": [dists],
        }


class InMemoryHttpClient:
    # Global registry across all client instances
    _registry = {}

    def __init__(self, server_name, database):
        self.server_name = server_name
        self.database = database
        key = (server_name, database)
        if key not in self._registry:
            self._registry[key] = {}

    @classmethod
    def reset_all(cls):
        cls._registry = {}

    def list_collections(self):
        return [
            {"name": name} for name in self._registry[(self.server_name, self.database)].keys()
        ]

    def delete_collection(self, name):
        self._registry[(self.server_name, self.database)].pop(name, None)

    def get_or_create_collection(self, name):
        key = (self.server_name, self.database)
        if name not in self._registry[key]:
            self._registry[key][name] = {}
        return InMemoryCollection(name, self._registry[key][name])

    def get_collection(self, name):
        key = (self.server_name, self.database)
        if name not in self._registry[key]:
            raise RuntimeError(f"Collection '{name}' not found")
        return InMemoryCollection(name, self._registry[key][name])


@pytest.fixture(autouse=True)
def reset_registry():
    InMemoryHttpClient.reset_all()
    yield


@pytest.fixture()
def test_client(monkeypatch):
    # Patch api.get_client to our in-memory client
    def fake_get_client(server_name: str, db_name: str):
        return InMemoryHttpClient(server_name, db_name)

    monkeypatch.setattr(api_module, "get_client", fake_get_client)
    return TestClient(api_module.app)


def seed_student(client: InMemoryHttpClient, sid: str, doc: str, meta: dict):
    col = client.get_or_create_collection("students")
    col.add(ids=[sid], documents=[doc], metadatas=[meta])


def seed_course(client: InMemoryHttpClient, cid: str, doc: str, meta: dict):
    col = client.get_or_create_collection("courses")
    col.add(ids=[cid], documents=[doc], metadatas=[meta])


def seed_review(client: InMemoryHttpClient, rid: str, doc: str, meta: dict):
    col = client.get_or_create_collection("course_review")
    col.add(ids=[rid], documents=[doc], metadatas=[meta])


def get_collection_store(server, db, name):
    key = (server, db)
    return InMemoryHttpClient._registry.get(key, {}).get(name, {})


def test_insert_student_success(test_client):
    body = {
        "document": "John Smith profile",
        "metadata": {
            "name": "John",
            "surname": "Smith",
            "email": "john@example.com",
            "final_score": 9.5,
            "study_year": 2,
        },
    }
    resp = test_client.post("/student", json=body)
    assert resp.status_code == 200, resp.text
    sid = str(resp.json()["student_id"])

    # Present in DBVS1:db11 and DBVS2:db21
    s1 = get_collection_store("DBVS1", "db11", "students")
    s2 = get_collection_store("DBVS2", "db21", "students")
    assert sid in s1
    assert sid in s2


def test_insert_student_dbvs2_failure_rolls_back_dbvs1(test_client, monkeypatch):
    # Force DBVS2 add to raise
    original_get_client = api_module.get_client

    def failing_dbvs2_client(server_name, db_name):
        base = InMemoryHttpClient(server_name, db_name)
        if server_name == "DBVS2":
            # Wrap the collection to raise on add
            class FailingAdd(InMemoryCollection):
                def add(self, ids, documents, metadatas):
                    raise RuntimeError("Simulated DBVS2 failure on add")

            class Proxy(InMemoryHttpClient):
                def get_or_create_collection(self, name):
                    col = super().get_or_create_collection(name)
                    return FailingAdd(col.name, col._store)

                def get_collection(self, name):
                    col = super().get_collection(name)
                    return FailingAdd(col.name, col._store)

            return Proxy(server_name, db_name)
        return base

    monkeypatch.setattr(api_module, "get_client", failing_dbvs2_client)

    body = {
        "document": "Jane Doe profile",
        "metadata": {
            "name": "Jane",
            "surname": "Doe",
            "email": "jane@example.com",
            "final_score": 8.7,
            "study_year": 1,
        },
    }
    resp = test_client.post("/student", json=body)
    assert resp.status_code == 500

    # Ensure rollback in DBVS1
    s1 = get_collection_store("DBVS1", "db11", "students")
    assert len(s1) == 0


def test_add_course_review_routes_to_correct_dbvs1_fragment(test_client):
    # Seed course id=5 into DBVS2:db21 (years 1-2 -> DBVS1:db11)
    c_db2 = InMemoryHttpClient("DBVS2", "db21")
    seed_course(c_db2, "5", "Database Systems", {"name": "Database Systems"})

    resp = test_client.post("/course/5/review", json={"text": "Great course!"})
    assert resp.status_code == 200, resp.text
    db_inserted = resp.json()["dbvs1_database"]
    assert db_inserted == "db11"

    reviews_store = get_collection_store("DBVS1", "db11", "course_review")
    assert len(reviews_store) == 1
    stored = list(reviews_store.values())[0]
    assert stored["metadata"]["course_id"] == 5
    assert stored["document"] == "Great course!"


def test_upgrade_student_year_moves_across_fragments(test_client):
    # Seed existing student with year 2 in db11/db21
    sid = "101"
    meta1 = {"final_score": 9.0, "study_year": 2, "timestamp": "2025-01-01T00:00:00Z"}
    meta2 = {"student_id": 101, "name": "K", "surname": "L", "email": "k@e.com", "study_year": 2}

    s1 = InMemoryHttpClient("DBVS1", "db11")
    s2 = InMemoryHttpClient("DBVS2", "db21")
    seed_student(s1, sid, "doc", meta1)
    seed_student(s2, sid, "doc", meta2)

    resp = test_client.post("/student/101/upgrade")
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["previous_year"] == 2 and out["new_year"] == 3

    # Now student must be in db12/db22 and removed from db11/db21
    assert sid not in get_collection_store("DBVS1", "db11", "students")
    assert sid in get_collection_store("DBVS1", "db12", "students")
    assert sid not in get_collection_store("DBVS2", "db21", "students")
    assert sid in get_collection_store("DBVS2", "db22", "students")


def test_delete_course_deletes_related_reviews(test_client):
    # Seed course 16 in db22 and related reviews in db12
    seed_course(InMemoryHttpClient("DBVS2", "db22"), "16", "DB Systems", {"name": "Database Systems"})
    seed_review(InMemoryHttpClient("DBVS1", "db12"), "r1", "bad slides", {"course_id": 16})
    seed_review(InMemoryHttpClient("DBVS1", "db12"), "r2", "great content", {"course_id": 16})

    resp = test_client.delete("/course/16")
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["deleted_course_reviews"] == 2
    assert "db12" == out["peer_db"]
    assert "db22" == out["source_db"]


def test_delete_student_success_with_rollback_on_second_delete(test_client, monkeypatch):
    # Seed student in both vertical fragments
    sid = "202"
    seed_student(InMemoryHttpClient("DBVS1", "db11"), sid, "doc1", {"final_score": 7.0, "study_year": 1})
    seed_student(InMemoryHttpClient("DBVS2", "db21"), sid, "doc2", {"student_id": 202, "name": "A", "surname": "B", "email": "a@b.c", "study_year": 1})

    # Make DBVS2 deletion fail
    orig_get_client = api_module.get_client

    class FailingDeleteCollection(InMemoryCollection):
        def delete(self, ids):
            raise RuntimeError("Simulated DBVS2 deletion failure")

    def patched_get_client(server_name, db_name):
        base = InMemoryHttpClient(server_name, db_name)
        if server_name == "DBVS2":
            class Proxy(InMemoryHttpClient):
                def get_or_create_collection(self, name):
                    col = super().get_or_create_collection(name)
                    return FailingDeleteCollection(col.name, col._store)

                def get_collection(self, name):
                    col = super().get_collection(name)
                    return FailingDeleteCollection(col.name, col._store)

            return Proxy(server_name, db_name)
        return base

    monkeypatch.setattr(api_module, "get_client", patched_get_client)

    resp = test_client.delete("/student/202")
    assert resp.status_code == 500

    # DBVS1 must be rolled back (student present again)
    s1 = get_collection_store("DBVS1", "db11", "students")
    assert sid in s1


def test_move_course_transfers_related_entities_and_reviews(test_client):
    # Seed course 5 in DBVS2:db21 with linked exam_id=5 and program_id=5
    c_db21 = InMemoryHttpClient("DBVS2", "db21")
    seed_course(c_db21, "5", "Database Systems", {"name": "Database Systems", "exam_id": 5, "program_id": 5})

    # Seed related exam and program in db21
    exams_col = c_db21.get_or_create_collection("exams")
    exams_col.add(ids=["5"], documents=["Database Final"], metadatas=[{"course_id": 5, "name": "Database Final", "passing_score": 8.8}])

    progs_col = c_db21.get_or_create_collection("programs")
    progs_col.add(ids=["5"], documents=["Data Science"], metadatas=[{"name": "Data Science"}])

    # Seed two course reviews in DBVS1:db11 with course_id 5
    r_db11 = InMemoryHttpClient("DBVS1", "db11")
    seed_review(r_db11, "rv1", "good", {"course_id": 5})
    seed_review(r_db11, "rv2", "great", {"course_id": 5})

    # Move course to db22
    resp = test_client.post("/course/5/move", json={"target_db": "db22"})
    assert resp.status_code == 200, resp.text
    out = resp.json()
    assert out["from_dbvs2"] == "db21"
    assert out["to_dbvs2"] == "db22"
    assert out["moved_reviews"] == 2
    assert out["from_dbvs1"] == "db11"
    assert out["to_dbvs1"] == "db12"

    # DBVS2 assertions: course/exam/program removed from db21 and present in db22
    assert "5" not in get_collection_store("DBVS2", "db21", "courses")
    assert "5" in get_collection_store("DBVS2", "db22", "courses")
    assert "5" not in get_collection_store("DBVS2", "db21", "exams")
    assert "5" in get_collection_store("DBVS2", "db22", "exams")
    assert "5" not in get_collection_store("DBVS2", "db21", "programs")
    assert "5" in get_collection_store("DBVS2", "db22", "programs")

    # DBVS1 assertions: reviews moved from db11 to db12
    assert "rv1" not in get_collection_store("DBVS1", "db11", "course_review")
    assert "rv2" not in get_collection_store("DBVS1", "db11", "course_review")
    assert "rv1" in get_collection_store("DBVS1", "db12", "course_review")
    assert "rv2" in get_collection_store("DBVS1", "db12", "course_review")
