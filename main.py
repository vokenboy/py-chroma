import chromadb
import json
import csv
import uuid
import os

client = chromadb.HttpClient(host="localhost", port=8000)
DATA_FOLDER = "./data"

# --- Define fragment sets ---
vertical_fragments = {
    "internal": ["programs", "courses", "exam", "documents"],
    "external": ["support_tickets", "support_responses", "professor_reviews"],
}

# Shared (used in both vertical fragments)
shared_table = "students"

# --- CSV import helper ---
def import_csv_to_chroma(collection_name, filename, condition=lambda _: True):
    filepath = os.path.join(DATA_FOLDER, filename)
    if not os.path.exists(filepath):
        print(f"⚠️ Missing {filename}")
        return

    try:
        client.delete_collection(collection_name)
    except Exception:
        pass

    collection = client.get_or_create_collection(collection_name)
    ids, documents, metadatas = [], [], []

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                metadata = json.loads(row["metadata"])
                if not condition(metadata):
                    continue
                ids.append(str(uuid.uuid4()))
                documents.append(row["document"].strip())
                metadatas.append(metadata)
            except Exception as e:
                print(f"❌ Error reading row: {e}")

    if documents:
        collection.add(ids=ids, documents=documents, metadatas=metadatas)
        print(f"✅ Imported {len(documents)} records into '{collection_name}'")
    else:
        print(f"⚠️ No valid records for {collection_name}")

    return collection

# --- Vertical fragmentation ---
for name in vertical_fragments["internal"]:
    import_csv_to_chroma(f"internal_{name}", f"{name}.csv")

for name in vertical_fragments["external"]:
    import_csv_to_chroma(f"external_{name}", f"{name}.csv")

# --- Horizontal fragmentation of shared table (students) ---
def passed(metadata): return metadata.get("final_score", 0) >= 50
def failed(metadata): return metadata.get("final_score", 0) < 50

import_csv_to_chroma("students_passed", f"{shared_table}.csv", condition=passed)
import_csv_to_chroma("students_failed", f"{shared_table}.csv", condition=failed)
