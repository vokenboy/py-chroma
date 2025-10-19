import chromadb
from chromadb.config import Settings
import os, csv, json, uuid

admin = chromadb.AdminClient(
    Settings(
        chroma_server_host="localhost",
        chroma_server_http_port=8001
    )
)

tenant_name = "tenant"
db = "test"

try:
    admin.create_tenant(tenant=tenant_name)
    print(f"Tenant '{tenant_name}' created.")
except Exception:
    print(f"Tenant '{tenant_name}' already exists.")

try:
    admin.create_database(tenant=tenant_name, database=db)
    print(f"Database '{db}' created.")
except Exception:
    print(f"Database '{db}' already exists.")

client = chromadb.Client(tenant=tenant_name, database=db)


DATA_FOLDER = "./DB11"
TABLES = ["professor_reviews", "students", "support_tickets", "support_responses"]

def import_csv_to_chroma(base_folder, collection_name, filename, label):
    filepath = os.path.join(base_folder, filename)
    if not os.path.exists(filepath):
        print(f"⚠️ Skipping {collection_name}, file {filepath} not found.")
        return

    # Try to delete existing collection if it exists
    try:
        client.delete_collection(collection_name)
        print(f"🗑️ Old collection '{collection_name}' deleted ({label}).")
    except Exception:
        pass

    # Create or get collection
    collection = client.get_or_create_collection(collection_name)
    ids, docs, metas = [], [], []

    # Read CSV file
    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ids.append(str(uuid.uuid4()))
                docs.append(row["document"].strip())
                metas.append(json.loads(row["metadata"]))
            except Exception as e:
                print(f"❌ Error reading {filename}: {e}")

    # Add data
    if docs:
        collection.add(ids=ids, documents=docs, metadatas=metas)
        print(f"✅ Imported {len(docs)} → {collection_name} ({label})")
    else:
        print(f"⚠️ No valid data in {filename}.")

# --- Import all your CSVs into the DB ---
print(f"📦 Importing data into tenant='{tenant_name}', database='{db}'...")

for name in TABLES:
    filename = f"{name}.csv"
    import_csv_to_chroma(client, DATA_FOLDER, name, filename, "Fragment 1")

# --- List all collections in the DB ---
collections = client.list_collections()
print(f"\nFound {len(collections)} collection(s):")
for collection in collections:
    print("-", collection.name)
