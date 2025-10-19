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
db = "db3"

try:
    admin.create_tenant(tenant=tenant_name)
    print(f"Tenant '{tenant_name}' created.")
except Exception:
    print(f"ℹTenant '{tenant_name}' already exists.")

try:
    admin.create_database(tenant=tenant_name, database=db)
    print(f"✅ Database '{db}' created.")
except Exception:
    print(f"ℹ️ Database '{db}' already exists.")

client_db1 = chromadb.PersistentClient(
    database="db3",
)

DATA_FOLDER = "./DB11"
TABLES = ["professor_reviews"]

def import_csv_to_chroma(base_folder, collection_name, filename, label):
    filepath = os.path.join(base_folder, filename)
    if not os.path.exists(filepath):
        print(f"⚠️ Skipping {collection_name}, file {filepath} not found.")
        return

    try:
        client_db1.delete_collection(collection_name)
        print(f"🗑️ Old collection '{collection_name}' deleted ({label}).")
    except Exception:
        pass

    collection = client_db1.get_or_create_collection(collection_name)
    ids, documents, metadatas = [], [], []

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ids.append(str(uuid.uuid4()))
                documents.append(row["document"].strip())
                metadatas.append(json.loads(row["metadata"]))
            except Exception as e:
                print(f"❌ Error reading {filename}: {e}")

    if documents:
        collection.add(ids=ids, documents=documents, metadatas=metadatas)
        print(f"✅ Imported {len(documents)} → {collection_name} ({label})")
    else:
        print(f"⚠️ No valid data found in {filename}.")


print(f"📦 Importing data into tenant='{tenant_name}', database='{db}'...")

for name in TABLES:
    filename = f"{name}.csv"
    import_csv_to_chroma(DATA_FOLDER, name, filename, "Fragment 1")
