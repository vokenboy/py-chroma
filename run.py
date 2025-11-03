import chromadb
from chromadb import Settings
import os, csv, json, uuid

adminClient = chromadb.AdminClient(Settings(
    chroma_api_impl="chromadb.api.fastapi.FastAPI",
    chroma_server_host="localhost",
    chroma_server_http_port="8000",
))

def get_or_create_tenant_for_user(user_id, db_name):
    tenant_id = f"tenant_user:{user_id}"
    try:
        adminClient.get_tenant(tenant_id)
        print(f"Tenant '{tenant_id}' already exists.")
    except Exception:
        print(f"Creating tenant '{tenant_id}'...")
        adminClient.create_tenant(tenant_id)
    try:
        adminClient.get_database(db_name, tenant_id)
        print(f"Database '{db_name}' already exists for tenant '{tenant_id}'.")
    except Exception:
        print(f"Creating database '{db_name}' for tenant '{tenant_id}'...")
        adminClient.create_database(db_name, tenant_id)

    return tenant_id, db_name


user_id = "user12"
tenant, db11 = get_or_create_tenant_for_user(user_id, "db11")
tenant, db12 = get_or_create_tenant_for_user(user_id, "db12")

client_db11 = chromadb.HttpClient(tenant=tenant, database=db11)
client_db12 = chromadb.HttpClient(tenant=tenant, database=db12)

DATA_FOLDER = "./DB11"
TABLES11 = ["course_review", "students", "support_tickets", "support_responses"]
TABLES12 = ["course_review"]

def import_csv_to_chroma(client, base_folder, collection_name, filename):
    filepath = os.path.join(base_folder, filename)
    if not os.path.exists(filepath):
        print(f"File not found: {filepath}")
        return

    try:
        client.delete_collection(collection_name)
    except Exception:
        pass

    collection = client.get_or_create_collection(collection_name)
    ids, docs, metas = [], [], []

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                ids.append(str(uuid.uuid4()))
                docs.append(row["document"].strip())
                metas.append(json.loads(row["metadata"]))
            except Exception as e:
                print(f"Error reading {filename}: {e}")

    if docs:
        collection.add(ids=ids, documents=docs, metadatas=metas)
        print(f"Imported {len(docs)} documents into '{collection_name}'.")
    else:
        print(f"No valid data in {filename}.")

for name in TABLES11:
    import_csv_to_chroma(client_db11, DATA_FOLDER, name, f"{name}.csv")

for name in TABLES12:
    import_csv_to_chroma(client_db12, DATA_FOLDER, name, f"{name}.csv")

print(client_db11.list_collections())
print(client_db12.list_collections())
