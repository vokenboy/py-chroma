import chromadb
from chromadb import Settings
import os, csv, json, uuid

def get_or_create_tenant_for_user(admin_client, user_id, db_name):
    tenant_id = f"tenant_user:{user_id}"

    try:
        admin_client.get_tenant(tenant_id)
        print(f"Tenant '{tenant_id}' already exists.")
    except Exception:
        print(f"Creating tenant '{tenant_id}'...")
        admin_client.create_tenant(tenant_id)

    try:
        admin_client.get_database(db_name, tenant_id)
        print(f"Database '{db_name}' already exists for tenant '{tenant_id}'.")
    except Exception:
        print(f"Creating database '{db_name}' for tenant '{tenant_id}'...")
        admin_client.create_database(db_name, tenant_id)

    return tenant_id, db_name

def import_csv_to_chroma(client, base_folder, collection_name, filename, id_fn=None, metadata_fn=None):
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
                doc = row["document"].strip()
                meta = json.loads(row["metadata"]) if row.get("metadata") else {}

                if callable(metadata_fn):
                    try:
                        meta = metadata_fn(meta, row)
                    except Exception as e:
                        print(f"metadata_fn error on {filename}: {e}")

                # Prefer explicit CSV id column if present
                rid = None
                if "id" in row and str(row["id"]).strip() != "":
                    rid = str(row["id"]).strip()
                elif callable(id_fn):
                    try:
                        rid = id_fn(meta, row)
                    except Exception as e:
                        print(f"id_fn error on {filename}: {e}")
                        rid = None

                if not rid:
                    rid = str(uuid.uuid4())

                ids.append(rid)
                docs.append(doc)
                metas.append(meta)
            except Exception as e:
                print(f"Error reading {filename}: {e}")

    if docs:
        collection.add(ids=ids, documents=docs, metadatas=metas)
        print(f"Imported {len(docs)} documents into '{collection_name}'.")
    else:
        print(f"No valid data in {filename}.")
