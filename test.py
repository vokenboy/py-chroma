import chromadb
from chromadb import Settings

SERVERS = [
    {"host": "localhost", "port": 8000},
    {"host": "localhost", "port": 8001},
]

TENANTS = ["tenant_user:user12"]
DATABASES = ["db11", "db12", "db21", "db22"]


def print_collection_data(client, collection_name, limit=3):
    """Print sample data from a specific collection."""
    try:
        collection = client.get_collection(collection_name)
        data = collection.get(limit=limit)

        documents = data.get("documents", [])
        metadatas = data.get("metadatas", [])

        if not documents:
            print(f"        No data found in '{collection_name}'.")
            return

        print(f"        Showing up to {limit} entries from '{collection_name}':")
        for i, doc in enumerate(documents[:limit]):
            print(f"          [{i+1}] Document: {doc}")
            if i < len(metadatas):
                print(f"              Metadata: {metadatas[i]}")
    except Exception as e:
        print(f"        Error reading data from '{collection_name}': {e}")


def list_all(admin_client, host, port):
    print(f"\nServer: {host}:{port}")

    for tenant in TENANTS:
        tenant_printed = False
        for db in DATABASES:
            try:
                admin_client.get_database(db, tenant)
            except Exception:
                continue

            if not tenant_printed:
                print(f"  Tenant: {tenant}")
                tenant_printed = True

            print(f"    Database: {db}")

            try:
                client = chromadb.HttpClient(
                    tenant=tenant,
                    database=db,
                    host=host,
                    port=port
                )

                collections = client.list_collections()
                if collections:
                    for col in collections:
                        print(f"      Collection: {col.name}")
                        print_collection_data(client, col.name)
                else:
                    print("      (No collections)")
            except Exception as e:
                print(f"      Error listing collections: {e}")


for srv in SERVERS:
    host = srv["host"]
    port = srv["port"]

    admin = chromadb.AdminClient(Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host=host,
        chroma_server_http_port=port,
    ))

    list_all(admin, host, port)
