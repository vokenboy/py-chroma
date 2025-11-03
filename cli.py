import chromadb
from chromadb import Settings

SERVERS = [
    {"name": "DBVS1", "host": "localhost", "port": 8000},
    {"name": "DBVS2", "host": "localhost", "port": 8001},
]

TENANTS = ["tenant_user:user12"]
DATABASES = ["db11", "db12", "db21", "db22"]


def get_client(host, port, tenant, database):
    return chromadb.HttpClient(
        tenant=tenant,
        database=database,
        port=port,
        host=host
    )


def print_collection_data(client, collection_name):
    try:
        collection = client.get_collection(collection_name)
        data = collection.get(limit=None)

        ids = data.get("ids", [])
        documents = data.get("documents", [])
        metadatas = data.get("metadatas", [])

        if not documents:
            print(f"\nNo data found in '{collection_name}'.")
            return

        print(f"\nShowing entries from '{collection_name}':")

        for i, doc in enumerate(documents):
            print(f"\n  [{i+1}] ID: {ids[i] if i < len(ids) else 'N/A'}")
            print(f"      Document: {doc}")
            if i < len(metadatas):
                print(f"      Metadata: {metadatas[i]}")

    except Exception as e:
        print(f"Error reading data from '{collection_name}': {e}")


def list_collections(client):
    try:
        collections = client.list_collections()
        if not collections:
            print("\n(No collections found.)")
            return []
        print("\nAvailable Collections:")
        for i, col in enumerate(collections):
            print(f"  [{i+1}] {col.name}")
        return collections
    except Exception as e:
        print(f"Error listing collections: {e}")
        return []


def select_from_list(options, label):
    print(f"\nSelect {label}:")
    for i, item in enumerate(options, 1):
        name = item["name"] if isinstance(item, dict) else item
        print(f"  [{i}] {name}")
    while True:
        try:
            choice = int(input(f"Enter {label} number: "))
            if 1 <= choice <= len(options):
                return options[choice - 1]
        except ValueError:
            pass
        print(f"Invalid {label}. Try again.")


def main():
    server = select_from_list(SERVERS, "server")
    host, port = server["host"], server["port"]

    admin = chromadb.AdminClient(Settings(
        chroma_api_impl="chromadb.api.fastapi.FastAPI",
        chroma_server_host=host,
        chroma_server_http_port=port,
    ))

    print("\nChecking available databases...")
    available_dbs = []
    for db in DATABASES:
        try:
            admin.get_database(db, "tenant_user:user12")
            available_dbs.append(db)
        except Exception:
            continue

    if not available_dbs:
        print("No databases found for this tenant.")
        return

    selected_db = select_from_list(available_dbs, "database")

    client = get_client(host, port, "tenant_user:user12", selected_db)

    collections = list_collections(client)
    if not collections:
        return

    collection = select_from_list(
        [{"name": c.name} for c in collections], "collection"
    )
    print_collection_data(client, collection["name"])


if __name__ == "__main__":
    main()
