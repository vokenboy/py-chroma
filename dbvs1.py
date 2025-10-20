import chromadb
from chromadb import Settings
from utils import (
    get_or_create_tenant_for_user,
    import_csv_to_chroma
)

PORT = 8000

admin = chromadb.AdminClient(Settings(
    chroma_api_impl="chromadb.api.fastapi.FastAPI",
    chroma_server_host="localhost",
    chroma_server_http_port=PORT,
))

user_id = "user12"
tenant, db11 = get_or_create_tenant_for_user(admin, user_id, "db11")
tenant, db12 = get_or_create_tenant_for_user(admin, user_id, "db12")

client_db11 = chromadb.HttpClient(tenant=tenant, database=db11, port=PORT)
client_db12 = chromadb.HttpClient(tenant=tenant, database=db12, port=PORT)

DATA_FOLDER = "./DB11"
TABLES11 = ["professor_reviews", "students", "support_tickets", "support_responses"]
TABLES12 = ["professor_reviews"]

for name in TABLES11:
    import_csv_to_chroma(client_db11, DATA_FOLDER, name, f"{name}.csv")

for name in TABLES12:
    import_csv_to_chroma(client_db12, DATA_FOLDER, name, f"{name}.csv")

print(client_db11.list_collections())
print(client_db12.list_collections())
