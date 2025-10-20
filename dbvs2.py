import chromadb
from chromadb import Settings
from utils import (
    get_or_create_tenant_for_user,
    import_csv_to_chroma
)

PORT = 8001

admin = chromadb.AdminClient(Settings(
    chroma_api_impl="chromadb.api.fastapi.FastAPI",
    chroma_server_host="localhost",
    chroma_server_http_port=PORT,
))

user_id = "user12"
tenant, db21 = get_or_create_tenant_for_user(admin, user_id, "db21")
tenant, db22 = get_or_create_tenant_for_user(admin, user_id, "db22")

client_db21 = chromadb.HttpClient(tenant=tenant, database=db21, port=PORT)
client_db22 = chromadb.HttpClient(tenant=tenant, database=db22, port=PORT)

DATA_FOLDER = "./DB21"
TABLES11 = ["courses", "documents", "exam", "programs", "students"]
TABLES12 = ["courses"]

for name in TABLES11:
    import_csv_to_chroma(client_db21, DATA_FOLDER, name, f"{name}.csv")

for name in TABLES12:
    import_csv_to_chroma(client_db22, DATA_FOLDER, name, f"{name}.csv")

print(client_db21.list_collections())
print(client_db22.list_collections())
