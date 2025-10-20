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

DB21_FOLDER = "./DB21"
DB22_FOLDER = "./DB21"
TABLES21 = ["courses", "documents", "exams", "programs", "students"]
TABLES22 = ["courses", "documents", "exams", "programs", "students"]

for name in TABLES21:
    import_csv_to_chroma(client_db21, DB21_FOLDER, name, f"{name}.csv")

for name in TABLES22:
    import_csv_to_chroma(client_db22, DB22_FOLDER, name, f"{name}.csv")

print(client_db21.list_collections())
print(client_db22.list_collections())
