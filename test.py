import chromadb
from chromadb.config import Settings

# Initialize ChromaDB client with persistent storage
client = chromadb.PersistentClient(path="multitenant")

# List all collections
collections = client.list_collections()
print(f"Found {len(collections)} collection(s):\n")

for collection in collections:
    print(f"Collection Name: {collection.name}")
    print(f"Collection ID: {collection.id}")
    
    # Get collection details
    count = collection.count()
    print(f"Number of documents: {count}")
    
    # Peek at first few items
    if count > 0:
        peek = collection.peek(limit=5)
        print(f"\nFirst {min(5, count)} items:")
        print(f"IDs: {peek['ids']}")
        print(f"Metadata: {peek['metadatas']}")
        if peek['documents']:
            print(f"Documents: {peek['documents'][:3]}...")
    
    print("-" * 80)

"""
collection = client.get_collection(name="your_collection_name")
results = collection.query(
    query_texts=["your query text"],
    n_results=5
)
print("Query results:", results)
"""