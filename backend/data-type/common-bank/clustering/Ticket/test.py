import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load .env variables
load_dotenv()

# MongoDB connection setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db["clusters"]

# Query documents with domain='tickets', sort by cluster_id ascending
query = {"data": "tickets"}
results = collection.find(query).sort("cluster_id", 1)

# Count and display results
count = collection.count_documents(query)
print(f"Cluster IDs (ascending) for domain='tickets': (Total: {count})")

# Print each cluster_id
for doc in results:
    print(doc.get("cluster_id"))
