from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Connect to MongoDB
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]
tickets_collection = db["tickets_new"]

# Define stage categories
closed_stages = ["Resolved", "Close", "Report"]
open_stages = ["Receive", "Authenticate", "Categorize", "Resolution", "Escalation", "Update"]

# Update tickets with closed resolution_status
result_closed = tickets_collection.update_many(
    {"stages": {"$in": closed_stages}},
    {"$set": {"resolution_status": "closed"}}
)

# Update tickets with open resolution_status
result_open = tickets_collection.update_many(
    {"stages": {"$in": open_stages}},
    {"$set": {"resolution_status": "open"}}
)

print(f"Updated {result_closed.modified_count} tickets to 'closed' resolution_status")
print(f"Updated {result_open.modified_count} tickets to 'open' resolution_status")
print(f"Total tickets updated: {result_closed.modified_count + result_open.modified_count}")