# Import required libraries
from pymongo import MongoClient
import os
import random
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

print("Starting action_pending_from field updates...")

# Step 1: Remove existing action_pending_from field from all records
print("\nStep 1: Removing existing action_pending_from field from all records...")
result1 = db.email_new.update_many(
    {"action_pending_from": {"$exists": True}},
    {"$unset": {"action_pending_from": ""}}
)
print(f"Removed action_pending_from field from {result1.modified_count} documents")

# Step 2: Update records with specific filter to set action_pending_from: "company"
print("\nStep 2: Setting action_pending_from to 'company' for specific filter...")
filter_criteria = {
    "action_pending_status": "yes",
    "category": "External",
    "thread.message_count": 1
}
result2 = db.email_new.update_many(
    filter_criteria,
    {"$set": {"action_pending_from": "company"}}
)
print(f"Updated {result2.modified_count} documents with action_pending_from: 'company'")

# Step 3: Get remaining records with action_pending_status: "yes"
print("\nStep 3: Processing remaining records with action_pending_status: 'yes'...")
remaining_records = list(db.email_new.find({
    "action_pending_status": "yes",
    "action_pending_from": {"$exists": False}
}))

print(f"Found {len(remaining_records)} remaining records to process")

# Step 4: Random distribution for remaining records
print("\nStep 4: Applying random distribution...")
company_count = 0
customer_count = 0

for record in remaining_records:
    # Random choice between "company" and "customer"
    action_pending_from = random.choice(["company", "customer"])
    
    # Update the record
    db.email_new.update_one(
        {"_id": record["_id"]},
        {"$set": {"action_pending_from": action_pending_from}}
    )
    
    if action_pending_from == "company":
        company_count += 1
    else:
        customer_count += 1

print(f"Random distribution completed:")
print(f"  - 'company': {company_count} records")
print(f"  - 'customer': {customer_count} records")

# Step 5: Verification
print("\nStep 5: Verification...")
total_yes = db.email_new.count_documents({"action_pending_status": "yes"})
company_total = db.email_new.count_documents({"action_pending_status": "yes", "action_pending_from": "company"})
customer_total = db.email_new.count_documents({"action_pending_status": "yes", "action_pending_from": "customer"})
unassigned = db.email_new.count_documents({"action_pending_status": "yes", "action_pending_from": {"$exists": False}})

print(f"Verification results:")
print(f"  - Total records with action_pending_status: 'yes': {total_yes}")
print(f"  - Records with action_pending_from: 'company': {company_total}")
print(f"  - Records with action_pending_from: 'customer': {customer_total}")
print(f"  - Unassigned records: {unassigned}")

print("\nAction pending updates completed successfully!")

# Close the connection
client.close()
