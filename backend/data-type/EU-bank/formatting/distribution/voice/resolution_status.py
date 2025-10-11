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
voice_collection = db["voice_new"]

print("=" * 80)
print("UPDATING RESOLUTION STATUS FOR VOICE CALLS")
print("=" * 80)

# Define stage categories
closed_stages = ["Resolved", "Close", "Report"]
open_stages = ["Receive", "Authenticate", "Categorize", "Resolution", "Escalation", "Update"]

print(f"\nClosed stages: {closed_stages}")
print(f"Open stages: {open_stages}")

# Get initial counts
total_records = voice_collection.count_documents({})
print(f"\nTotal records in voice_new: {total_records}")

# Check current stages distribution
print("\nCurrent stages distribution:")
for stage in closed_stages + open_stages:
    count = voice_collection.count_documents({"stages": stage})
    if count > 0:
        print(f"  {stage}: {count} records")

# Update voice calls with closed resolution_status
print("\n" + "=" * 80)
print("Updating 'closed' resolution_status...")
result_closed = voice_collection.update_many(
    {"stages": {"$in": closed_stages}},
    {"$set": {"resolution_status": "closed"}}
)

# Update voice calls with open resolution_status
print("Updating 'open' resolution_status...")
result_open = voice_collection.update_many(
    {"stages": {"$in": open_stages}},
    {"$set": {"resolution_status": "open"}}
)

print("\n" + "=" * 80)
print("RESULTS")
print("=" * 80)
print(f"✅ Updated {result_closed.modified_count} voice calls to 'closed' resolution_status")
print(f"✅ Updated {result_open.modified_count} voice calls to 'open' resolution_status")
print(f"✅ Total voice calls updated: {result_closed.modified_count + result_open.modified_count}")

# Verification
print("\n" + "=" * 80)
print("VERIFICATION")
print("=" * 80)

closed_count = voice_collection.count_documents({"resolution_status": "closed"})
open_count = voice_collection.count_documents({"resolution_status": "open"})
total_with_status = closed_count + open_count

print(f"\nResolution status distribution:")
print(f"  Closed: {closed_count} records ({(closed_count/total_records*100):.2f}%)")
print(f"  Open: {open_count} records ({(open_count/total_records*100):.2f}%)")
print(f"  Total with resolution_status: {total_with_status} records")

if total_with_status < total_records:
    without_status = total_records - total_with_status
    print(f"\n⚠️ Records without resolution_status: {without_status}")
else:
    print(f"\n✅ All records have resolution_status assigned!")

# Close connection
client.close()
print("\n" + "=" * 80)
print("✅ RESOLUTION STATUS UPDATE COMPLETED SUCCESSFULLY!")
print("=" * 80)

