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

print("="*70)
print("REMOVING LLM TRACKING FIELDS FROM tickets_new COLLECTION")
print("="*70)

# Check current status
total_records = tickets_collection.count_documents({})
records_with_llm_processed = tickets_collection.count_documents({"llm_processed": {"$exists": True}})
records_with_llm_model = tickets_collection.count_documents({"llm_model_used": {"$exists": True}})
records_with_llm_processed_at = tickets_collection.count_documents({"llm_processed_at": {"$exists": True}})

print(f"\nBefore Removal:")
print(f"  Total records: {total_records}")
print(f"  Records with 'llm_processed': {records_with_llm_processed}")
print(f"  Records with 'llm_model_used': {records_with_llm_model}")
print(f"  Records with 'llm_processed_at': {records_with_llm_processed_at}")

if records_with_llm_processed == 0 and records_with_llm_model == 0 and records_with_llm_processed_at == 0:
    print("\n✓ No LLM tracking fields found. Nothing to remove.")
    client.close()
    exit()

print("\nRemoving LLM tracking fields...")

# Remove the fields using $unset
result = tickets_collection.update_many(
    {},  # Match all documents
    {
        "$unset": {
            "llm_processed": "",
            "llm_model_used": "",
            "llm_processed_at": ""
        }
    }
)

print(f"\n✓ Operation completed!")
print(f"  Matched documents: {result.matched_count}")
print(f"  Modified documents: {result.modified_count}")

# Verify removal
print(f"\nAfter Removal:")
records_with_llm_processed_after = tickets_collection.count_documents({"llm_processed": {"$exists": True}})
records_with_llm_model_after = tickets_collection.count_documents({"llm_model_used": {"$exists": True}})
records_with_llm_processed_at_after = tickets_collection.count_documents({"llm_processed_at": {"$exists": True}})

print(f"  Records with 'llm_processed': {records_with_llm_processed_after}")
print(f"  Records with 'llm_model_used': {records_with_llm_model_after}")
print(f"  Records with 'llm_processed_at': {records_with_llm_processed_at_after}")

if records_with_llm_processed_after == 0 and records_with_llm_model_after == 0 and records_with_llm_processed_at_after == 0:
    print("\n✅ All LLM tracking fields successfully removed!")
else:
    print(f"\n⚠ Warning: {records_with_llm_processed_after + records_with_llm_model_after + records_with_llm_processed_at_after} fields still exist")

print("\nDone!")
client.close()
