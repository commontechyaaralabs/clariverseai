# Ultra-simple script to delete records without subcluster_label
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def delete_records_without_subcluster_label():
    # Connect to MongoDB
    client = MongoClient(os.getenv('MONGO_CONNECTION_STRING'))
    db = client[os.getenv('MONGO_DATABASE_NAME')]
    collection = db['social_media']
    
    print(f"Total documents before: {collection.count_documents({})}")
    
    # Show what we're working with
    sample = collection.find_one({})
    print("Sample document:")
    if sample:
        print(f"  Fields: {list(sample.keys())}")
        print(f"  subcluster_label: '{sample.get('subcluster_label', 'NOT_FOUND')}'")
    
    # Delete in steps
    print("\nDeleting documents...")
    
    # Step 1: Delete where field doesn't exist
    count1 = collection.count_documents({"subcluster_label": {"$exists": False}})
    print(f"Documents without subcluster_label field: {count1}")
    if count1 > 0:
        result1 = collection.delete_many({"subcluster_label": {"$exists": False}})
        print(f"Deleted {result1.deleted_count} documents (field missing)")
    
    # Step 2: Delete where field is null
    count2 = collection.count_documents({"subcluster_label": None})
    print(f"Documents with subcluster_label = null: {count2}")
    if count2 > 0:
        result2 = collection.delete_many({"subcluster_label": None})
        print(f"Deleted {result2.deleted_count} documents (null value)")
    
    # Step 3: Delete where field is empty string
    count3 = collection.count_documents({"subcluster_label": ""})
    print(f"Documents with subcluster_label = '': {count3}")
    if count3 > 0:
        result3 = collection.delete_many({"subcluster_label": ""})
        print(f"Deleted {result3.deleted_count} documents (empty string)")
    
    print(f"\nTotal documents after: {collection.count_documents({})}")
    
    # Final check
    remaining = collection.count_documents({
        "$or": [
            {"subcluster_label": {"$exists": False}},
            {"subcluster_label": None},
            {"subcluster_label": ""}
        ]
    })
    
    print(f"Documents still without subcluster_label: {remaining}")
    
    client.close()
    print("Done!")

if __name__ == "__main__":
    delete_records_without_subcluster_label()
