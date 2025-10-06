# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
import random
import json

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collections
tickets_collection = db['tickets']
tickets_new_collection = db['tickets_new']

def copy_tickets_to_new_collection():
    """
    Copy all records from 'tickets' collection to 'tickets_new' collection
    """
    print("Starting ticket collection copy process...")
    print(f"Source collection: tickets")
    print(f"Target collection: tickets_new")
    
    # Check if source collection exists and has data
    source_count = tickets_collection.count_documents({})
    print(f"✓ Found {source_count} records in source collection 'tickets'")
    
    if source_count == 0:
        print("⚠ No records found in source collection. Exiting.")
        return
    
    # Check if target collection already exists
    target_count = tickets_new_collection.count_documents({})
    if target_count > 0:
        print(f"⚠ Target collection 'tickets_new' already has {target_count} records.")
        response = input("Do you want to continue and add to existing records? (y/n): ")
        if response.lower() != 'y':
            print("Operation cancelled.")
            return
    
    # Get all records from source collection
    print(f"\nReading all records from 'tickets' collection...")
    records = list(tickets_collection.find())
    print(f"✓ Loaded {len(records)} records from source collection")
    
    # Insert records into target collection
    print(f"\nInserting records into 'tickets_new' collection...")
    
    try:
        # Use insert_many for better performance
        result = tickets_new_collection.insert_many(records)
        inserted_count = len(result.inserted_ids)
        
        print(f"✓ Successfully inserted {inserted_count} records into 'tickets_new'")
        
        # Verify the copy
        final_count = tickets_new_collection.count_documents({})
        print(f"✓ Verification: 'tickets_new' now contains {final_count} records")
        
        if inserted_count == source_count:
            print("✅ Copy operation completed successfully!")
        else:
            print(f"⚠ Warning: Expected {source_count} records, but inserted {inserted_count}")
            
    except Exception as e:
        print(f"❌ Error during insertion: {e}")
        return
    
    # Show sample of copied data
    if final_count > 0:
        print(f"\n{'='*60}")
        print("SAMPLE OF COPIED DATA")
        print(f"{'='*60}")
        
        # Get a sample record
        sample_record = tickets_new_collection.find_one()
        print(f"Sample record keys: {list(sample_record.keys())}")
        
        # Show first few field values
        for key, value in list(sample_record.items())[:5]:
            if isinstance(value, str) and len(value) > 100:
                print(f"  {key}: {value[:100]}...")
            else:
                print(f"  {key}: {value}")
        
        if len(sample_record) > 5:
            print(f"  ... and {len(sample_record) - 5} more fields")

def clear_tickets_new_collection():
    """
    Clear all records from 'tickets_new' collection
    """
    print("Clearing 'tickets_new' collection...")
    
    count = tickets_new_collection.count_documents({})
    if count == 0:
        print("✓ Collection is already empty")
        return
    
    response = input(f"Are you sure you want to delete {count} records from 'tickets_new'? (y/n): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    try:
        result = tickets_new_collection.delete_many({})
        print(f"✓ Deleted {result.deleted_count} records from 'tickets_new'")
    except Exception as e:
        print(f"❌ Error during deletion: {e}")

def show_collection_stats():
    """
    Show statistics for both collections
    """
    print(f"\n{'='*60}")
    print("COLLECTION STATISTICS")
    print(f"{'='*60}")
    
    tickets_count = tickets_collection.count_documents({})
    tickets_new_count = tickets_new_collection.count_documents({})
    
    print(f"Source collection 'tickets':      {tickets_count} records")
    print(f"Target collection 'tickets_new':  {tickets_new_count} records")
    
    if tickets_count > 0 and tickets_new_count > 0:
        if tickets_count == tickets_new_count:
            print("✅ Both collections have the same number of records")
        else:
            print(f"⚠ Different record counts: {tickets_count} vs {tickets_new_count}")

def main():
    """
    Main function with menu options
    """
    while True:
        print(f"\n{'='*60}")
        print("TICKET COLLECTION COPY UTILITY")
        print(f"{'='*60}")
        print("1. Copy tickets to tickets_new")
        print("2. Clear tickets_new collection")
        print("3. Show collection statistics")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            copy_tickets_to_new_collection()
        elif choice == '2':
            clear_tickets_new_collection()
        elif choice == '3':
            show_collection_stats()
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please enter 1-4.")

# Run the script
if __name__ == "__main__":
    main()
