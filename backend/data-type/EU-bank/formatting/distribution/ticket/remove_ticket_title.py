# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collections
tickets_collection = db['tickets_new']

def remove_thread_ticket_title():
    """
    Remove thread.ticket_title field from all records
    """
    print("Starting thread.ticket_title removal process...")
    print("Collection: tickets_new")
    
    # Check if collection has data
    total_count = tickets_collection.count_documents({})
    print(f"✓ Found {total_count} records in 'tickets_new' collection")
    
    if total_count == 0:
        print("⚠ No records found in collection. Exiting.")
        return
    
    # Count records that have thread.ticket_title field
    records_with_ticket_title = tickets_collection.count_documents({
        "thread.ticket_title": {"$exists": True}
    })
    print(f"✓ Found {records_with_ticket_title} records with 'thread.ticket_title' field")
    
    if records_with_ticket_title == 0:
        print("✓ No records have 'thread.ticket_title' field. Nothing to remove.")
        return
    
    # Show confirmation
    print(f"\nThis will remove 'thread.ticket_title' from {records_with_ticket_title} records.")
    response = input("Are you sure you want to continue? (y/n): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Remove the field using $unset
    print(f"\nRemoving 'thread.ticket_title' field...")
    
    try:
        result = tickets_collection.update_many(
            {"thread.ticket_title": {"$exists": True}},
            {"$unset": {"thread.ticket_title": ""}}
        )
        
        print(f"✅ Successfully removed 'thread.ticket_title' from {result.modified_count} records")
        
        # Verify the removal
        remaining_count = tickets_collection.count_documents({
            "thread.ticket_title": {"$exists": True}
        })
        print(f"✓ Verification: {remaining_count} records still have 'thread.ticket_title' field")
        
        if remaining_count == 0:
            print("✅ All 'thread.ticket_title' fields have been successfully removed!")
        else:
            print(f"⚠ Warning: {remaining_count} records still have the field")
            
    except Exception as e:
        print(f"❌ Error during removal: {e}")
        return
    
    # Show sample of updated record
    print(f"\n{'='*60}")
    print("SAMPLE OF UPDATED RECORD")
    print(f"{'='*60}")
    
    sample_record = tickets_collection.find_one()
    if sample_record and 'thread' in sample_record:
        print(f"Thread object keys after update: {list(sample_record['thread'].keys())}")
        
        # Show thread structure
        for key, value in sample_record['thread'].items():
            if isinstance(value, str) and len(value) > 80:
                print(f"  {key}: {value[:80]}...")
            else:
                print(f"  {key}: {value}")
    else:
        print("No sample record found or thread object missing")

def show_field_statistics():
    """
    Show statistics about thread.ticket_title field
    """
    print(f"\n{'='*60}")
    print("FIELD STATISTICS")
    print(f"{'='*60}")
    
    total_count = tickets_collection.count_documents({})
    records_with_ticket_title = tickets_collection.count_documents({
        "thread.ticket_title": {"$exists": True}
    })
    records_without_ticket_title = total_count - records_with_ticket_title
    
    print(f"Total records: {total_count}")
    print(f"Records with 'thread.ticket_title': {records_with_ticket_title}")
    print(f"Records without 'thread.ticket_title': {records_without_ticket_title}")
    
    if records_with_ticket_title > 0:
        percentage = (records_with_ticket_title / total_count * 100)
        print(f"Percentage with field: {percentage:.1f}%")
    else:
        print("✅ No records have 'thread.ticket_title' field")

def show_sample_thread():
    """
    Show a sample thread object structure
    """
    print(f"\n{'='*60}")
    print("SAMPLE THREAD OBJECT")
    print(f"{'='*60}")
    
    sample_record = tickets_collection.find_one({"thread": {"$exists": True}})
    if sample_record and 'thread' in sample_record:
        print(f"Thread object keys: {list(sample_record['thread'].keys())}")
        print(f"\nThread object structure:")
        
        for key, value in sample_record['thread'].items():
            if isinstance(value, str) and len(value) > 100:
                print(f"  {key}: {value[:100]}...")
            else:
                print(f"  {key}: {value}")
    else:
        print("No thread object found in collection")

def main():
    """
    Main function with menu options
    """
    while True:
        print(f"\n{'='*60}")
        print("THREAD.TICKET_TITLE REMOVAL UTILITY")
        print(f"{'='*60}")
        print("1. Remove thread.ticket_title from all records")
        print("2. Show field statistics")
        print("3. Show sample thread object")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            remove_thread_ticket_title()
        elif choice == '2':
            show_field_statistics()
        elif choice == '3':
            show_sample_thread()
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please enter 1-4.")

# Run the script
if __name__ == "__main__":
    main()
