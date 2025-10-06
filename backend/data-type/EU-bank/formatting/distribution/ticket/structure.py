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
tickets_collection = db['tickets_new']

def remove_specific_fields():
    """
    Remove specific fields from all records in tickets_new collection
    """
    print("Starting field removal process...")
    print("Collection: tickets_new")
    
    # Fields to remove
    fields_to_remove = [
        "stages",
        "follow_up_required", 
        "action_pending_from",
        "action_pending_status",
        "assigned_team_email",
        "follow_up_date",
        "follow_up_reason",
        "next_action_suggestion",
        "overall_sentiment",
        "priority",
        "resolution_status",
        "sentiment",
        "ticket_raised",
        "ticket_summary",
        "title"
    ]
    
    print(f"Fields to remove: {', '.join(fields_to_remove)}")
    
    # Check if collection has data
    total_count = tickets_collection.count_documents({})
    print(f"✓ Found {total_count} records in 'tickets_new' collection")
    
    if total_count == 0:
        print("⚠ No records found in collection. Exiting.")
        return
    
    # Show confirmation
    print(f"\nThis will remove the specified fields from ALL {total_count} records.")
    response = input("Are you sure you want to continue? (y/n): ")
    if response.lower() != 'y':
        print("Operation cancelled.")
        return
    
    # Process records in batches for better performance
    batch_size = 100
    processed_count = 0
    updated_count = 0
    
    print(f"\nProcessing records in batches of {batch_size}...")
    
    try:
        # Process all records
        for skip in range(0, total_count, batch_size):
            # Get batch of records
            records = list(tickets_collection.find().skip(skip).limit(batch_size))
            
            for record in records:
                record_id = record['_id']
                fields_removed = []
                
                # Check which fields exist and need to be removed
                for field in fields_to_remove:
                    if field in record:
                        fields_removed.append(field)
                
                if fields_removed:
                    # Remove the fields using $unset
                    unset_fields = {field: "" for field in fields_removed}
                    tickets_collection.update_one(
                        {'_id': record_id},
                        {'$unset': unset_fields}
                    )
                    updated_count += 1
                
                processed_count += 1
                
                # Progress update
                if processed_count % 50 == 0:
                    print(f"  Processed {processed_count}/{total_count} records...")
        
        print(f"\n✅ Field removal completed!")
        print(f"✓ Total records processed: {processed_count}")
        print(f"✓ Records updated: {updated_count}")
        print(f"✓ Records unchanged: {processed_count - updated_count}")
        
    except Exception as e:
        print(f"❌ Error during field removal: {e}")
        return
    
    # Show sample of updated record
    if updated_count > 0:
        print(f"\n{'='*60}")
        print("SAMPLE OF UPDATED RECORD")
        print(f"{'='*60}")
        
        sample_record = tickets_collection.find_one()
        print(f"Sample record keys after update: {list(sample_record.keys())}")
        
        # Show first few field values
        for key, value in list(sample_record.items())[:8]:
            if isinstance(value, str) and len(value) > 100:
                print(f"  {key}: {value[:100]}...")
            else:
                print(f"  {key}: {value}")
        
        if len(sample_record) > 8:
            print(f"  ... and {len(sample_record) - 8} more fields")

def show_field_statistics():
    """
    Show statistics about which fields exist in the collection
    """
    print(f"\n{'='*60}")
    print("FIELD STATISTICS")
    print(f"{'='*60}")
    
    # Fields to check
    fields_to_check = [
        "stages",
        "follow_up_required", 
        "action_pending_from",
        "action_pending_status",
        "assigned_team_email",
        "follow_up_date",
        "follow_up_reason",
        "next_action_suggestion",
        "overall_sentiment",
        "priority",
        "resolution_status",
        "sentiment",
        "ticket_raised",
        "ticket_summary",
        "title"
    ]
    
    total_count = tickets_collection.count_documents({})
    print(f"Total records: {total_count}")
    print(f"\nField presence statistics:")
    
    for field in fields_to_check:
        count = tickets_collection.count_documents({field: {"$exists": True}})
        percentage = (count / total_count * 100) if total_count > 0 else 0
        status = "✓ EXISTS" if count > 0 else "✗ NOT FOUND"
        print(f"  {field:<25} {count:>4}/{total_count} ({percentage:>5.1f}%) {status}")

def show_sample_record():
    """
    Show a sample record structure
    """
    print(f"\n{'='*60}")
    print("SAMPLE RECORD STRUCTURE")
    print(f"{'='*60}")
    
    sample_record = tickets_collection.find_one()
    if sample_record:
        print(f"Sample record keys: {list(sample_record.keys())}")
        print(f"\nFirst 10 fields with values:")
        
        for i, (key, value) in enumerate(list(sample_record.items())[:10]):
            if isinstance(value, str) and len(value) > 80:
                print(f"  {i+1:2}. {key}: {value[:80]}...")
            else:
                print(f"  {i+1:2}. {key}: {value}")
        
        if len(sample_record) > 10:
            print(f"  ... and {len(sample_record) - 10} more fields")
    else:
        print("No records found in collection.")

def main():
    """
    Main function with menu options
    """
    while True:
        print(f"\n{'='*60}")
        print("TICKET FIELD REMOVAL UTILITY")
        print(f"{'='*60}")
        print("1. Remove specified fields from all records")
        print("2. Show field statistics")
        print("3. Show sample record structure")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            remove_specific_fields()
        elif choice == '2':
            show_field_statistics()
        elif choice == '3':
            show_sample_record()
        elif choice == '4':
            print("Goodbye!")
            break
        else:
            print("Invalid choice. Please enter 1-4.")

# Run the script
if __name__ == "__main__":
    main()