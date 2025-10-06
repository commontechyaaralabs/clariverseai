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

def remove_and_nullify_fields():
    """
    Remove specific fields and set others to null in all records
    """
    print("Starting field removal and nullification process...")
    print("Collection: tickets_new")
    
    # Fields to remove completely
    fields_to_remove = [
        "thread.title",
        "thread.ticket_title",
        "messages.title"
    ]
    
    # Fields to set to null
    fields_to_nullify = [
        "thread.first_message_at",
        "thread.last_message_at",
        "messages.headers.ticket_title",
        "messages.headers.body.text.plain", 
        "messages.headers.date",
        "messages.body.text.plain"
    ]
    
    print(f"Fields to remove: {', '.join(fields_to_remove)}")
    print(f"Fields to nullify: {', '.join(fields_to_nullify)}")
    
    # Check if collection has data
    total_count = tickets_collection.count_documents({})
    print(f"✓ Found {total_count} records in 'tickets_new' collection")
    
    if total_count == 0:
        print("⚠ No records found in collection. Exiting.")
        return
    
    # Show confirmation
    print(f"\nThis will modify ALL {total_count} records.")
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
                changes_made = False
                
                # Prepare unset operations (remove fields)
                unset_fields = {}
                for field in fields_to_remove:
                    if field_exists(record, field):
                        unset_fields[field] = ""
                        changes_made = True
                
                # Prepare set operations (nullify fields) - only for non-nested fields
                set_fields = {}
                for field in fields_to_nullify:
                    if field_exists(record, field) and '.' not in field:
                        set_fields[field] = None
                        changes_made = True
                
                # Special handling for messages array fields
                if 'messages' in record and isinstance(record['messages'], list):
                    # Remove title from each message
                    for message in record['messages']:
                        if 'title' in message:
                            del message['title']
                            changes_made = True
                        
                        # Nullify headers fields in each message
                        if 'headers' in message and isinstance(message['headers'], dict):
                            if 'ticket_title' in message['headers']:
                                message['headers']['ticket_title'] = None
                                changes_made = True
                            
                            if 'body' in message['headers'] and isinstance(message['headers']['body'], dict):
                                if 'text' in message['headers']['body'] and isinstance(message['headers']['body']['text'], dict):
                                    if 'plain' in message['headers']['body']['text']:
                                        message['headers']['body']['text']['plain'] = None
                                        changes_made = True
                            
                            if 'date' in message['headers']:
                                message['headers']['date'] = None
                                changes_made = True
                        
                        # Nullify body.text.plain in each message
                        if 'body' in message and isinstance(message['body'], dict):
                            if 'text' in message['body'] and isinstance(message['body']['text'], dict):
                                if 'plain' in message['body']['text']:
                                    message['body']['text']['plain'] = None
                                    changes_made = True
                
                # Handle thread object
                if 'thread' in record and isinstance(record['thread'], dict):
                    # Remove thread.title if it exists
                    if 'title' in record['thread']:
                        del record['thread']['title']
                        changes_made = True
                    
                    # Nullify thread-level fields
                    if 'first_message_at' in record['thread']:
                        record['thread']['first_message_at'] = None
                        changes_made = True
                    
                    if 'last_message_at' in record['thread']:
                        record['thread']['last_message_at'] = None
                        changes_made = True
                
                if changes_made:
                    # Update the record - use separate operations to avoid conflicts
                    update_operations = {}
                    
                    # Handle unset operations (removing fields) - only for non-thread fields
                    unset_non_thread = {}
                    for field, value in unset_fields.items():
                        if not field.startswith('thread.'):
                            unset_non_thread[field] = value
                    
                    if unset_non_thread:
                        update_operations['$unset'] = unset_non_thread
                    
                    # Handle set operations (nullifying fields) - only for non-thread fields
                    if set_fields:
                        update_operations['$set'] = update_operations.get('$set', {})
                        update_operations['$set'].update(set_fields)
                    
                    # Update messages array if it was modified
                    if 'messages' in record:
                        if '$set' not in update_operations:
                            update_operations['$set'] = {}
                        update_operations['$set']['messages'] = record['messages']
                    
                    # Update thread if it was modified
                    if 'thread' in record:
                        if '$set' not in update_operations:
                            update_operations['$set'] = {}
                        update_operations['$set']['thread'] = record['thread']
                    
                    # Execute the update
                    tickets_collection.update_one(
                        {'_id': record_id},
                        update_operations
                    )
                    updated_count += 1
                
                processed_count += 1
                
                # Progress update
                if processed_count % 50 == 0:
                    print(f"  Processed {processed_count}/{total_count} records...")
        
        print(f"\n✅ Field removal and nullification completed!")
        print(f"✓ Total records processed: {processed_count}")
        print(f"✓ Records updated: {updated_count}")
        print(f"✓ Records unchanged: {processed_count - updated_count}")
        
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
        return
    
    # Show sample of updated record
    if updated_count > 0:
        print(f"\n{'='*60}")
        print("SAMPLE OF UPDATED RECORD")
        print(f"{'='*60}")
        
        sample_record = tickets_collection.find_one()
        print(f"Sample record keys after update: {list(sample_record.keys())}")
        
        # Show thread structure
        if 'thread' in sample_record:
            print(f"\nThread structure: {list(sample_record['thread'].keys())}")
        
        # Show first message structure
        if 'messages' in sample_record and sample_record['messages']:
            first_message = sample_record['messages'][0]
            print(f"\nFirst message structure: {list(first_message.keys())}")
            if 'headers' in first_message:
                print(f"First message headers: {list(first_message['headers'].keys())}")

def field_exists(record, field_path):
    """
    Check if a nested field exists in a record
    """
    try:
        parts = field_path.split('.')
        current = record
        
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return False
        return True
    except:
        return False

def show_field_statistics():
    """
    Show statistics about which fields exist in the collection
    """
    print(f"\n{'='*60}")
    print("FIELD STATISTICS")
    print(f"{'='*60}")
    
    # Fields to check
    fields_to_check = [
        "thread.title",
        "thread.ticket_title",
        "thread.first_message_at",
        "thread.last_message_at",
        "messages.title",
        "messages.headers.ticket_title",
        "messages.headers.body.text.plain",
        "messages.headers.date",
        "messages.body.text.plain"
    ]
    
    total_count = tickets_collection.count_documents({})
    print(f"Total records: {total_count}")
    print(f"\nField presence statistics:")
    
    for field in fields_to_check:
        count = 0
        for record in tickets_collection.find():
            if field_exists(record, field):
                count += 1
        
        percentage = (count / total_count * 100) if total_count > 0 else 0
        status = "✓ EXISTS" if count > 0 else "✗ NOT FOUND"
        print(f"  {field:<35} {count:>4}/{total_count} ({percentage:>5.1f}%) {status}")

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
        
        # Show thread structure
        if 'thread' in sample_record:
            print(f"\nThread structure: {list(sample_record['thread'].keys())}")
            for key, value in sample_record['thread'].items():
                if isinstance(value, str) and len(value) > 80:
                    print(f"  {key}: {value[:80]}...")
                else:
                    print(f"  {key}: {value}")
        
        # Show first message structure
        if 'messages' in sample_record and sample_record['messages']:
            first_message = sample_record['messages'][0]
            print(f"\nFirst message structure: {list(first_message.keys())}")
            if 'headers' in first_message:
                print(f"Headers: {list(first_message['headers'].keys())}")
                for key, value in first_message['headers'].items():
                    if isinstance(value, str) and len(value) > 80:
                        print(f"  {key}: {value[:80]}...")
                    else:
                        print(f"  {key}: {value}")
    else:
        print("No records found in collection.")

def main():
    """
    Main function with menu options
    """
    while True:
        print(f"\n{'='*60}")
        print("TICKET FIELD REMOVAL & NULLIFICATION UTILITY")
        print(f"{'='*60}")
        print("1. Remove and nullify specified fields")
        print("2. Show field statistics")
        print("3. Show sample record structure")
        print("4. Exit")
        
        choice = input("\nEnter your choice (1-4): ").strip()
        
        if choice == '1':
            remove_and_nullify_fields()
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