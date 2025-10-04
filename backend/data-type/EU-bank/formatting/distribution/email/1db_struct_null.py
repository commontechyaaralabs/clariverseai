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

# Target collection
collection = db['email_new']

# Count total records
total_records = collection.count_documents({})
updated_count = 0

print(f"Total records to update: {total_records}")
print("Starting update operation...\n")

# Process each document one by one
for document in collection.find():
    try:
        # Prepare update operations - combining all fields from both codes
        update_fields = {
            'thread.subject_norm': None,
            'thread.first_message_at': None,
            'thread.last_message_at': None,
            'first_message_at': None,
            'last_message_at': None
        }
        
        # Update messages array - set fields to null for all messages
        if 'messages' in document and isinstance(document['messages'], list):
            for i in range(len(document['messages'])):
                update_fields[f'messages.{i}.headers.date'] = None
                update_fields[f'messages.{i}.headers.subject'] = None
                update_fields[f'messages.{i}.body.text.plain'] = None
        
        # Update the document
        collection.update_one(
            {'_id': document['_id']},
            {'$set': update_fields}
        )
        
        updated_count += 1
        
        # Print progress every 100 records
        if updated_count % 100 == 0:
            print(f"Updated {updated_count}/{total_records} records...")
            
    except Exception as e:
        print(f"Error updating document with _id {document.get('_id')}: {str(e)}")

# Print summary
print("\n" + "="*50)
print("Update operation completed!")
print(f"Total records updated: {updated_count}/{total_records}")
print("="*50)

# Close the connection
client.close()