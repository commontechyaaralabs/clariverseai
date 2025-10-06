# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collections
chat_new_collection = db['chat_new']

def remove_chat_topic_field():
    """
    Remove the 'chat.topic' field from all records in chat_new collection
    """
    print(f"Removing 'chat.topic' field from {chat_new_collection.name} collection...")
    
    # Count total records first
    total_records = chat_new_collection.count_documents({})
    print(f"✓ Found {total_records} records in collection")
    
    if total_records == 0:
        print("No records found in collection!")
        return
    
    # Remove the 'chat.topic' field from all records
    result = chat_new_collection.update_many(
        {},  # Empty filter means all documents
        {"$unset": {"chat.topic": ""}}  # Remove the chat.topic field
    )
    
    print(f"✓ Successfully removed 'chat.topic' field from {result.modified_count} records")
    print(f"✓ Matched {result.matched_count} records")

# Run the script
if __name__ == "__main__":
    try:
        remove_chat_topic_field()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        # Close the database connection
        client.close()