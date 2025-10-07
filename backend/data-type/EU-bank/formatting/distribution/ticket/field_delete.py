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

# Get collection
tickets_collection = db['tickets_new']

# Remove the thread.ticket_title field from all documents
result = tickets_collection.update_many(
    {},  # Empty filter to match all documents
    {
        "$unset": {
            "thread.ticket_title": ""  # Remove the nested field
        }
    }
)

# Print the results
print(f"Matched documents: {result.matched_count}")
print(f"Modified documents: {result.modified_count}")

# Close the connection
client.close()

print("\nField 'thread.ticket_title' has been removed from all records successfully!")