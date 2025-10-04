# Import required libraries
from pymongo import MongoClient
from collections import defaultdict
import csv
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

# Source and destination collections
source_collection = db['email']
destination_collection = db['email_new']

# Fields to copy
fields_to_copy = [
    'provider',
    'thread',
    'messages',
    'dominant_topic',
    'subtopics',
    'kmeans_cluster_id',
    'subcluster_id',
    'subcluster_label',
    'dominant_cluster_label',
    'kmeans_cluster_keyphrase',
    'domain',
    'stages'
]

# Counter for tracking progress
total_records = source_collection.count_documents({})
copied_count = 0
skipped_count = 0

print(f"Total records to process: {total_records}")
print("Starting copy operation...\n")

# Process each document one by one
for document in source_collection.find():
    try:
        # Create new document with only the specified fields
        new_document = {}
        
        for field in fields_to_copy:
            if field in document:
                new_document[field] = document[field]
        
        # Insert the new document into the destination collection
        if new_document:  # Only insert if there's at least one field to copy
            destination_collection.insert_one(new_document)
            copied_count += 1
            
            # Print progress every 100 records
            if copied_count % 100 == 0:
                print(f"Copied {copied_count}/{total_records} records...")
        else:
            skipped_count += 1
            
    except Exception as e:
        print(f"Error processing document with _id {document.get('_id')}: {str(e)}")
        skipped_count += 1

# Print summary
print("\n" + "="*50)
print("Copy operation completed!")
print(f"Total records processed: {total_records}")
print(f"Successfully copied: {copied_count}")
print(f"Skipped/Failed: {skipped_count}")
print("="*50)

# Close the connection
client.close()