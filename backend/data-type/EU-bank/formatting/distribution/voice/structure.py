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
voice_transcripts_collection = db['voice_transcripts']
voice_new_collection = db['voice_new']

def transform_document(doc):
    """
    Transform a document from voice_transcripts collection:
    - Remove thread.createdDateTime and thread.lastUpdatedDateTime
    - Make message.[].body.content and message.[].createdDateTime null
    - Remove specified fields
    """
    # Create a copy of the document
    transformed_doc = doc.copy()
    
    # Remove thread.createdDateTime and thread.lastUpdatedDateTime
    if 'thread' in transformed_doc:
        if 'createdDateTime' in transformed_doc['thread']:
            del transformed_doc['thread']['createdDateTime']
        if 'lastUpdatedDateTime' in transformed_doc['thread']:
            del transformed_doc['thread']['lastUpdatedDateTime']
    
    # Make message.[].body.content and message.[].createdDateTime null
    if 'messages' in transformed_doc:
        for message in transformed_doc['messages']:
            if 'body' in message and 'content' in message['body']:
                message['body']['content'] = None
            if 'createdDateTime' in message:
                message['createdDateTime'] = None
    
    # Remove specified fields from the root level
    fields_to_remove = [
        'stages', 
        'follow_up_date', 
        'follow_up_reason', 
        'follow_up_required', 
        'action_pending_status', 
        'action_pending_from', 
        'call_started', 
        'call_summary', 
        'llm_model_used',
        'llm_processed', 
        'llm_processed_at', 
        'next_action_suggestion', 
        'overall_sentiment', 
        'resolution_status', 
        'sentiment'
    ]
    
    for field in fields_to_remove:
        if field in transformed_doc:
            del transformed_doc[field]
    
    return transformed_doc

def copy_and_transform_collection():
    """
    Copy voice_transcripts collection to voice_new with transformations
    """
    print("Starting collection copy and transformation...")
    print(f"Source collection: voice_transcripts")
    print(f"Target collection: voice_new")
    
    # Check if voice_new collection exists and drop it
    if 'voice_new' in db.list_collection_names():
        print("voice_new collection exists. Dropping it...")
        voice_new_collection.drop()
        print("Dropped existing voice_new collection.")
    
    # Get all documents from voice_transcripts
    total_docs = voice_transcripts_collection.count_documents({})
    print(f"\nTotal documents in voice_transcripts: {total_docs}")
    
    if total_docs == 0:
        print("No documents found in voice_transcripts collection.")
        return
    
    # Process and insert documents in batches
    batch_size = 100
    processed_count = 0
    
    cursor = voice_transcripts_collection.find({})
    batch = []
    
    for doc in cursor:
        # Transform the document
        transformed_doc = transform_document(doc)
        batch.append(transformed_doc)
        
        # Insert batch when it reaches batch_size
        if len(batch) >= batch_size:
            voice_new_collection.insert_many(batch)
            processed_count += len(batch)
            print(f"Processed {processed_count}/{total_docs} documents...")
            batch = []
    
    # Insert remaining documents
    if batch:
        voice_new_collection.insert_many(batch)
        processed_count += len(batch)
        print(f"Processed {processed_count}/{total_docs} documents...")
    
    print(f"\n✓ Successfully copied and transformed {processed_count} documents to voice_new collection!")
    
    # Verify the new collection
    new_total = voice_new_collection.count_documents({})
    print(f"✓ Verification: voice_new collection has {new_total} documents")
    
    # Show a sample transformed document
    print("\n--- Sample transformed document ---")
    sample_doc = voice_new_collection.find_one({})
    if sample_doc:
        print(json.dumps(sample_doc, indent=2, default=str))

# Main execution
if __name__ == "__main__":
    try:
        copy_and_transform_collection()
        print("\n✓ Process completed successfully!")
    except Exception as e:
        print(f"\n✗ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Close MongoDB connection
        client.close()
        print("\nMongoDB connection closed.")
