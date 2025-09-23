import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import random
from collections import Counter
from itertools import combinations

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get connection details from environment variables
mongo_connection_string = os.getenv('MONGO_CONNECTION_STRING')
mongo_database_name = os.getenv('MONGO_DATABASE_NAME')

def copy_fields_to_voice_transcripts():
    """
    Copy specified fields from voice collection to voice_transcripts collection
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        # Get collections
        voice_collection = db['voice']
        voice_transcripts_collection = db['voice_transcripts']
        
        logger.info("Connected to MongoDB successfully")
        
        # Fields to copy
        fields_to_copy = [
            "call_id",
            "customer_name", 
            "email",
            "customer_id",
            "dominant_topic",
            "subtopics",
            "kmeans_cluster_id",
            "subcluster_id",
            "subcluster_label",
            "kmeans_cluster_keyphrase",
            "domain"
        ]
        
        # Create projection for MongoDB query (include _id and specified fields)
        projection = {field: 1 for field in fields_to_copy}
        projection["_id"] = 1  # Include _id for reference
        
        # Get all documents from voice collection with only required fields
        logger.info("Fetching documents from voice collection...")
        voice_documents = list(voice_collection.find({}, projection))
        
        if not voice_documents:
            logger.warning("No documents found in voice collection")
            return
        
        logger.info(f"Found {len(voice_documents)} documents in voice collection")
        
        # Prepare documents for insertion into voice_transcripts
        documents_to_insert = []
        
        for doc in voice_documents:
            # Create new document with only the required fields
            new_doc = {}
            
            # Copy each specified field if it exists
            for field in fields_to_copy:
                if field in doc:
                    new_doc[field] = doc[field]
                else:
                    logger.warning(f"Field '{field}' not found in document with _id: {doc['_id']}")
            
            # Add the document to insert list if it has data
            if new_doc:
                documents_to_insert.append(new_doc)
        
        if not documents_to_insert:
            logger.warning("No valid documents to insert")
            return
        
        # Check if voice_transcripts collection already has documents
        existing_count = voice_transcripts_collection.count_documents({})
        if existing_count > 0:
            logger.info(f"voice_transcripts collection already has {existing_count} documents")
            
            # Option 1: Clear existing collection (uncomment if you want to replace all data)
            # voice_transcripts_collection.delete_many({})
            # logger.info("Cleared existing documents from voice_transcripts collection")
            
            # Option 2: Skip documents that already exist based on call_id (recommended)
            existing_call_ids = set()
            existing_docs = voice_transcripts_collection.find({}, {"call_id": 1})
            for doc in existing_docs:
                if "call_id" in doc:
                    existing_call_ids.add(doc["call_id"])
            
            # Filter out documents that already exist
            original_count = len(documents_to_insert)
            documents_to_insert = [doc for doc in documents_to_insert 
                                 if doc.get("call_id") not in existing_call_ids]
            
            logger.info(f"Filtered out {original_count - len(documents_to_insert)} existing documents")
        
        if not documents_to_insert:
            logger.info("No new documents to insert - all documents already exist")
            return
        
        # Insert documents into voice_transcripts collection
        logger.info(f"Inserting {len(documents_to_insert)} documents into voice_transcripts collection...")
        
        # Insert in batches to handle large datasets efficiently
        batch_size = 1000
        total_inserted = 0
        
        for i in range(0, len(documents_to_insert), batch_size):
            batch = documents_to_insert[i:i + batch_size]
            result = voice_transcripts_collection.insert_many(batch)
            total_inserted += len(result.inserted_ids)
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} documents")
        
        logger.info(f"Successfully copied {total_inserted} documents to voice_transcripts collection")
        
        # Verify the operation
        final_count = voice_transcripts_collection.count_documents({})
        logger.info(f"Total documents in voice_transcripts collection: {final_count}")
        
        # Show sample of copied data
        sample_doc = voice_transcripts_collection.find_one()
        if sample_doc:
            logger.info("Sample document from voice_transcripts:")
            for field in fields_to_copy:
                if field in sample_doc:
                    logger.info(f"  {field}: {sample_doc[field]}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise
    
    finally:
        # Close MongoDB connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def verify_copy_operation():
    """
    Verify that the copy operation was successful
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        voice_collection = db['voice']
        voice_transcripts_collection = db['voice_transcripts']
        
        # Get counts
        voice_count = voice_collection.count_documents({})
        transcripts_count = voice_transcripts_collection.count_documents({})
        
        logger.info(f"Voice collection count: {voice_count}")
        logger.info(f"Voice transcripts collection count: {transcripts_count}")
        
        # Check if all call_ids are present
        voice_call_ids = set()
        for doc in voice_collection.find({}, {"call_id": 1}):
            if "call_id" in doc:
                voice_call_ids.add(doc["call_id"])
        
        transcript_call_ids = set()
        for doc in voice_transcripts_collection.find({}, {"call_id": 1}):
            if "call_id" in doc:
                transcript_call_ids.add(doc["call_id"])
        
        missing_call_ids = voice_call_ids - transcript_call_ids
        if missing_call_ids:
            logger.warning(f"Missing call_ids in voice_transcripts: {missing_call_ids}")
        else:
            logger.info("All call_ids successfully copied")
            
    except Exception as e:
        logger.error(f"Error during verification: {str(e)}")
    
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    try:
        logger.info("Starting copy operation from voice to voice_transcripts collection")
        copy_fields_to_voice_transcripts()
        
        logger.info("Verifying copy operation...")
        verify_copy_operation()
        
        logger.info("Copy operation completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")