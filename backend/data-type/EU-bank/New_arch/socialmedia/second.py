# Import required libraries
from pymongo import MongoClient
from collections import defaultdict
import re
import os
from dotenv import load_dotenv
from bson import ObjectId
from datetime import datetime
import logging

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def connect_to_mongodb():
    """Connect to MongoDB and return client and database objects"""
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        logger.info(f"Successfully connected to MongoDB database: {MONGO_DATABASE_NAME}")
        return client, db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def remove_metadata_fields():
    """Remove metadata fields from social_media collection"""
    client, db = connect_to_mongodb()
    
    try:
        # Get the social_media collection
        collection = db['social_media']
        
        # Count documents before cleanup
        total_docs = collection.count_documents({})
        logger.info(f"Found {total_docs} documents in social_media collection")
        
        if total_docs == 0:
            logger.warning("No documents found in social_media collection")
            return
        
        # Fields to remove
        fields_to_remove = [
            'original_id',
            'migration_timestamp', 
            'migration_source'
        ]
        
        # Update all documents to remove these fields
        result = collection.update_many(
            {},  # Match all documents
            {"$unset": {field: "" for field in fields_to_remove}}
        )
        
        logger.info(f"Successfully removed metadata fields from {result.modified_count} documents")
        
        # Verify the cleanup by checking a sample document
        sample_doc = collection.find_one({})
        if sample_doc:
            logger.info("Sample document after cleanup:")
            for key, value in sample_doc.items():
                if key != '_id':  # Skip MongoDB's _id field
                    logger.info(f"  - {key}: {value}")
            
            # Check if metadata fields were removed
            removed_fields = [field for field in fields_to_remove if field in sample_doc]
            if removed_fields:
                logger.warning(f"Warning: Some metadata fields still exist: {removed_fields}")
            else:
                logger.info("✓ All metadata fields successfully removed")
        
        # Final count
        final_count = collection.count_documents({})
        logger.info(f"Final document count: {final_count}")
        
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")
        raise
    finally:
        client.close()
        logger.info("MongoDB connection closed")

def verify_cleanup():
    """Verify that metadata fields have been removed"""
    client, db = connect_to_mongodb()
    
    try:
        collection = db['social_media']
        
        # Check if any documents still have metadata fields
        metadata_fields = ['original_id', 'migration_timestamp', 'migration_source']
        
        for field in metadata_fields:
            count = collection.count_documents({field: {"$exists": True}})
            if count > 0:
                logger.warning(f"Found {count} documents still containing '{field}' field")
            else:
                logger.info(f"✓ No documents contain '{field}' field")
        
        # Show sample of remaining fields
        sample_doc = collection.find_one({})
        if sample_doc:
            remaining_fields = [key for key in sample_doc.keys() if key != '_id']
            logger.info(f"Remaining fields in documents: {remaining_fields}")
            
    except Exception as e:
        logger.error(f"Verification failed: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    try:
        logger.info("Starting metadata fields removal from social_media collection...")
        remove_metadata_fields()
        logger.info("Metadata fields removal completed successfully!")
        
        # Verify the cleanup
        logger.info("Verifying cleanup...")
        verify_cleanup()
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        exit(1)