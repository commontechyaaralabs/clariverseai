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

def remove_records_without_subcluster_label():
    """Remove records from social_media collection where subcluster_label is missing or empty"""
    client, db = connect_to_mongodb()
    
    try:
        # Get the social_media collection
        collection = db['social_media']
        
        # Count documents before cleanup
        total_docs_before = collection.count_documents({})
        logger.info(f"Found {total_docs_before} documents in social_media collection")
        
        if total_docs_before == 0:
            logger.warning("No documents found in social_media collection")
            return
        
        # First, let's check what subcluster_label values exist
        logger.info("Checking subcluster_label field values...")
        
        # Count documents with different subcluster_label conditions
        no_field = collection.count_documents({"subcluster_label": {"$exists": False}})
        null_field = collection.count_documents({"subcluster_label": None})
        empty_field = collection.count_documents({"subcluster_label": ""})
        whitespace_field = collection.count_documents({"subcluster_label": {"$regex": "^\\s*$"}})
        valid_field = collection.count_documents({
            "subcluster_label": {
                "$exists": True,
                "$ne": None,
                "$ne": "",
                "$not": {"$regex": "^\\s*$"}
            }
        })
        
        logger.info(f"Documents without subcluster_label field: {no_field}")
        logger.info(f"Documents with subcluster_label = null: {null_field}")
        logger.info(f"Documents with subcluster_label = '': {empty_field}")
        logger.info(f"Documents with subcluster_label = whitespace: {whitespace_field}")
        logger.info(f"Documents with valid subcluster_label: {valid_field}")
        
        # Show sample documents to understand the data structure
        sample_docs = list(collection.find({}).limit(3))
        logger.info("Sample documents from collection:")
        for i, doc in enumerate(sample_docs, 1):
            logger.info(f"Document {i}:")
            logger.info(f"  - _id: {doc.get('_id')}")
            logger.info(f"  - email_id: {doc.get('email_id', 'N/A')}")
            logger.info(f"  - username: {doc.get('username', 'N/A')}")
            logger.info(f"  - channel: {doc.get('channel', 'N/A')}")
            logger.info(f"  - subcluster_label: '{doc.get('subcluster_label', 'FIELD_NOT_EXISTS')}'")
            logger.info(f"  - All fields: {list(doc.keys())}")
            logger.info("  ---")
        
        # Try different deletion approaches
        total_deleted = 0
        
        # Approach 1: Delete documents where field doesn't exist
        if no_field > 0:
            logger.info(f"Deleting {no_field} documents where subcluster_label field doesn't exist...")
            result1 = collection.delete_many({"subcluster_label": {"$exists": False}})
            total_deleted += result1.deleted_count
            logger.info(f"Deleted {result1.deleted_count} documents (field doesn't exist)")
        
        # Approach 2: Delete documents where field is null
        if null_field > 0:
            logger.info(f"Deleting {null_field} documents where subcluster_label is null...")
            result2 = collection.delete_many({"subcluster_label": None})
            total_deleted += result2.deleted_count
            logger.info(f"Deleted {result2.deleted_count} documents (field is null)")
        
        # Approach 3: Delete documents where field is empty string
        if empty_field > 0:
            logger.info(f"Deleting {empty_field} documents where subcluster_label is empty string...")
            result3 = collection.delete_many({"subcluster_label": ""})
            total_deleted += result3.deleted_count
            logger.info(f"Deleted {result3.deleted_count} documents (field is empty string)")
        
        # Approach 4: Delete documents where field contains only whitespace
        if whitespace_field > 0:
            logger.info(f"Deleting {whitespace_field} documents where subcluster_label is whitespace...")
            result4 = collection.delete_many({"subcluster_label": {"$regex": "^\\s*$"}})
            total_deleted += result4.deleted_count
            logger.info(f"Deleted {result4.deleted_count} documents (field is whitespace)")
        
        # Count documents after cleanup
        total_docs_after = collection.count_documents({})
        logger.info(f"Documents before deletion: {total_docs_before}")
        logger.info(f"Documents after deletion: {total_docs_after}")
        logger.info(f"Total documents removed: {total_deleted}")
        
        # Final verification
        remaining_invalid = collection.count_documents({
            "$or": [
                {"subcluster_label": {"$exists": False}},
                {"subcluster_label": None},
                {"subcluster_label": ""},
                {"subcluster_label": {"$regex": "^\\s*$"}}
            ]
        })
        
        if remaining_invalid == 0:
            logger.info("✓ All documents without valid subcluster_label have been removed")
        else:
            logger.warning(f"Warning: {remaining_invalid} documents without valid subcluster_label still exist")
        
    except Exception as e:
        logger.error(f"Deletion failed: {e}")
        raise
    finally:
        client.close()
        logger.info("MongoDB connection closed")

def verify_cleanup():
    """Verify that all remaining documents have subcluster_label"""
    client, db = connect_to_mongodb()
    
    try:
        collection = db['social_media']
        
        # Count total documents
        total_docs = collection.count_documents({})
        logger.info(f"Total documents remaining: {total_docs}")
        
        # Count documents with valid subcluster_label
        valid_subcluster_query = {
            "subcluster_label": {
                "$exists": True,
                "$ne": None,
                "$ne": "",
                "$not": {"$regex": "^\\s*$"}
            }
        }
        
        valid_docs = collection.count_documents(valid_subcluster_query)
        logger.info(f"Documents with valid subcluster_label: {valid_docs}")
        
        # Count documents without subcluster_label (should be 0)
        invalid_subcluster_query = {
            "$or": [
                {"subcluster_label": {"$exists": False}},
                {"subcluster_label": None},
                {"subcluster_label": ""},
                {"subcluster_label": {"$regex": "^\\s*$"}}
            ]
        }
        
        invalid_docs = collection.count_documents(invalid_subcluster_query)
        logger.info(f"Documents without valid subcluster_label: {invalid_docs}")
        
        if invalid_docs == 0:
            logger.info("✓ All remaining documents have valid subcluster_label")
        else:
            logger.warning(f"Warning: {invalid_docs} documents still lack valid subcluster_label")
        
        # Show sample of remaining documents
        sample_docs = list(collection.find({}).limit(3))
        logger.info("Sample remaining documents:")
        for i, doc in enumerate(sample_docs, 1):
            logger.info(f"Document {i}:")
            logger.info(f"  - email_id: {doc.get('email_id', 'N/A')}")
            logger.info(f"  - username: {doc.get('username', 'N/A')}")
            logger.info(f"  - channel: {doc.get('channel', 'N/A')}")
            logger.info(f"  - subcluster_label: '{doc.get('subcluster_label', 'N/A')}'")
            logger.info("  ---")
            
    except Exception as e:
        logger.error(f"Verification failed: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    try:
        logger.info("Starting removal of records without subcluster_label...")
        remove_records_without_subcluster_label()
        logger.info("Record removal completed successfully!")
        
        # Verify the cleanup
        logger.info("Verifying cleanup...")
        verify_cleanup()
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        exit(1)