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

def remove_fields_from_tickets():
    """
    Remove specified fields from all documents in the tickets collection
    Fields to remove: description, priority, urgency, title, embeddings
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        tickets_collection = db['tickets']
        
        logger.info("Connected to MongoDB successfully")
        
        # Define fields to remove
        fields_to_remove = {
            "description": "",
            "priority": "",
            "urgency": "",
            "title": "",
            "embeddings": ""
        }
        
        # Get initial count
        initial_count = tickets_collection.count_documents({})
        logger.info(f"Total documents in tickets collection: {initial_count}")
        
        # Remove specified fields from all documents
        result = tickets_collection.update_many(
            {},  # Empty filter matches all documents
            {"$unset": fields_to_remove}  # $unset removes the specified fields
        )
        
        logger.info(f"Modified {result.modified_count} documents")
        logger.info("Successfully removed fields: description, priority, urgency, title, embeddings")
        
        # Verify the operation by checking a sample document
        sample_doc = tickets_collection.find_one({})
        if sample_doc:
            remaining_fields = list(sample_doc.keys())
            logger.info(f"Remaining fields in sample document: {remaining_fields}")
            
            # Check if any of the removed fields still exist
            removed_fields = ['description', 'priority', 'urgency', 'title', 'embeddings']
            still_present = [field for field in removed_fields if field in remaining_fields]
            
            if still_present:
                logger.warning(f"Some fields were not removed: {still_present}")
            else:
                logger.info("All specified fields have been successfully removed")
        
        # Close the connection
        client.close()
        logger.info("MongoDB connection closed")
        
        return result.modified_count
        
    except Exception as e:
        logger.error(f"Error occurred while removing fields: {str(e)}")
        return None

def verify_field_removal():
    """
    Verify that the specified fields have been removed from all documents
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        tickets_collection = db['tickets']
        
        # Fields that should be removed
        removed_fields = ['description', 'priority', 'urgency', 'title', 'embeddings']
        
        # Check if any documents still contain these fields
        for field in removed_fields:
            count = tickets_collection.count_documents({field: {"$exists": True}})
            if count > 0:
                logger.warning(f"Field '{field}' still exists in {count} documents")
            else:
                logger.info(f"Field '{field}' successfully removed from all documents")
        
        # Show structure of a sample document after removal
        sample_doc = tickets_collection.find_one({})
        if sample_doc:
            logger.info("Sample document structure after field removal:")
            for key in sample_doc.keys():
                logger.info(f"  - {key}")
        
        client.close()
        
    except Exception as e:
        logger.error(f"Error occurred during verification: {str(e)}")

# Main execution
if __name__ == "__main__":
    logger.info("Starting field removal process...")
    
    # Remove fields from all documents
    modified_count = remove_fields_from_tickets()
    
    if modified_count is not None:
        logger.info(f"Process completed. Modified {modified_count} documents.")
        
        # Verify the removal
        logger.info("Verifying field removal...")
        verify_field_removal()
    else:
        logger.error("Field removal process failed.")