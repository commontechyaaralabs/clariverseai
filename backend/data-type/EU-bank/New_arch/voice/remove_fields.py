import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get connection details from environment variables or use fallback
mongo_connection_string = os.getenv('MONGO_CONNECTION_STRING', "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
mongo_database_name = os.getenv('MONGO_DATABASE_NAME', "sparzaai")

def remove_fields_from_voice_transcripts():
    """
    Remove specified fields from voice_transcripts collection
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        # Get voice_transcripts collection
        voice_transcripts_collection = db['voice_transcripts']
        
        logger.info("Connected to MongoDB successfully")
        
        # Fields to remove
        fields_to_remove = [
            "customer_name",
            "customer_id", 
            "email",
            "call_purpose",
            "priority",
            "resolution_status",
            "sentiment",
            "urgency",
            "timestamp"
        ]
        
        # Check if collection exists and has documents
        total_docs = voice_transcripts_collection.count_documents({})
        if total_docs == 0:
            logger.warning("No documents found in voice_transcripts collection")
            return
        
        logger.info(f"Found {total_docs} documents in voice_transcripts collection")
        
        # Create unset operation to remove the fields
        unset_operation = {field: "" for field in fields_to_remove}
        
        # Update all documents to remove the specified fields
        logger.info("Removing specified fields from all documents...")
        result = voice_transcripts_collection.update_many(
            {},  # Empty filter to match all documents
            {"$unset": unset_operation}
        )
        
        logger.info(f"Successfully updated {result.modified_count} documents")
        logger.info(f"Fields removed: {', '.join(fields_to_remove)}")
        
        # Verify the removal by checking a sample document
        sample_doc = voice_transcripts_collection.find_one({})
        if sample_doc:
            logger.info("Sample document after field removal:")
            for field in fields_to_remove:
                if field in sample_doc:
                    logger.warning(f"Field '{field}' still exists in sample document")
                else:
                    logger.info(f"Field '{field}' successfully removed")
        
        logger.info("Field removal completed successfully")
        
    except Exception as e:
        logger.error(f"Error removing fields from voice_transcripts collection: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    remove_fields_from_voice_transcripts()
