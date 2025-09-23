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

def convert_urgency_to_boolean():
    """
    Convert urgency field from string values ("yes"/"no") to boolean values (true/false)
    in voice_transcripts collection
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        # Get voice_transcripts collection
        voice_transcripts_collection = db['voice_transcripts']
        
        logger.info("Connected to MongoDB successfully")
        
        # Check if collection exists and has documents
        total_docs = voice_transcripts_collection.count_documents({})
        if total_docs == 0:
            logger.warning("No documents found in voice_transcripts collection")
            return
        
        logger.info(f"Found {total_docs} documents in voice_transcripts collection")
        
        # Check current urgency field distribution
        logger.info("Current urgency field distribution:")
        pipeline = [
            {"$group": {
                "_id": "$urgency",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        current_stats = list(voice_transcripts_collection.aggregate(pipeline))
        for stat in current_stats:
            logger.info(f"  {stat['_id']}: {stat['count']} documents")
        
        # Convert "yes" to true (boolean)
        logger.info("Converting 'yes' values to true (boolean)...")
        result_yes = voice_transcripts_collection.update_many(
            {"urgency": "yes"},
            {"$set": {"urgency": True}}
        )
        logger.info(f"Updated {result_yes.modified_count} documents from 'yes' to true")
        
        # Convert "no" to false (boolean)
        logger.info("Converting 'no' values to false (boolean)...")
        result_no = voice_transcripts_collection.update_many(
            {"urgency": "no"},
            {"$set": {"urgency": False}}
        )
        logger.info(f"Updated {result_no.modified_count} documents from 'no' to false")
        
        # Handle any other string values that might exist
        logger.info("Checking for any other urgency values...")
        other_values = voice_transcripts_collection.distinct("urgency")
        logger.info(f"Found urgency values: {other_values}")
        
        # Convert any remaining string values to false
        string_values = [val for val in other_values if isinstance(val, str)]
        if string_values:
            logger.info(f"Converting remaining string values {string_values} to false...")
            for string_val in string_values:
                result_other = voice_transcripts_collection.update_many(
                    {"urgency": string_val},
                    {"$set": {"urgency": False}}
                )
                logger.info(f"Updated {result_other.modified_count} documents from '{string_val}' to false")
        
        # Verify the conversion
        logger.info("Verification - Final urgency field distribution:")
        final_stats = list(voice_transcripts_collection.aggregate(pipeline))
        for stat in final_stats:
            value_type = type(stat['_id']).__name__
            logger.info(f"  {stat['_id']} ({value_type}): {stat['count']} documents")
        
        # Show sample documents
        sample_true = voice_transcripts_collection.find_one({"urgency": True})
        sample_false = voice_transcripts_collection.find_one({"urgency": False})
        
        if sample_true:
            logger.info("Sample document with urgency: true:")
            logger.info(f"  Document ID: {sample_true['_id']}")
            logger.info(f"  Message count: {sample_true.get('thread', {}).get('message_count')}")
            logger.info(f"  Stage: {sample_true.get('stages')}")
            logger.info(f"  Urgency: {sample_true.get('urgency')} (type: {type(sample_true.get('urgency')).__name__})")
        
        if sample_false:
            logger.info("Sample document with urgency: false:")
            logger.info(f"  Document ID: {sample_false['_id']}")
            logger.info(f"  Message count: {sample_false.get('thread', {}).get('message_count')}")
            logger.info(f"  Stage: {sample_false.get('stages')}")
            logger.info(f"  Urgency: {sample_false.get('urgency')} (type: {type(sample_false.get('urgency')).__name__})")
        
        # Count final distribution
        true_count = voice_transcripts_collection.count_documents({"urgency": True})
        false_count = voice_transcripts_collection.count_documents({"urgency": False})
        total_processed = true_count + false_count
        
        logger.info(f"Final summary:")
        logger.info(f"  urgency: true - {true_count} documents")
        logger.info(f"  urgency: false - {false_count} documents")
        logger.info(f"  Total processed - {total_processed} documents")
        
        if total_processed == total_docs:
            logger.info("✓ All documents successfully converted to boolean values")
        else:
            logger.warning(f"⚠ Some documents may not have been processed. Expected: {total_docs}, Processed: {total_processed}")
        
        logger.info("Urgency field conversion to boolean completed successfully")
        
    except Exception as e:
        logger.error(f"Error converting urgency field to boolean: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    convert_urgency_to_boolean()
