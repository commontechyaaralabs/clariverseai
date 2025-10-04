# EU Banking Social Media - Assign Stages to Records Without Stages Field
import os
import logging
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
SOCIAL_MEDIA_COLLECTION = "social_media"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def init_database():
    """Initialize database connection"""
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        social_media_col = db[SOCIAL_MEDIA_COLLECTION]
        logger.info("Database connection established")
        return client, social_media_col
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None, None

def assign_stages_to_records():
    """Assign stages to records that don't have the stages field"""
    
    # Initialize database
    client, social_media_col = init_database()
    if social_media_col is None:
        return
    
    try:
        # Define the stages in order
        stages = [
            "Receive",
            "Authenticate", 
            "Categorize",
            "Resolution",
            "Escalation",
            "Update",
            "Resolved",
            "Close",
            "Report"
        ]
        
        # Find records without stages field
        records_without_stages = list(social_media_col.find(
            {"stages": {"$exists": False}},
            {"_id": 1}
        ))
        
        total_records = len(records_without_stages)
        logger.info(f"Found {total_records} records without stages field")
        
        if total_records == 0:
            logger.info("No records found without stages field")
            return
        
        # Assign stages one by one, cycling through the stages list
        updated_count = 0
        
        for i, record in enumerate(records_without_stages):
            # Cycle through stages using modulo
            stage_to_assign = stages[i % len(stages)]
            
            # Update the record with the assigned stage
            result = social_media_col.update_one(
                {"_id": record["_id"]},
                {"$set": {"stages": stage_to_assign}}
            )
            
            if result.modified_count > 0:
                updated_count += 1
                logger.info(f"Record {i+1}/{total_records}: Assigned stage '{stage_to_assign}' to record {record['_id']}")
            else:
                logger.warning(f"Failed to update record {record['_id']}")
        
        # Verify the assignment
        logger.info(f"Successfully updated {updated_count} records")
        
        # Check final distribution
        logger.info("Final stage distribution:")
        for stage in stages:
            count = social_media_col.count_documents({"stages": stage})
            logger.info(f"  {stage}: {count} records")
        
        # Verify no records are left without stages
        remaining_without_stages = social_media_col.count_documents({"stages": {"$exists": False}})
        logger.info(f"Records still without stages: {remaining_without_stages}")
        
        if remaining_without_stages == 0:
            logger.info("✅ All records now have stages assigned!")
        else:
            logger.warning(f"⚠️ {remaining_without_stages} records still missing stages")
            
    except Exception as e:
        logger.error(f"Error during stage assignment: {e}")
    finally:
        if client:
            client.close()
            logger.info("Database connection closed")

def main():
    """Main function"""
    logger.info("Starting stage assignment for records without stages field...")
    assign_stages_to_records()
    logger.info("Stage assignment process completed!")

if __name__ == "__main__":
    main()
