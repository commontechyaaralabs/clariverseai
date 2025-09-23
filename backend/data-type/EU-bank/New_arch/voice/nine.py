import os
import random
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

def add_follow_up_fields():
    """
    Add follow_up_required, follow_up_date, and follow_up_reason fields to voice_transcripts collection
    based on the distribution table:
    
    Stage \ Msg count	26 msgs	28 msgs	30 msgs	36 msgs	40 msgs	Stage Total
    Escalation	25	25	37	74	84	245
    Update	15	15	23	46	54	153
    Resolved	9	9	14	28	32	92
    Resolution	6	6	9	18	22	61
    Close	3	3	5	9	11	31
    Receive	2	2	2	4	5	15
    Authenticate	1	1	1	2	3	8
    Categorize	0	1	1	1	2	5
    Report	0	0	0	1	1	2
    Totals	61	62	92	182	214	612
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
        
        # Define follow_up_required distribution by stage and message count
        follow_up_distribution = {
            26: {
                "Escalation": 25,
                "Update": 15,
                "Resolved": 9,
                "Resolution": 6,
                "Close": 3,
                "Receive": 2,
                "Authenticate": 1,
                "Categorize": 0,
                "Report": 0
            },
            28: {
                "Escalation": 25,
                "Update": 15,
                "Resolved": 9,
                "Resolution": 6,
                "Close": 3,
                "Receive": 2,
                "Authenticate": 1,
                "Categorize": 1,
                "Report": 0
            },
            30: {
                "Escalation": 37,
                "Update": 23,
                "Resolved": 14,
                "Resolution": 9,
                "Close": 5,
                "Receive": 2,
                "Authenticate": 1,
                "Categorize": 1,
                "Report": 0
            },
            36: {
                "Escalation": 74,
                "Update": 46,
                "Resolved": 28,
                "Resolution": 18,
                "Close": 9,
                "Receive": 4,
                "Authenticate": 2,
                "Categorize": 1,
                "Report": 1
            },
            40: {
                "Escalation": 84,
                "Update": 54,
                "Resolved": 32,
                "Resolution": 22,
                "Close": 11,
                "Receive": 5,
                "Authenticate": 3,
                "Categorize": 2,
                "Report": 1
            }
        }
        
        # First, set all documents to follow_up_required: "no" and add other fields
        logger.info("Setting all documents to follow_up_required: 'no' and adding follow_up fields...")
        result = voice_transcripts_collection.update_many(
            {},
            {
                "$set": {
                    "follow_up_required": "no",
                    "follow_up_date": None,
                    "follow_up_reason": None
                }
            }
        )
        logger.info(f"Set {result.modified_count} documents with follow_up fields")
        
        # Get all documents grouped by message count and stage
        documents_by_msg_count_and_stage = {}
        all_documents = list(voice_transcripts_collection.find({}))
        
        for doc in all_documents:
            msg_count = doc.get('thread', {}).get('message_count', 0)
            stage = doc.get('stages', 'Unknown')
            
            if msg_count not in documents_by_msg_count_and_stage:
                documents_by_msg_count_and_stage[msg_count] = {}
            if stage not in documents_by_msg_count_and_stage[msg_count]:
                documents_by_msg_count_and_stage[msg_count][stage] = []
            
            documents_by_msg_count_and_stage[msg_count][stage].append(doc)
        
        logger.info("Documents grouped by message count and stage:")
        for msg_count, stages in documents_by_msg_count_and_stage.items():
            logger.info(f"  {msg_count} messages:")
            for stage, docs in stages.items():
                logger.info(f"    {stage}: {len(docs)} documents")
        
        # Process each message count group
        total_follow_up_updated = 0
        
        for msg_count, stages in documents_by_msg_count_and_stage.items():
            if msg_count not in follow_up_distribution:
                logger.warning(f"No follow_up distribution defined for {msg_count} messages, skipping")
                continue
            
            logger.info(f"Processing {msg_count} messages for follow_up_required assignment")
            
            # Get follow_up distribution for this message count
            follow_up_counts = follow_up_distribution[msg_count]
            
            # Process each stage for this message count
            for stage, follow_up_count in follow_up_counts.items():
                if stage not in stages:
                    logger.warning(f"No documents found for stage '{stage}' with {msg_count} messages")
                    continue
                
                available_docs = stages[stage]
                logger.info(f"  Stage '{stage}': {len(available_docs)} available, {follow_up_count} to mark as follow_up_required: 'yes'")
                
                if len(available_docs) < follow_up_count:
                    logger.warning(f"Not enough documents for stage '{stage}' with {msg_count} messages. Available: {len(available_docs)}, Required: {follow_up_count}")
                    follow_up_count = len(available_docs)
                
                # Randomly select documents to mark as follow_up_required: "yes"
                random.shuffle(available_docs)
                follow_up_docs = available_docs[:follow_up_count]
                
                # Update selected documents to follow_up_required: "yes"
                for doc in follow_up_docs:
                    try:
                        result = voice_transcripts_collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {"follow_up_required": "yes"}}
                        )
                        
                        if result.modified_count > 0:
                            total_follow_up_updated += 1
                            logger.debug(f"Updated document {doc['_id']} to follow_up_required: 'yes'")
                        else:
                            logger.warning(f"Failed to update document {doc['_id']}")
                    
                    except Exception as e:
                        logger.error(f"Error updating document {doc.get('_id')}: {str(e)}")
                        continue
                
                logger.info(f"    Marked {follow_up_count} documents as follow_up_required: 'yes' for stage '{stage}'")
        
        logger.info(f"Successfully updated {total_follow_up_updated} documents to follow_up_required: 'yes'")
        
        # Verify the transformation
        logger.info("Verification - Follow_up_required distribution:")
        pipeline = [
            {"$group": {
                "_id": "$follow_up_required",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        follow_up_stats = list(voice_transcripts_collection.aggregate(pipeline))
        for stat in follow_up_stats:
            logger.info(f"  {stat['_id']}: {stat['count']} documents")
        
        # Show detailed breakdown by message count and stage
        logger.info("Detailed follow_up_required breakdown by message count and stage:")
        for msg_count in [26, 28, 30, 36, 40]:
            logger.info(f"  {msg_count} messages:")
            for stage in ["Escalation", "Update", "Resolved", "Resolution", "Close", "Receive", "Authenticate", "Categorize", "Report"]:
                follow_up_count = voice_transcripts_collection.count_documents({
                    "thread.message_count": msg_count,
                    "stages": stage,
                    "follow_up_required": "yes"
                })
                total_count = voice_transcripts_collection.count_documents({
                    "thread.message_count": msg_count,
                    "stages": stage
                })
                logger.info(f"    {stage}: {follow_up_count}/{total_count} follow_up_required")
        
        # Show sample documents
        sample_follow_up_yes = voice_transcripts_collection.find_one({"follow_up_required": "yes"})
        sample_follow_up_no = voice_transcripts_collection.find_one({"follow_up_required": "no"})
        
        if sample_follow_up_yes:
            logger.info("Sample document with follow_up_required: 'yes':")
            logger.info(f"  Document ID: {sample_follow_up_yes['_id']}")
            logger.info(f"  Message count: {sample_follow_up_yes.get('thread', {}).get('message_count')}")
            logger.info(f"  Stage: {sample_follow_up_yes.get('stages')}")
            logger.info(f"  Follow_up_required: {sample_follow_up_yes.get('follow_up_required')}")
            logger.info(f"  Follow_up_date: {sample_follow_up_yes.get('follow_up_date')}")
            logger.info(f"  Follow_up_reason: {sample_follow_up_yes.get('follow_up_reason')}")
        
        if sample_follow_up_no:
            logger.info("Sample document with follow_up_required: 'no':")
            logger.info(f"  Document ID: {sample_follow_up_no['_id']}")
            logger.info(f"  Message count: {sample_follow_up_no.get('thread', {}).get('message_count')}")
            logger.info(f"  Stage: {sample_follow_up_no.get('stages')}")
            logger.info(f"  Follow_up_required: {sample_follow_up_no.get('follow_up_required')}")
            logger.info(f"  Follow_up_date: {sample_follow_up_no.get('follow_up_date')}")
            logger.info(f"  Follow_up_reason: {sample_follow_up_no.get('follow_up_reason')}")
        
        logger.info("Follow_up fields addition completed successfully")
        
    except Exception as e:
        logger.error(f"Error adding follow_up fields: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    add_follow_up_fields()
