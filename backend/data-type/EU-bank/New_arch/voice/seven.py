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

def add_urgency_field():
    """
    Add urgency field to voice_transcripts collection based on the distribution table:
    
    Stage \ Msg count	26 msgs	28 msgs	30 msgs	36 msgs	40 msgs	Stage Total (urgent)
    Escalation	23	23	33	68	79	226
    Receive	5	4	3	3	2	17
    Authenticate	3	4	5	5	4	21
    Categorize	3	3	3	3	2	14
    Resolution	2	3	4	6	6	21
    Update	2	2	3	3	4	14
    Resolved	2	2	3	5	5	17
    Close	2	2	2	2	2	10
    Report	1	1	1	2	2	7
    Totals	43	44	57	97	106	347
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
        
        # Define urgent records distribution by stage and message count
        urgent_distribution = {
            26: {
                "Escalation": 23,
                "Receive": 5,
                "Authenticate": 3,
                "Categorize": 3,
                "Resolution": 2,
                "Update": 2,
                "Resolved": 2,
                "Close": 2,
                "Report": 1
            },
            28: {
                "Escalation": 23,
                "Receive": 4,
                "Authenticate": 4,
                "Categorize": 3,
                "Resolution": 3,
                "Update": 2,
                "Resolved": 2,
                "Close": 2,
                "Report": 1
            },
            30: {
                "Escalation": 33,
                "Receive": 3,
                "Authenticate": 5,
                "Categorize": 3,
                "Resolution": 4,
                "Update": 3,
                "Resolved": 3,
                "Close": 2,
                "Report": 1
            },
            36: {
                "Escalation": 68,
                "Receive": 3,
                "Authenticate": 5,
                "Categorize": 3,
                "Resolution": 6,
                "Update": 3,
                "Resolved": 5,
                "Close": 2,
                "Report": 2
            },
            40: {
                "Escalation": 79,
                "Receive": 2,
                "Authenticate": 4,
                "Categorize": 2,
                "Resolution": 6,
                "Update": 4,
                "Resolved": 5,
                "Close": 2,
                "Report": 2
            }
        }
        
        # First, set all documents to urgency: "no"
        logger.info("Setting all documents to urgency: 'no'...")
        result = voice_transcripts_collection.update_many(
            {},
            {"$set": {"urgency": "no"}}
        )
        logger.info(f"Set {result.modified_count} documents to urgency: 'no'")
        
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
        total_urgent_updated = 0
        
        for msg_count, stages in documents_by_msg_count_and_stage.items():
            if msg_count not in urgent_distribution:
                logger.warning(f"No urgent distribution defined for {msg_count} messages, skipping")
                continue
            
            logger.info(f"Processing {msg_count} messages for urgent assignment")
            
            # Get urgent distribution for this message count
            urgent_counts = urgent_distribution[msg_count]
            
            # Process each stage for this message count
            for stage, urgent_count in urgent_counts.items():
                if stage not in stages:
                    logger.warning(f"No documents found for stage '{stage}' with {msg_count} messages")
                    continue
                
                available_docs = stages[stage]
                logger.info(f"  Stage '{stage}': {len(available_docs)} available, {urgent_count} to mark as urgent")
                
                if len(available_docs) < urgent_count:
                    logger.warning(f"Not enough documents for stage '{stage}' with {msg_count} messages. Available: {len(available_docs)}, Required: {urgent_count}")
                    urgent_count = len(available_docs)
                
                # Randomly select documents to mark as urgent
                random.shuffle(available_docs)
                urgent_docs = available_docs[:urgent_count]
                
                # Update selected documents to urgency: "yes"
                for doc in urgent_docs:
                    try:
                        result = voice_transcripts_collection.update_one(
                            {"_id": doc["_id"]},
                            {"$set": {"urgency": "yes"}}
                        )
                        
                        if result.modified_count > 0:
                            total_urgent_updated += 1
                            logger.debug(f"Updated document {doc['_id']} to urgency: 'yes'")
                        else:
                            logger.warning(f"Failed to update document {doc['_id']}")
                    
                    except Exception as e:
                        logger.error(f"Error updating document {doc.get('_id')}: {str(e)}")
                        continue
                
                logger.info(f"    Marked {urgent_count} documents as urgent for stage '{stage}'")
        
        logger.info(f"Successfully updated {total_urgent_updated} documents to urgency: 'yes'")
        
        # Verify the transformation
        logger.info("Verification - Urgency distribution:")
        pipeline = [
            {"$group": {
                "_id": "$urgency",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        urgency_stats = list(voice_transcripts_collection.aggregate(pipeline))
        for stat in urgency_stats:
            logger.info(f"  {stat['_id']}: {stat['count']} documents")
        
        # Show detailed breakdown by message count and stage
        logger.info("Detailed urgency breakdown by message count and stage:")
        for msg_count in [26, 28, 30, 36, 40]:
            logger.info(f"  {msg_count} messages:")
            for stage in ["Escalation", "Receive", "Authenticate", "Categorize", "Resolution", "Update", "Resolved", "Close", "Report"]:
                urgent_count = voice_transcripts_collection.count_documents({
                    "thread.message_count": msg_count,
                    "stages": stage,
                    "urgency": "yes"
                })
                total_count = voice_transcripts_collection.count_documents({
                    "thread.message_count": msg_count,
                    "stages": stage
                })
                logger.info(f"    {stage}: {urgent_count}/{total_count} urgent")
        
        # Show sample documents
        sample_urgent = voice_transcripts_collection.find_one({"urgency": "yes"})
        sample_non_urgent = voice_transcripts_collection.find_one({"urgency": "no"})
        
        if sample_urgent:
            logger.info("Sample urgent document:")
            logger.info(f"  Document ID: {sample_urgent['_id']}")
            logger.info(f"  Message count: {sample_urgent.get('thread', {}).get('message_count')}")
            logger.info(f"  Stage: {sample_urgent.get('stages')}")
            logger.info(f"  Urgency: {sample_urgent.get('urgency')}")
        
        if sample_non_urgent:
            logger.info("Sample non-urgent document:")
            logger.info(f"  Document ID: {sample_non_urgent['_id']}")
            logger.info(f"  Message count: {sample_non_urgent.get('thread', {}).get('message_count')}")
            logger.info(f"  Stage: {sample_non_urgent.get('stages')}")
            logger.info(f"  Urgency: {sample_non_urgent.get('urgency')}")
        
        logger.info("Urgency field addition completed successfully")
        
    except Exception as e:
        logger.error(f"Error adding urgency field: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    add_urgency_field()
