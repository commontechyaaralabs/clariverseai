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

def add_stages_field():
    """
    Add stages field to voice_transcripts collection based on the distribution table:
    
    Stage \ Msg count	26 msgs (306)	28 msgs (306)	30 msgs (408)	36 msgs (510)	40 msgs (510)
    Receive	10	9	12	16	14
    Authenticate	9	10	12	16	14
    Categorize	11	12	16	22	21
    Resolution	61	61	82	101	103
    Escalation	38	37	49	60	61
    Update	24	24	33	41	41
    Resolved	91	92	122	153	154
    Close	38	37	49	60	61
    Report	24	24	33	41	41
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
        
        # Define stages and their distribution by message count
        stages_distribution = {
            26: {
                "Receive": 10,
                "Authenticate": 9,
                "Categorize": 11,
                "Resolution": 61,
                "Escalation": 38,
                "Update": 24,
                "Resolved": 91,
                "Close": 38,
                "Report": 24
            },
            28: {
                "Receive": 9,
                "Authenticate": 10,
                "Categorize": 12,
                "Resolution": 61,
                "Escalation": 37,
                "Update": 24,
                "Resolved": 92,
                "Close": 37,
                "Report": 24
            },
            30: {
                "Receive": 12,
                "Authenticate": 12,
                "Categorize": 16,
                "Resolution": 82,
                "Escalation": 49,
                "Update": 33,
                "Resolved": 122,
                "Close": 49,
                "Report": 33
            },
            36: {
                "Receive": 16,
                "Authenticate": 16,
                "Categorize": 22,
                "Resolution": 101,
                "Escalation": 60,
                "Update": 41,
                "Resolved": 153,
                "Close": 60,
                "Report": 41
            },
            40: {
                "Receive": 14,
                "Authenticate": 14,
                "Categorize": 21,
                "Resolution": 103,
                "Escalation": 61,
                "Update": 41,
                "Resolved": 154,
                "Close": 61,
                "Report": 41
            }
        }
        
        # Get all documents grouped by message count
        documents_by_msg_count = {}
        all_documents = list(voice_transcripts_collection.find({}))
        
        for doc in all_documents:
            msg_count = doc.get('thread', {}).get('message_count', 0)
            if msg_count not in documents_by_msg_count:
                documents_by_msg_count[msg_count] = []
            documents_by_msg_count[msg_count].append(doc)
        
        logger.info("Documents grouped by message count:")
        for msg_count, docs in documents_by_msg_count.items():
            logger.info(f"  {msg_count} messages: {len(docs)} documents")
        
        # Process each message count group
        total_updated = 0
        
        for msg_count, docs in documents_by_msg_count.items():
            if msg_count not in stages_distribution:
                logger.warning(f"No stage distribution defined for {msg_count} messages, skipping")
                continue
            
            logger.info(f"Processing {len(docs)} documents with {msg_count} messages")
            
            # Get stage distribution for this message count
            stage_counts = stages_distribution[msg_count]
            
            # Shuffle documents to randomize stage assignment
            random.shuffle(docs)
            
            # Create stage assignment list
            stage_assignments = []
            for stage, count in stage_counts.items():
                stage_assignments.extend([stage] * count)
            
            # Shuffle stage assignments
            random.shuffle(stage_assignments)
            
            # Ensure we have enough stage assignments
            while len(stage_assignments) < len(docs):
                # Add more stages proportionally
                for stage, count in stage_counts.items():
                    if len(stage_assignments) < len(docs):
                        stage_assignments.append(stage)
            
            # Truncate if we have too many
            stage_assignments = stage_assignments[:len(docs)]
            
            # Assign stages to documents
            for i, doc in enumerate(docs):
                try:
                    stage = stage_assignments[i] if i < len(stage_assignments) else "Resolved"
                    
                    # Update document with stages field
                    result = voice_transcripts_collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"stages": stage}}
                    )
                    
                    if result.modified_count > 0:
                        total_updated += 1
                        logger.debug(f"Updated document {doc['_id']} with stage: {stage}")
                    else:
                        logger.warning(f"Failed to update document {doc['_id']}")
                
                except Exception as e:
                    logger.error(f"Error updating document {doc.get('_id')}: {str(e)}")
                    continue
            
            # Log stage distribution for this message count
            logger.info(f"Stage distribution for {msg_count} messages:")
            stage_counts_actual = {}
            for stage in stage_assignments:
                stage_counts_actual[stage] = stage_counts_actual.get(stage, 0) + 1
            
            for stage, count in stage_counts_actual.items():
                logger.info(f"  {stage}: {count} documents")
        
        logger.info(f"Successfully updated {total_updated} documents with stages field")
        
        # Verify the transformation
        logger.info("Verification - Stage distribution across all documents:")
        pipeline = [
            {"$group": {
                "_id": "$stages",
                "count": {"$sum": 1}
            }},
            {"$sort": {"count": -1}}
        ]
        
        stage_stats = list(voice_transcripts_collection.aggregate(pipeline))
        for stat in stage_stats:
            logger.info(f"  {stat['_id']}: {stat['count']} documents")
        
        # Show sample document
        sample_doc = voice_transcripts_collection.find_one({"stages": {"$exists": True}})
        if sample_doc:
            logger.info("Sample document with stages:")
            logger.info(f"  Document ID: {sample_doc['_id']}")
            logger.info(f"  Message count: {sample_doc.get('thread', {}).get('message_count')}")
            logger.info(f"  Stage: {sample_doc.get('stages')}")
        
        logger.info("Stages field addition completed successfully")
        
    except Exception as e:
        logger.error(f"Error adding stages field: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    add_stages_field()
