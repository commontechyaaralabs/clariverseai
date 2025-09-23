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

def set_messages_content_to_null():
    """
    Set all messages.body.content fields to null in voice_transcripts collection
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
        
        # Get all documents that have messages
        documents_with_messages = list(voice_transcripts_collection.find({
            "messages": {"$exists": True, "$ne": []}
        }))
        
        logger.info(f"Found {len(documents_with_messages)} documents with messages")
        
        updated_count = 0
        
        for doc in documents_with_messages:
            try:
                doc_id = doc['_id']
                messages = doc.get('messages', [])
                
                if not messages:
                    logger.warning(f"Document {doc_id} has no messages, skipping")
                    continue
                
                # Count messages with non-null content
                messages_with_content = 0
                for message in messages:
                    if message.get('body', {}).get('content') is not None:
                        messages_with_content += 1
                
                if messages_with_content == 0:
                    logger.info(f"Document {doc_id} already has all message content as null")
                    continue
                
                # Update all messages to set content to null
                update_operations = {}
                for i, message in enumerate(messages):
                    if 'body' in message and 'content' in message['body']:
                        update_operations[f"messages.{i}.body.content"] = None
                
                # Apply the update
                if update_operations:
                    # Use $set to set content to null for all messages
                    result = voice_transcripts_collection.update_one(
                        {"_id": doc_id},
                        {"$set": update_operations}
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                        logger.info(f"Updated document {doc_id} - set {messages_with_content} message contents to null")
                    else:
                        logger.warning(f"Failed to update document {doc_id}")
                
            except Exception as e:
                logger.error(f"Error processing document {doc.get('_id')}: {str(e)}")
                continue
        
        logger.info(f"Successfully updated {updated_count} documents")
        
        # Verify the update by checking a sample document
        sample_doc = voice_transcripts_collection.find_one({"messages": {"$exists": True, "$ne": []}})
        if sample_doc:
            logger.info("Verification - Sample document after update:")
            messages = sample_doc.get('messages', [])
            logger.info(f"  Total messages: {len(messages)}")
            
            # Check if all content fields are null
            all_content_null = True
            for i, message in enumerate(messages):
                content = message.get('body', {}).get('content')
                if content is not None:
                    all_content_null = False
                    logger.warning(f"  Message {i} still has content: {content}")
            
            if all_content_null:
                logger.info("  ✓ All message contents are successfully set to null")
            else:
                logger.warning("  ✗ Some message contents are not null")
        
        logger.info("Message content nullification completed successfully")
        
    except Exception as e:
        logger.error(f"Error setting message content to null: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    set_messages_content_to_null()
