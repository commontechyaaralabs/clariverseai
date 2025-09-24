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

def update_voice_transcripts():
    """
    Update voice_transcripts collection:
    1. Modify message_count based on mapping
    2. Trim messages array to match new message_count
    3. Ensure last message is from agent
    4. If last two messages are from agent, reduce count by 1
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        collection = db['voice_transcripts']
        
        # Message count mapping
        message_count_mapping = {
            25: 18,
            28: 20,
            30: 22,
            35: 24,
            40: 25
        }
        
        # Get all documents
        documents = list(collection.find({}))
        logger.info(f"Found {len(documents)} documents to process")
        
        for doc in documents:
            doc_id = doc['_id']
            current_message_count = doc['thread']['message_count']
            
            # Check if we need to update this document
            if current_message_count in message_count_mapping:
                new_message_count = message_count_mapping[current_message_count]
                messages = doc['messages']
                
                # Check if last two messages are from agent
                if len(messages) >= 2:
                    last_message = messages[-1]
                    second_last_message = messages[-2]
                    
                    last_is_agent = last_message['from']['user']['displayName'] == 'Bank Agent'
                    second_last_is_agent = second_last_message['from']['user']['displayName'] == 'Bank Agent'
                    
                    # If last two messages are from agent, reduce count by 1
                    if last_is_agent and second_last_is_agent:
                        new_message_count -= 1
                        logger.info(f"Document {doc_id}: Last two messages are from agent, reducing count from {message_count_mapping[current_message_count]} to {new_message_count}")
                
                # Trim messages array to new count
                trimmed_messages = messages[:new_message_count]
                
                # Ensure last message is from agent
                if trimmed_messages and trimmed_messages[-1]['from']['user']['displayName'] != 'Bank Agent':
                    # Find the last agent message
                    last_agent_index = -1
                    for i in range(len(trimmed_messages) - 1, -1, -1):
                        if trimmed_messages[i]['from']['user']['displayName'] == 'Bank Agent':
                            last_agent_index = i
                            break
                    
                    if last_agent_index != -1:
                        # Trim to include the last agent message
                        trimmed_messages = trimmed_messages[:last_agent_index + 1]
                        new_message_count = len(trimmed_messages)
                        logger.info(f"Document {doc_id}: Adjusted to end with agent message, new count: {new_message_count}")
                
                # Update the document
                update_result = collection.update_one(
                    {'_id': doc_id},
                    {
                        '$set': {
                            'thread.message_count': new_message_count,
                            'messages': trimmed_messages
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    logger.info(f"Successfully updated document {doc_id}: {current_message_count} -> {new_message_count} messages")
                else:
                    logger.warning(f"Failed to update document {doc_id}")
            else:
                logger.info(f"Document {doc_id}: message_count {current_message_count} not in mapping, skipping")
        
        logger.info("Voice transcripts update completed")
        
    except Exception as e:
        logger.error(f"Error updating voice transcripts: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    update_voice_transcripts()