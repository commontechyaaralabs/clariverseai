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

def generate_message_id(index):
    """Generate a unique message ID"""
    import hashlib
    import time
    return f"msg{index}-{hashlib.md5(str(time.time() + index).encode()).hexdigest()[:12]}"

def create_agent_message(message_id, chat_id, content=None):
    """Create an agent message"""
    return {
        "id": message_id,
        "chatId": chat_id,
        "createdDateTime": None,
        "from": {
            "user": {
                "id": "agent@bank.com",
                "displayName": "Bank Agent",
                "userIdentityType": "aadUser"
            }
        },
        "body": {
            "contentType": "text",
            "content": content
        }
    }

def create_customer_message(message_id, chat_id, customer_email, customer_name, content=None):
    """Create a customer message"""
    return {
        "id": message_id,
        "chatId": chat_id,
        "createdDateTime": None,
        "from": {
            "user": {
                "id": customer_email,
                "displayName": customer_name,
                "userIdentityType": "aadUser"
            }
        },
        "body": {
            "contentType": "text",
            "content": content
        }
    }

def generate_additional_messages(original_messages, target_count, customer_email, customer_name, chat_id):
    """Generate additional messages to reach target count"""
    new_messages = []
    
    # Get the last message to determine who should speak next
    last_speaker = "agent" if original_messages[-1]["from"]["user"]["id"] == "agent@bank.com" else "customer"
    
    # Generate additional messages
    for i in range(len(original_messages), target_count):
        message_id = generate_message_id(i + 1)
        
        # Alternate speakers, but ensure last message is from agent
        if i == target_count - 1:
            # Last message must be from agent
            speaker = "agent"
        else:
            # Alternate between customer and agent
            speaker = "customer" if last_speaker == "agent" else "agent"
        
        if speaker == "agent":
            message = create_agent_message(message_id, chat_id, None)
        else:
            message = create_customer_message(message_id, chat_id, customer_email, customer_name, None)
        
        new_messages.append(message)
        last_speaker = speaker
    
    return new_messages

def redistribute_message_counts():
    """
    Redistribute voice_transcripts collection with new message counts and percentages:
    - 26 messages: 15%
    - 28 messages: 15% 
    - 30 messages: 20%
    - 36 messages: 25%
    - 40 messages: 25%
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
        
        # New message counts and their percentages
        message_distribution = {
            26: 0.15,  # 15%
            28: 0.15,  # 15%
            30: 0.20,  # 20%
            36: 0.25,  # 25%
            40: 0.25   # 25%
        }
        
        # Calculate how many documents should have each message count
        distribution_counts = {}
        for msg_count, percentage in message_distribution.items():
            count = int(total_docs * percentage)
            distribution_counts[msg_count] = count
            logger.info(f"Target: {count} documents with {msg_count} messages ({percentage*100}%)")
        
        # Adjust for rounding differences
        total_distributed = sum(distribution_counts.values())
        if total_distributed < total_docs:
            # Add remaining documents to the largest group (40 messages)
            distribution_counts[40] += (total_docs - total_distributed)
            logger.info(f"Adjusted 40-message count to {distribution_counts[40]} to cover all documents")
        
        # Get all documents
        documents = list(voice_transcripts_collection.find({}))
        
        # Shuffle documents to randomize distribution
        random.shuffle(documents)
        
        # Track current distribution
        current_distribution = {26: 0, 28: 0, 30: 0, 36: 0, 40: 0}
        
        # Process each document
        for i, doc in enumerate(documents):
            try:
                # Determine target message count for this document
                target_count = None
                for msg_count in [26, 28, 30, 36, 40]:
                    if current_distribution[msg_count] < distribution_counts[msg_count]:
                        target_count = msg_count
                        break
                
                if target_count is None:
                    # Fallback to 40 if all quotas are filled
                    target_count = 40
                
                current_distribution[target_count] += 1
                
                logger.info(f"Processing document {i+1}/{len(documents)} - Target: {target_count} messages")
                
                # Extract customer info from thread members
                customer_email = None
                customer_name = None
                if 'thread' in doc and 'members' in doc['thread']:
                    for member in doc['thread']['members']:
                        if member['id'] != 'agent@bank.com':
                            customer_email = member['id']
                            customer_name = member['displayName']
                            break
                
                if not customer_email or not customer_name:
                    logger.warning(f"Could not find customer info in document {doc.get('_id')}")
                    continue
                
                # Get chat_id
                chat_id = doc.get('thread', {}).get('chat_id', f"chat_{doc.get('_id')}")
                
                # Process messages
                original_messages = doc.get('messages', [])
                
                # Set all existing message content to null
                for message in original_messages:
                    if 'body' in message and 'content' in message['body']:
                        message['body']['content'] = None
                
                # Generate additional messages if needed
                if len(original_messages) < target_count:
                    additional_messages = generate_additional_messages(
                        original_messages, 
                        target_count, 
                        customer_email, 
                        customer_name, 
                        chat_id
                    )
                    original_messages.extend(additional_messages)
                elif len(original_messages) > target_count:
                    # Truncate to target count, ensuring last message is from agent
                    original_messages = original_messages[:target_count]
                    if original_messages[-1]["from"]["user"]["id"] != "agent@bank.com":
                        # Replace last message with agent message
                        last_message_id = generate_message_id(target_count)
                        original_messages[-1] = create_agent_message(last_message_id, chat_id, None)
                
                # Ensure last message is from agent
                if original_messages and original_messages[-1]["from"]["user"]["id"] != "agent@bank.com":
                    last_message_id = generate_message_id(len(original_messages))
                    original_messages[-1] = create_agent_message(last_message_id, chat_id, None)
                
                # Create new document structure
                new_doc = {
                    "_id": doc["_id"],
                    "provider": doc.get("provider", "voice_call"),
                    "thread": {
                        "chat_id": chat_id,
                        "chatType": doc.get("thread", {}).get("chatType", "oneOnOne"),
                        "createdDateTime": doc.get("thread", {}).get("createdDateTime"),
                        "lastUpdatedDateTime": doc.get("thread", {}).get("lastUpdatedDateTime"),
                        "message_count": len(original_messages),
                        "members": doc.get("thread", {}).get("members", [])
                    },
                    "messages": original_messages
                }
                
                # Add other fields from original document
                for key, value in doc.items():
                    if key not in ["_id", "provider", "thread", "messages"]:
                        new_doc[key] = value
                
                # Update the document
                voice_transcripts_collection.replace_one(
                    {"_id": doc["_id"]}, 
                    new_doc
                )
                
                logger.info(f"Successfully updated document {doc['_id']} with {len(original_messages)} messages")
                
            except Exception as e:
                logger.error(f"Error processing document {doc.get('_id')}: {str(e)}")
                continue
        
        # Log final distribution
        logger.info("Final distribution:")
        for msg_count in [26, 28, 30, 36, 40]:
            actual_count = current_distribution[msg_count]
            target_count = distribution_counts[msg_count]
            percentage = (actual_count / total_docs) * 100
            logger.info(f"  {msg_count} messages: {actual_count} documents ({percentage:.1f}%)")
        
        logger.info("Message count redistribution completed successfully")
        
        # Verify the transformation
        sample_doc = voice_transcripts_collection.find_one({})
        if sample_doc:
            logger.info("Sample transformed document:")
            logger.info(f"  Provider: {sample_doc.get('provider')}")
            logger.info(f"  Thread message count: {sample_doc.get('thread', {}).get('message_count')}")
            logger.info(f"  Actual messages count: {len(sample_doc.get('messages', []))}")
        
    except Exception as e:
        logger.error(f"Error redistributing message counts: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    redistribute_message_counts()
