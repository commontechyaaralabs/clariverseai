# Import required libraries
from pymongo import MongoClient
from collections import defaultdict
import secrets
import random
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def generate_hex_id(length=16):
    """Generate a random hexadecimal string of specified length"""
    return secrets.token_hex(length)

def generate_message_count():
    """Generate message count based on requirements:
    250 emails should have 1 message only
    Other records should have 3, 4, 6, 8 randomly
    """
    # This will be called for each conversation
    # You'll need to track how many single-message conversations you've created
    return random.choice([3, 4, 6, 8])

def create_participants(sender_email, sender_name, receiver_emails, receiver_names):
    """Create participants list from sender and receiver data"""
    participants = []
    
    # Add sender
    participants.append({
        "type": "from",
        "name": sender_name,
        "email": sender_email
    })
    
    # Add receivers
    for i, receiver_email in enumerate(receiver_emails):
        receiver_name = receiver_names[i] if i < len(receiver_names) else "Unknown"
        participants.append({
            "type": "to", 
            "name": receiver_name,
            "email": receiver_email
        })
    
    return participants

def create_messages(conversation_id, participants, message_count, original_subject):
    """Create messages array based on message count and participants"""
    messages = []
    all_participants = participants.copy()
    previous_sender_email = None
    
    for i in range(message_count):
        # Select sender based on rules
        if i == 0:
            # First message from original sender
            sender = next(p for p in participants if p["type"] == "from")
        else:
            # Subsequent messages: exclude the previous sender
            available_senders = [p for p in all_participants if p["email"] != previous_sender_email]
            if not available_senders:  # Fallback if no other senders available
                available_senders = all_participants
            sender = random.choice(available_senders)
        
        # Update previous sender for next iteration
        previous_sender_email = sender["email"]
        
        # Create recipients (everyone except sender)
        recipients = [p for p in all_participants if p["email"] != sender["email"]]
        
        message = {
            "provider_ids": {
                "m365": {
                    "id": generate_hex_id(8),
                    "conversationId": conversation_id,
                    "internetMessageId": f"<{generate_hex_id(12)}@domain.com>"
                }
            },
            "headers": {
                "date": None,  # Placeholder as requested
                "subject": None,  # Set to null as requested
                "from": [{"name": sender["name"], "email": sender["email"]}],
                "to": [{"name": r["name"], "email": r["email"]} for r in recipients]
            },
            "body": {
                "mime_type": "text/plain",
                "text": {"plain": None}  # Placeholder as requested
            }
        }
        messages.append(message)
    
    return messages

def transform_emailmessages_to_email():
    """Main function to transform emailmessages collection to email collection"""
    
    # Connect to MongoDB
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE_NAME]
    
    emailmessages_collection = db['emailmessages']
    email_collection = db['email']
    
    # Clear existing email collection (optional)
    # email_collection.delete_many({})
    
    # Group messages by conversation_id
    conversations = defaultdict(list)
    
    # Fetch all emailmessages and group by conversation_id
    for message in emailmessages_collection.find():
        conv_id = message.get('conversation_id')
        if conv_id:
            conversations[conv_id].append(message)
    
    print(f"Found {len(conversations)} conversations")
    
    # Counter for single message conversations (limit to 250)
    single_message_count = 0
    max_single_messages = 250
    
    processed_count = 0
    
    for conv_id, messages_list in conversations.items():
        # Take the first message as the base for creating the email thread
        base_message = messages_list[0]
        
        # Generate unique thread identifiers
        thread_id = f"conv_m365_{generate_hex_id(6)}"
        m365_conversation_id = generate_hex_id(12)
        
        # Create participants
        participants = create_participants(
            base_message['sender_id'],
            base_message['sender_name'],
            base_message['receiver_ids'],
            base_message['receiver_names']
        )
        
        # Determine message count
        if single_message_count < max_single_messages:
            message_count = 1
            single_message_count += 1
        else:
            message_count = generate_message_count()
        
        # Create messages
        messages = create_messages(
            m365_conversation_id,
            participants,
            message_count,
            base_message.get('subject', 'No Subject')
        )
        
        # Create the email document
        email_document = {
            "provider": "m365",
            "thread": {
                "thread_id": thread_id,
                "thread_key": {"m365_conversation_id": m365_conversation_id},
                "subject_norm": None,  # Placeholder as requested
                "participants": participants,
                "first_message_at": None,  # Placeholder as requested
                "last_message_at": None,   # Placeholder as requested
                "message_count": message_count
            },
            "messages": messages,
            
            # Copy fields from original emailmessages
            "dominant_topic": base_message.get('dominant_topic'),
            "subtopics": base_message.get('subtopics'),
            "kmeans_cluster_id": base_message.get('kmeans_cluster_id'),
            "subcluster_id": base_message.get('subcluster_id'),
            "subcluster_label": base_message.get('subcluster_label'),
            "dominant_cluster_label": base_message.get('dominant_cluster_label'),
            "urgency": base_message.get('urgency'),
            "kmeans_cluster_keyphrase": base_message.get('kmeans_cluster_keyphrase'),
            "domain": base_message.get('domain')
        }
        
        # Insert into email collection
        try:
            result = email_collection.insert_one(email_document)
            processed_count += 1
            
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} conversations...")
                
        except Exception as e:
            print(f"Error inserting conversation {conv_id}: {str(e)}")
    
    print(f"Transformation completed. Processed {processed_count} conversations.")
    print(f"Single message conversations: {single_message_count}")
    
    # Close connection
    client.close()

def verify_transformation():
    """Verify the transformation by checking some sample records"""
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE_NAME]
    email_collection = db['email']
    
    # Get total count
    total_count = email_collection.count_documents({})
    print(f"\nTotal email documents: {total_count}")
    
    # Check message count distribution
    message_counts = {}
    for doc in email_collection.find({}, {"thread.message_count": 1}):
        count = doc["thread"]["message_count"]
        message_counts[count] = message_counts.get(count, 0) + 1
    
    print("\nMessage count distribution:")
    for count, freq in sorted(message_counts.items()):
        print(f"  {count} messages: {freq} conversations")
    
    # Show a sample document
    sample = email_collection.find_one()
    if sample:
        print(f"\nSample document structure:")
        print(f"  Provider: {sample.get('provider')}")
        print(f"  Thread ID: {sample['thread']['thread_id']}")
        print(f"  Participants: {len(sample['thread']['participants'])}")
        print(f"  Messages: {len(sample['messages'])}")
        print(f"  Domain: {sample.get('domain')}")
    
    client.close()

if __name__ == "__main__":
    # Check if environment variables are set
    if not MONGO_CONNECTION_STRING or not MONGO_DATABASE_NAME:
        print("Error: Please set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your .env file")
        exit(1)
    
    print("Starting email collection transformation...")
    transform_emailmessages_to_email()
    
    print("\nVerifying transformation...")
    verify_transformation()
    
    print("\nTransformation completed successfully!")