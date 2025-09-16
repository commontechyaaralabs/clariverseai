# Import required libraries
from pymongo import MongoClient
from collections import defaultdict
import secrets
import random
from datetime import datetime, timezone
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
    """Generate random message count between 15, 17, 18"""
    return random.choice([15, 17, 18])

def create_chat_members_from_chunks(chat_members_chunks):
    """Transform chat_members from chat-chunks to chatmessages format"""
    members = []
    for member in chat_members_chunks:
        transformed_member = {
            "id": member.get("email", member.get("id", generate_hex_id(8))),
            "displayName": member.get("display_name", "Unknown User"),
            "userIdentityType": "aadUser"
        }
        members.append(transformed_member)
    return members

def create_messages_from_chunks(chat_id, chat_members, message_count, raw_segments=None):
    """Create messages array based on message count and members"""
    messages = []
    
    # If we have raw_segments, use them as reference for realistic conversation
    if raw_segments and len(raw_segments) > 0:
        # Use existing segments as templates but create the required number of messages
        segment_templates = raw_segments
    else:
        segment_templates = []
    
    previous_sender_id = None
    
    for i in range(message_count):
        # Select sender (avoid same sender consecutively when possible)
        available_senders = [m for m in chat_members if m["id"] != previous_sender_id]
        if not available_senders:
            available_senders = chat_members
        
        sender = random.choice(available_senders)
        previous_sender_id = sender["id"]
        
        # Generate message ID
        message_id = f"msg{i+1}-{generate_hex_id(6)}"
        
        # Create timestamp (placeholder as requested)
        created_datetime = None
        
        # Set content to null as requested
        content = None
        
        message = {
            "id": message_id,
            "chatId": chat_id,
            "createdDateTime": created_datetime,
            "from": {
                "user": {
                    "id": sender["id"],
                    "displayName": sender["displayName"],
                    "userIdentityType": sender["userIdentityType"]
                }
            },
            "body": {
                "contentType": "text",
                "content": content
            }
        }
        messages.append(message)
    
    return messages

def transform_chatchunks_to_chatmessages():
    """Main function to transform chat-chunks collection to chatmessages collection"""
    
    # Connect to MongoDB
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE_NAME]
    
    chatchunks_collection = db['chat-chunks']
    chatmessages_collection = db['chatmessages']
    
    # Optional: Clear existing chatmessages collection
    # chatmessages_collection.delete_many({})
    
    processed_count = 0
    
    # Process each chat chunk
    for chunk in chatchunks_collection.find():
        try:
            # Extract data from chat-chunks
            original_chat_id = chunk.get('chat_id')
            chat_members_data = chunk.get('chat_members', [])
            raw_segments = chunk.get('raw_segments', [])
            
            # Generate new chat structure
            teams_chat_id = f"19:{generate_hex_id(12)}@thread.v2"
            
            # Transform chat members
            chat_members = create_chat_members_from_chunks(chat_members_data)
            
            # Generate message count (15, 17, or 18)
            message_count = generate_message_count()
            
            # Create messages
            messages = create_messages_from_chunks(
                teams_chat_id,
                chat_members,
                message_count,
                raw_segments
            )
            
            # Create the chatmessages document
            chatmessages_document = {
                "provider": "m365",
                "chat": {
                    "chat_id": teams_chat_id,
                    "chatType": "oneOnOne" if len(chat_members) == 2 else "group",
                    "topic": chunk.get('dominant_topic'),
                    "createdDateTime": None,  # Placeholder as requested
                    "lastUpdatedDateTime": None,  # Placeholder as requested
                    "message_count": message_count,
                    "members": chat_members
                },
                "messages": messages,
                
                # Copy fields directly from chat-chunks as requested
                "kmeans_cluster_id": chunk.get('kmeans_cluster_id'),
                "subcluster_id": chunk.get('subcluster_id'),
                "subcluster_label": chunk.get('subcluster_label'),
                "dominant_cluster_label": chunk.get('dominant_cluster_label'),
                "kmeans_cluster_keyphrase": chunk.get('kmeans_cluster_keyphrase'),
                "domain": chunk.get('domain'),
                "processed_at": chunk.get('processed_at'),
                
                # Additional fields that might be useful
                "original_chat_id": original_chat_id,
                "dominant_topic": chunk.get('dominant_topic'),
                "subtopics": chunk.get('subtopics')
            }
            
            # Insert into chatmessages collection
            result = chatmessages_collection.insert_one(chatmessages_document)
            processed_count += 1
            
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} chat chunks...")
                
        except Exception as e:
            print(f"Error processing chat chunk {chunk.get('_id', 'unknown')}: {str(e)}")
            continue
    
    print(f"Transformation completed. Processed {processed_count} chat chunks.")
    
    # Close connection
    client.close()

def verify_transformation():
    """Verify the transformation by checking some sample records"""
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE_NAME]
    chatmessages_collection = db['chatmessages']
    
    # Get total count
    total_count = chatmessages_collection.count_documents({})
    print(f"\nTotal chatmessages documents: {total_count}")
    
    # Check message count distribution
    message_counts = {}
    for doc in chatmessages_collection.find({}, {"chat.message_count": 1}):
        count = doc["chat"]["message_count"]
        message_counts[count] = message_counts.get(count, 0) + 1
    
    print("\nMessage count distribution:")
    for count, freq in sorted(message_counts.items()):
        print(f"  {count} messages: {freq} chats")
    
    # Check chat type distribution
    chat_types = {}
    for doc in chatmessages_collection.find({}, {"chat.chatType": 1}):
        chat_type = doc["chat"]["chatType"]
        chat_types[chat_type] = chat_types.get(chat_type, 0) + 1
    
    print("\nChat type distribution:")
    for chat_type, freq in chat_types.items():
        print(f"  {chat_type}: {freq} chats")
    
    # Check domain distribution
    domains = {}
    for doc in chatmessages_collection.find({}, {"domain": 1}):
        domain = doc.get("domain", "unknown")
        domains[domain] = domains.get(domain, 0) + 1
    
    print("\nDomain distribution:")
    for domain, freq in sorted(domains.items()):
        print(f"  {domain}: {freq} chats")
    
    # Show a sample document structure
    sample = chatmessages_collection.find_one()
    if sample:
        print(f"\nSample document structure:")
        print(f"  Provider: {sample.get('provider')}")
        print(f"  Chat ID: {sample['chat']['chat_id']}")
        print(f"  Chat Type: {sample['chat']['chatType']}")
        print(f"  Members: {len(sample['chat']['members'])}")
        print(f"  Messages: {len(sample['messages'])}")
        print(f"  Domain: {sample.get('domain')}")
        print(f"  Cluster ID: {sample.get('kmeans_cluster_id')}")
        print(f"  Topic: {sample.get('dominant_topic')}")
        
        # Show member structure
        if sample['chat']['members']:
            print(f"  Sample member: {sample['chat']['members'][0]}")
        
        # Show message structure
        if sample['messages']:
            print(f"  Sample message keys: {list(sample['messages'][0].keys())}")
    
    client.close()

def show_sample_data():
    """Show sample data from both collections for comparison"""
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE_NAME]
    
    chatchunks_collection = db['chat-chunks']
    chatmessages_collection = db['chatmessages']
    
    print("\n" + "="*60)
    print("SAMPLE DATA COMPARISON")
    print("="*60)
    
    # Sample from chat-chunks
    chunk_sample = chatchunks_collection.find_one()
    if chunk_sample:
        print("\nSample from chat-chunks collection:")
        print(f"  Chat ID: {chunk_sample.get('chat_id')}")
        print(f"  Members count: {len(chunk_sample.get('chat_members', []))}")
        print(f"  Raw segments: {len(chunk_sample.get('raw_segments', []))}")
        print(f"  Domain: {chunk_sample.get('domain')}")
        print(f"  Topic: {chunk_sample.get('dominant_topic')}")
        
        if chunk_sample.get('chat_members'):
            print(f"  Sample member: {chunk_sample['chat_members'][0]}")
    
    # Sample from chatmessages
    message_sample = chatmessages_collection.find_one()
    if message_sample:
        print("\nSample from chatmessages collection:")
        print(f"  Chat ID: {message_sample['chat']['chat_id']}")
        print(f"  Members count: {len(message_sample['chat']['members'])}")
        print(f"  Messages count: {len(message_sample['messages'])}")
        print(f"  Domain: {message_sample.get('domain')}")
        print(f"  Topic: {message_sample.get('dominant_topic')}")
        
        if message_sample['chat']['members']:
            print(f"  Sample member: {message_sample['chat']['members'][0]}")
    
    client.close()

if __name__ == "__main__":
    # Check if environment variables are set
    if not MONGO_CONNECTION_STRING or not MONGO_DATABASE_NAME:
        print("Error: Please set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your .env file")
        exit(1)
    
    print("Starting chat-chunks to chatmessages transformation...")
    print("This script will:")
    print("1. Read from 'chat-chunks' collection")
    print("2. Transform chat_members data")
    print("3. Generate 15, 17, or 18 messages per chat (with NULL placeholders)")
    print("4. Copy required fields from chat-chunks")
    print("5. Insert into 'chatmessages' collection")
    print()
    
    # Ask for confirmation
    response = input("Do you want to proceed? (y/n): ")
    if response.lower() not in ['y', 'yes']:
        print("Operation cancelled.")
        exit(0)
    
    transform_chatchunks_to_chatmessages()
    
    print("\nVerifying transformation...")
    verify_transformation()
    
    print("\nShowing sample data comparison...")
    show_sample_data()
    
    print("\nTransformation completed successfully!")