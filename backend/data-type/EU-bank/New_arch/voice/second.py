import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import random
from collections import Counter
from itertools import combinations
from datetime import datetime
import uuid

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get connection details from environment variables
mongo_connection_string = os.getenv('MONGO_CONNECTION_STRING')
mongo_database_name = os.getenv('MONGO_DATABASE_NAME')

def generate_chat_id():
    """Generate a unique chat ID in Teams format"""
    return f"19:{uuid.uuid4().hex[:24]}@thread.v2"

def generate_message_id(index):
    """Generate a unique message ID"""
    return f"msg{index}-{uuid.uuid4().hex[:12]}"

def transform_voice_to_chatmessages_structure():
    """
    Transform voice_transcripts collection to match chatmessages DB structure
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        # Get collections
        voice_collection = db['voice']
        voice_transcripts_collection = db['voice_transcripts']
        
        logger.info("Connected to MongoDB successfully")
        
        # Clear existing voice_transcripts collection
        voice_transcripts_collection.delete_many({})
        logger.info("Cleared existing voice_transcripts collection")
        
        # Get all documents from voice collection
        logger.info("Fetching documents from voice collection...")
        voice_documents = list(voice_collection.find({}))
        
        if not voice_documents:
            logger.warning("No documents found in voice collection")
            return
        
        logger.info(f"Found {len(voice_documents)} documents in voice collection")
        
        # Transform each voice document to chatmessages structure
        transformed_documents = []
        
        for voice_doc in voice_documents:
            try:
                # Generate unique chat_id for this conversation
                chat_id = generate_chat_id()
                
                # Extract participants from conversation (user and agent)
                participants = set()
                conversation = voice_doc.get('conversation', [])
                
                for msg in conversation:
                    speaker = msg.get('speaker', '')
                    if speaker == 'user':
                        participants.add(('customer', voice_doc.get('customer_name', 'Unknown Customer'), voice_doc.get('email', f"customer@example.com")))
                    elif speaker == 'agent':
                        participants.add(('agent', 'Bank Agent', 'agent@bank.com'))
                
                # Create members array
                members = []
                for participant_type, name, email in participants:
                    members.append({
                        "id": email,
                        "displayName": name,
                        "userIdentityType": "aadUser"
                    })
                
                # Create messages array from conversation
                messages = []
                for i, conv_msg in enumerate(conversation, 1):
                    speaker = conv_msg.get('speaker', '')
                    content = conv_msg.get('text', '')
                    
                    # Determine the sender email
                    if speaker == 'user':
                        sender_email = voice_doc.get('email', 'customer@example.com')
                        sender_name = voice_doc.get('customer_name', 'Unknown Customer')
                    else:  # agent
                        sender_email = 'agent@bank.com'
                        sender_name = 'Bank Agent'
                    
                    message = {
                        "id": generate_message_id(i),
                        "chatId": chat_id,
                        "createdDateTime": None,
                        "from": {
                            "user": {
                                "id": sender_email,
                                "displayName": sender_name,
                                "userIdentityType": "aadUser"
                            }
                        },
                        "body": {
                            "contentType": "text",
                            "content": content
                        }
                    }
                    messages.append(message)
                
                # Create the transformed document matching chatmessages structure
                transformed_doc = {
                    "provider": "voice_call",  # Changed from m365 to indicate voice calls
                    "chat": {
                        "chat_id": chat_id,
                        "chatType": "oneOnOne",
                        "topic": voice_doc.get('dominant_topic', 'Voice Call'),
                        "createdDateTime": None,
                        "lastUpdatedDateTime": None,
                        "message_count": len(messages),
                        "members": members
                    },
                    "messages": messages,
                    
                    # Copy the clustering and analysis fields from original voice document
                    "kmeans_cluster_id": voice_doc.get('kmeans_cluster_id'),
                    "subcluster_id": voice_doc.get('subcluster_id'),
                    "subcluster_label": voice_doc.get('subcluster_label'),
                    "dominant_cluster_label": voice_doc.get('dominant_cluster_label'),
                    "kmeans_cluster_keyphrase": voice_doc.get('kmeans_cluster_keyphrase'),
                    "domain": voice_doc.get('domain'),
                    "processed_at": voice_doc.get('processed_at'),
                    "dominant_topic": voice_doc.get('dominant_topic'),
                    "subtopics": voice_doc.get('subtopics'),
                    
                    # Additional voice-specific fields
                    "call_id": voice_doc.get('call_id'),
                    "customer_name": voice_doc.get('customer_name'),
                    "customer_id": voice_doc.get('customer_id'),
                    "email": voice_doc.get('email'),
                    "call_purpose": voice_doc.get('call_purpose'),
                    "priority": voice_doc.get('priority'),
                    "resolution_status": voice_doc.get('resolution_status'),
                    "sentiment": voice_doc.get('sentiment'),
                    "urgency": voice_doc.get('urgency'),
                    "timestamp": voice_doc.get('timestamp')
                }
                
                transformed_documents.append(transformed_doc)
                
            except Exception as e:
                logger.error(f"Error transforming document {voice_doc.get('_id', 'unknown')}: {str(e)}")
                continue
        
        if not transformed_documents:
            logger.warning("No documents were successfully transformed")
            return
        
        # Insert transformed documents into voice_transcripts collection
        logger.info(f"Inserting {len(transformed_documents)} transformed documents into voice_transcripts collection...")
        
        # Insert in batches
        batch_size = 100
        total_inserted = 0
        
        for i in range(0, len(transformed_documents), batch_size):
            batch = transformed_documents[i:i + batch_size]
            result = voice_transcripts_collection.insert_many(batch)
            total_inserted += len(result.inserted_ids)
            logger.info(f"Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} documents")
        
        logger.info(f"Successfully transformed and inserted {total_inserted} documents")
        
        # Verify the transformation
        sample_doc = voice_transcripts_collection.find_one()
        if sample_doc:
            logger.info("Sample transformed document structure:")
            logger.info(f"  Provider: {sample_doc.get('provider')}")
            logger.info(f"  Chat ID: {sample_doc.get('chat', {}).get('chat_id')}")
            logger.info(f"  Topic: {sample_doc.get('chat', {}).get('topic')}")
            logger.info(f"  Message Count: {sample_doc.get('chat', {}).get('message_count')}")
            logger.info(f"  Members: {len(sample_doc.get('chat', {}).get('members', []))}")
            logger.info(f"  Call ID: {sample_doc.get('call_id')}")
            logger.info(f"  Domain: {sample_doc.get('domain')}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise
    
    finally:
        # Close MongoDB connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def verify_transformation():
    """
    Verify that the transformation was successful by comparing structures
    """
    try:
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        voice_collection = db['voice']
        voice_transcripts_collection = db['voice_transcripts']
        chatmessages_collection = db['chatmessages']
        
        # Get counts
        voice_count = voice_collection.count_documents({})
        transcripts_count = voice_transcripts_collection.count_documents({})
        
        logger.info(f"Original voice collection count: {voice_count}")
        logger.info(f"Transformed voice_transcripts count: {transcripts_count}")
        
        # Compare structure with chatmessages
        sample_chat = chatmessages_collection.find_one()
        sample_voice_transcript = voice_transcripts_collection.find_one()
        
        if sample_chat and sample_voice_transcript:
            logger.info("Structure comparison:")
            
            # Check main fields
            chat_fields = set(sample_chat.keys())
            transcript_fields = set(sample_voice_transcript.keys())
            
            logger.info(f"Chatmessages fields: {sorted(chat_fields)}")
            logger.info(f"Voice_transcripts fields: {sorted(transcript_fields)}")
            
            common_fields = chat_fields.intersection(transcript_fields)
            logger.info(f"Common fields: {sorted(common_fields)}")
            
            # Check chat structure
            if 'chat' in sample_voice_transcript:
                chat_structure_fields = set(sample_voice_transcript['chat'].keys())
                original_chat_fields = set(sample_chat['chat'].keys())
                logger.info(f"Chat structure match: {chat_structure_fields == original_chat_fields}")
            
            # Check messages structure
            if 'messages' in sample_voice_transcript and sample_voice_transcript['messages']:
                message_fields = set(sample_voice_transcript['messages'][0].keys())
                original_message_fields = set(sample_chat['messages'][0].keys())
                logger.info(f"Message structure match: {message_fields == original_message_fields}")
        
    except Exception as e:
        logger.error(f"Error during verification: {str(e)}")
    
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    try:
        logger.info("Starting transformation from voice to voice_transcripts with chatmessages structure")
        transform_voice_to_chatmessages_structure()
        
        logger.info("Verifying transformation...")
        verify_transformation()
        
        logger.info("Transformation completed successfully")
        
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")