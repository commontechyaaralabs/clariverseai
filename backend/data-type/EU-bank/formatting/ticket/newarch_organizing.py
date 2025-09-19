import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import random
from collections import Counter
from itertools import combinations
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get connection details from environment variables
mongo_connection_string = os.getenv('MONGO_CONNECTION_STRING')
mongo_database_name = os.getenv('MONGO_DATABASE_NAME')

def update_tickets_collection():
    """
    Update existing tickets collection to match email collection format
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        tickets_collection = db['tickets']
        
        # Fetch all tickets
        tickets = list(tickets_collection.find())
        logger.info(f"Found {len(tickets)} tickets to update")
        
        # Calculate message count distribution
        total_records = len(tickets)
        
        # Distribution logic:
        # 5% -> 1 message
        # 35% -> 4 messages 
        # 30% -> 6 messages
        # 30% -> 7 messages
        
        one_msg_count = int(total_records * 0.05)  # 5%
        four_msg_count = int(total_records * 0.35)  # 35%
        six_msg_count = int(total_records * 0.30)  # 30%
        seven_msg_count = total_records - one_msg_count - four_msg_count - six_msg_count  # Remaining ~30%
        
        logger.info(f"Message distribution: {one_msg_count} records with 1 message, {four_msg_count} records with 4 messages, {six_msg_count} records with 6 messages, {seven_msg_count} records with 7 messages")
        
        # Create message count assignments
        message_counts = []
        message_counts.extend([1] * one_msg_count)
        message_counts.extend([4] * four_msg_count)
        message_counts.extend([6] * six_msg_count)
        message_counts.extend([7] * seven_msg_count)
        
        # Shuffle to randomize distribution
        random.shuffle(message_counts)
        
        def create_message_template(ticket, message_index=0):
            """Create a single message template"""
            return {
                "provider_ids": {
                    "ticket_system": {
                        "id": f"{ticket['ticket_number']}_msg_{message_index}",
                        "ticket_id": ticket["ticket_number"]
                    }
                },
                "headers": {
                    "date": ticket.get("processed_at", datetime.now().isoformat()),
                    "ticket_title": ticket.get("dominant_topic", "") if message_index == 0 else f"{ticket.get('dominant_topic', '')} - Follow up {message_index}",
                    "from": [
                        {
                            "name": ticket.get("sender_name"),
                            "email": ticket.get("sender_id")
                        }
                    ],
                    "to": [
                        {
                            "type": "to",
                            "name": None,
                            "email": None
                        }
                    ]
                },
                "body": {
                    "mime_type": "text/plain",
                    "text": {
                        "plain": None
                    }
                }
            }
        
        updated_count = 0
        
        for i, ticket in enumerate(tickets):
            num_messages = message_counts[i] if i < len(message_counts) else 1
            
            # Create messages array based on assigned count
            messages = []
            for msg_idx in range(num_messages):
                messages.append(create_message_template(ticket, msg_idx))
            
            # Create update document with new structure
            update_doc = {
                "$set": {
                    "provider": "ticket_system",
                    "thread": {
                        "thread_id": f"ticket_{ticket['ticket_number']}",
                        "thread_key": {
                            "ticket_id": ticket["ticket_number"]
                        },
                        "subject_norm": ticket.get("dominant_topic", ""),
                        "participants": [
                            {
                                "type": "from",
                                "name": ticket.get("sender_name"),
                                "email": ticket.get("sender_id")
                            },
                            {
                                "type": "to",
                                "name": None,
                                "email": None
                            }
                        ],
                        "first_message_at": ticket.get("processed_at", datetime.now().isoformat()),
                        "last_message_at": ticket.get("processed_at", datetime.now().isoformat()),
                        "message_count": len(messages)
                    },
                    "messages": messages
                },
                "$unset": {
                    "ticket_number": "",
                    "sender_id": "",
                    "sender_name": ""
                }
            }
            
            # Update the document
            result = tickets_collection.update_one(
                {"_id": ticket["_id"]},
                update_doc
            )
            
            if result.modified_count > 0:
                updated_count += 1
        
        logger.info(f"Successfully updated {updated_count} tickets in the original collection")
        
        # Verify the update and show message distribution
        pipeline = [
            {"$project": {"message_count": {"$size": "$messages"}}},
            {"$group": {"_id": "$message_count", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        
        distribution = list(tickets_collection.aggregate(pipeline))
        logger.info("Actual message count distribution:")
        for item in distribution:
            percentage = (item['count'] / updated_count) * 100
            logger.info(f"  {item['_id']} messages: {item['count']} records ({percentage:.1f}%)")
        
        # Verify the update with a sample document
        sample_doc = tickets_collection.find_one()
        if sample_doc:
            logger.info("Sample updated document structure:")
            logger.info(f"Provider: {sample_doc.get('provider')}")
            logger.info(f"Thread participants count: {len(sample_doc['thread']['participants'])}")
            logger.info(f"Messages count: {len(sample_doc['messages'])}")
            logger.info(f"Ticket title: {sample_doc['messages'][0]['headers']['ticket_title']}")
            logger.info(f"To participant: {sample_doc['messages'][0]['headers']['to'][0]}")
            logger.info(f"Body plain text: {sample_doc['messages'][0]['body']['text']['plain']}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error updating tickets: {str(e)}")
        return False
    finally:
        if 'client' in locals():
            client.close()

def cleanup_restructured_collection():
    """
    Remove the accidentally created tickets_restructured collection
    """
    try:
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        if 'tickets_restructured' in db.list_collection_names():
            db.drop_collection('tickets_restructured')
            logger.info("Dropped tickets_restructured collection")
        else:
            logger.info("tickets_restructured collection doesn't exist")
            
    except Exception as e:
        logger.error(f"Error cleaning up: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

def main():
    """
    Main function to execute the restructuring
    """
    logger.info("Starting tickets collection restructuring...")
    
    # First, clean up the accidentally created collection
    cleanup_restructured_collection()
    
    # Update the original tickets collection
    success = update_tickets_collection()
    
    if success:
        logger.info("Tickets collection restructuring completed successfully!")
        logger.info("The original 'tickets' collection has been updated with the new structure.")
    else:
        logger.error("Tickets collection restructuring failed!")

if __name__ == "__main__":
    main()