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

def restructure_tickets_collection():
    """
    Restructure tickets collection with new message distribution and proper from/to alternating
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        tickets_collection = db['tickets']
        
        # Fetch all tickets
        tickets = list(tickets_collection.find())
        logger.info(f"Found {len(tickets)} tickets to restructure")
        
        # Calculate message count distribution based on new requirements
        total_records = len(tickets)
        
        # New distribution logic:
        # 5% -> 1 message (keep existing ones)
        # 35% -> 4 messages 
        # 25% -> 6 messages (reduced from 30%)
        # 25% -> 7 messages (reduced from 30%)
        # 10% -> 2 messages (new - taken from 6 and 7 message groups)
        
        one_msg_count = int(total_records * 0.05)    # 5%
        four_msg_count = int(total_records * 0.35)   # 35%
        six_msg_count = int(total_records * 0.25)    # 25%
        seven_msg_count = int(total_records * 0.25)  # 25%
        two_msg_count = total_records - one_msg_count - four_msg_count - six_msg_count - seven_msg_count  # Remaining ~10%
        
        logger.info(f"New message distribution:")
        logger.info(f"  1 message: {one_msg_count} records (5%)")
        logger.info(f"  2 messages: {two_msg_count} records (~10%)")
        logger.info(f"  4 messages: {four_msg_count} records (35%)")
        logger.info(f"  6 messages: {six_msg_count} records (25%)")
        logger.info(f"  7 messages: {seven_msg_count} records (25%)")
        
        # Get current records with 1 message to keep them as is
        current_one_msg_tickets = list(tickets_collection.find({"thread.message_count": 1}).limit(one_msg_count))
        current_one_msg_ids = [ticket["_id"] for ticket in current_one_msg_tickets]
        
        logger.info(f"Keeping {len(current_one_msg_tickets)} existing 1-message tickets")
        
        # Get remaining tickets that need redistribution
        remaining_tickets = [ticket for ticket in tickets if ticket["_id"] not in current_one_msg_ids]
        
        # Create message count assignments for remaining tickets
        message_counts = []
        message_counts.extend([2] * two_msg_count)
        message_counts.extend([4] * four_msg_count)
        message_counts.extend([6] * six_msg_count)
        message_counts.extend([7] * seven_msg_count)
        
        # Pad with 4 messages if we don't have enough assignments
        while len(message_counts) < len(remaining_tickets):
            message_counts.append(4)
        
        # Shuffle to randomize distribution
        random.shuffle(message_counts)
        
        def create_message_template(ticket, message_index=0, total_messages=1):
            """Create a single message template with proper from/to alternating"""
            
            # Extract original sender info
            original_sender = None
            if 'messages' in ticket and len(ticket['messages']) > 0:
                from_info = ticket['messages'][0]['headers']['from'][0]
                original_sender = {
                    "name": from_info.get('name'),
                    "email": from_info.get('email')
                }
            else:
                # Fallback to old structure
                original_sender = {
                    "name": ticket.get("sender_name"),
                    "email": ticket.get("sender_id")
                }
            
            # For odd message indices (0, 2, 4, 6...), sender is in 'from'
            # For even message indices (1, 3, 5...), sender is in 'to'
            if message_index % 2 == 0:  # 0, 2, 4, 6...
                from_participant = {
                    "name": original_sender["name"],
                    "email": original_sender["email"]
                }
                to_participant = {
                    "type": "to",
                    "name": None,
                    "email": None
                }
            else:  # 1, 3, 5...
                from_participant = {
                    "name": None,
                    "email": None
                }
                to_participant = {
                    "type": "to",
                    "name": original_sender["name"],
                    "email": original_sender["email"]
                }
            
            # Get subject from thread or fallback to dominant_topic
            subject = ""
            if 'thread' in ticket and 'subject_norm' in ticket['thread']:
                subject = ticket['thread']['subject_norm']
            else:
                subject = ticket.get("dominant_topic", "")
            
            return {
                "provider_ids": {
                    "ticket_system": {
                        "id": f"{ticket.get('_id')}_msg_{message_index}",
                        "ticket_id": str(ticket.get('_id'))
                    }
                },
                "headers": {
                    "date": ticket.get("processed_at", datetime.now().isoformat()),
                    "ticket_title": subject if message_index == 0 else f"{subject} - Follow up {message_index}",
                    "from": [from_participant],
                    "to": [to_participant]
                },
                "body": {
                    "mime_type": "text/plain",
                    "text": {
                        "plain": None
                    }
                }
            }
        
        updated_count = 0
        
        # Process remaining tickets (excluding the 1-message ones we're keeping)
        for i, ticket in enumerate(remaining_tickets):
            if i >= len(message_counts):
                break
                
            num_messages = message_counts[i]
            
            # Create messages array based on assigned count
            messages = []
            for msg_idx in range(num_messages):
                messages.append(create_message_template(ticket, msg_idx, num_messages))
            
            # Extract original sender info for thread participants
            original_sender = None
            if 'messages' in ticket and len(ticket['messages']) > 0:
                from_info = ticket['messages'][0]['headers']['from'][0]
                original_sender = {
                    "type": "from",
                    "name": from_info.get('name'),
                    "email": from_info.get('email')
                }
            else:
                # Fallback to old structure
                original_sender = {
                    "type": "from",
                    "name": ticket.get("sender_name"),
                    "email": ticket.get("sender_id")
                }
            
            # Get subject
            subject = ticket.get("dominant_topic", "")
            if 'thread' in ticket and 'subject_norm' in ticket['thread']:
                subject = ticket['thread']['subject_norm']
            
            # Create the new document structure with desired field order
            new_doc = {
                "messages": messages,
                "provider": "ticket_system",
                "thread": {
                    "thread_id": f"ticket_{ticket.get('_id')}",
                    "thread_key": {
                        "ticket_id": str(ticket.get('_id'))
                    },
                    "subject_norm": subject,
                    "participants": [
                        original_sender,
                        {
                            "type": "to",
                            "name": None,
                            "email": None
                        }
                    ],
                    "first_message_at": ticket.get("processed_at", datetime.now().isoformat()),
                    "last_message_at": ticket.get("processed_at", datetime.now().isoformat()),
                    "message_count": len(messages)
                }
            }
            
            # Add remaining fields in order
            remaining_fields = [
                "dominant_topic", "subtopics", "kmeans_cluster_id", "subcluster_id", 
                "subcluster_label", "dominant_cluster_label", "kmeans_cluster_keyphrase", 
                "domain", "processed_at"
            ]
            
            for field in remaining_fields:
                if field in ticket:
                    new_doc[field] = ticket[field]
            
            # Replace the entire document
            result = tickets_collection.replace_one(
                {"_id": ticket["_id"]},
                new_doc
            )
            
            if result.modified_count > 0:
                updated_count += 1
        
        logger.info(f"Successfully updated {updated_count} tickets")
        logger.info(f"Kept {len(current_one_msg_tickets)} existing 1-message tickets unchanged")
        
        # Verify the update and show message distribution
        pipeline = [
            {"$project": {"message_count": {"$size": "$messages"}}},
            {"$group": {"_id": "$message_count", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        
        distribution = list(tickets_collection.aggregate(pipeline))
        total_updated = sum(item['count'] for item in distribution)
        
        logger.info("Final message count distribution:")
        for item in distribution:
            percentage = (item['count'] / total_updated) * 100
            logger.info(f"  {item['_id']} messages: {item['count']} records ({percentage:.1f}%)")
        
        # Verify the document structure with a sample
        sample_doc = tickets_collection.find_one({"thread.message_count": {"$gt": 1}})
        if sample_doc:
            logger.info("\nSample document structure verification:")
            logger.info(f"Field order: {list(sample_doc.keys())}")
            logger.info(f"Messages count: {len(sample_doc['messages'])}")
            
            # Check from/to alternating pattern
            if len(sample_doc['messages']) > 1:
                logger.info("\nFrom/To alternating pattern check:")
                for i, msg in enumerate(sample_doc['messages'][:4]):  # Show first 4 messages
                    from_name = msg['headers']['from'][0].get('name', 'None')
                    to_name = msg['headers']['to'][0].get('name', 'None')
                    logger.info(f"  Message {i}: From='{from_name}', To='{to_name}'")
        
        return True
        
    except Exception as e:
        logger.error(f"Error restructuring tickets: {str(e)}")
        return False
    finally:
        if 'client' in locals():
            client.close()

def main():
    """
    Main function to execute the restructuring
    """
    logger.info("Starting tickets collection restructuring...")
    
    success = restructure_tickets_collection()
    
    if success:
        logger.info("Tickets collection restructuring completed successfully!")
        logger.info("Document structure updated with proper field ordering and from/to alternating pattern.")
    else:
        logger.error("Tickets collection restructuring failed!")

if __name__ == "__main__":
    main()