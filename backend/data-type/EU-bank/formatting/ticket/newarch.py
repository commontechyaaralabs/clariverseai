import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
import random
from collections import Counter
from itertools import combinations

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get connection details from environment variables
mongo_connection_string = os.getenv('MONGO_CONNECTION_STRING')
mongo_database_name = os.getenv('MONGO_DATABASE_NAME')

def copy_sender_fields():
    """
    Copy sender_id and sender_name fields from emailmessages collection to tickets collection
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        # Get collections
        emailmessages_collection = db['emailmessages']
        tickets_collection = db['tickets']
        
        logger.info("Connected to MongoDB successfully")
        
        # Get all documents from emailmessages collection with sender fields
        emailmessages = list(emailmessages_collection.find(
            {"sender_id": {"$exists": True}, "sender_name": {"$exists": True}},
            {"sender_id": 1, "sender_name": 1, "_id": 1}
        ))
        
        if not emailmessages:
            logger.warning("No documents found in emailmessages collection with sender_id and sender_name fields")
            return
        
        logger.info(f"Found {len(emailmessages)} documents in emailmessages collection with sender fields")
        
        # Get all documents from tickets collection
        tickets = list(tickets_collection.find({}, {"_id": 1}))
        
        if not tickets:
            logger.warning("No documents found in tickets collection")
            return
        
        logger.info(f"Found {len(tickets)} documents in tickets collection")
        
        # Method 1: Random assignment of sender fields to tickets
        # This assumes you want to randomly assign sender info to tickets
        updated_count = 0
        
        for ticket in tickets:
            # Randomly select a sender from emailmessages
            random_email = random.choice(emailmessages)
            
            # Update the ticket with sender information
            result = tickets_collection.update_one(
                {"_id": ticket["_id"]},
                {
                    "$set": {
                        "sender_id": random_email["sender_id"],
                        "sender_name": random_email["sender_name"]
                    }
                }
            )
            
            if result.modified_count > 0:
                updated_count += 1
        
        logger.info(f"Successfully updated {updated_count} tickets with sender information")
        
        # Verify the update
        tickets_with_sender = tickets_collection.count_documents({
            "sender_id": {"$exists": True},
            "sender_name": {"$exists": True}
        })
        
        logger.info(f"Verification: {tickets_with_sender} tickets now have sender information")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def copy_sender_fields_with_mapping():
    """
    Alternative method: Copy sender fields based on some mapping logic
    This assumes there's a relationship between emailmessages and tickets
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        # Get collections
        emailmessages_collection = db['emailmessages']
        tickets_collection = db['tickets']
        
        logger.info("Connected to MongoDB successfully")
        
        # If there's a common field between collections (e.g., email_id, user_id, etc.)
        # You can modify this query based on your data structure
        
        # Example: If tickets have an 'email_id' field that references emailmessages
        tickets_with_email_ref = list(tickets_collection.find(
            {"email_id": {"$exists": True}},
            {"_id": 1, "email_id": 1}
        ))
        
        if tickets_with_email_ref:
            logger.info(f"Found {len(tickets_with_email_ref)} tickets with email references")
            
            updated_count = 0
            for ticket in tickets_with_email_ref:
                # Find corresponding email message
                email_msg = emailmessages_collection.find_one(
                    {"_id": ticket["email_id"]},
                    {"sender_id": 1, "sender_name": 1}
                )
                
                if email_msg and "sender_id" in email_msg and "sender_name" in email_msg:
                    # Update ticket with sender information
                    result = tickets_collection.update_one(
                        {"_id": ticket["_id"]},
                        {
                            "$set": {
                                "sender_id": email_msg["sender_id"],
                                "sender_name": email_msg["sender_name"]
                            }
                        }
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
            
            logger.info(f"Successfully updated {updated_count} tickets with mapped sender information")
        else:
            logger.info("No tickets found with email references, using random assignment method")
            copy_sender_fields()
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def get_sender_statistics():
    """
    Get statistics about sender fields in both collections
    """
    try:
        # Connect to MongoDB
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        
        emailmessages_collection = db['emailmessages']
        tickets_collection = db['tickets']
        
        # Get statistics from emailmessages
        total_emails = emailmessages_collection.count_documents({})
        emails_with_sender = emailmessages_collection.count_documents({
            "sender_id": {"$exists": True},
            "sender_name": {"$exists": True}
        })
        
        # Get statistics from tickets
        total_tickets = tickets_collection.count_documents({})
        tickets_with_sender = tickets_collection.count_documents({
            "sender_id": {"$exists": True},
            "sender_name": {"$exists": True}
        })
        
        logger.info("=== STATISTICS ===")
        logger.info(f"EmailMessages Collection:")
        logger.info(f"  Total documents: {total_emails}")
        logger.info(f"  Documents with sender fields: {emails_with_sender}")
        
        logger.info(f"Tickets Collection:")
        logger.info(f"  Total documents: {total_tickets}")
        logger.info(f"  Documents with sender fields: {tickets_with_sender}")
        
        # Get unique senders from emailmessages
        unique_senders = emailmessages_collection.distinct("sender_id", {
            "sender_id": {"$exists": True}
        })
        logger.info(f"  Unique senders in emailmessages: {len(unique_senders)}")
        
    except Exception as e:
        logger.error(f"Error getting statistics: {str(e)}")
    finally:
        if 'client' in locals():
            client.close()

if __name__ == "__main__":
    # First, get current statistics
    logger.info("Getting current statistics...")
    get_sender_statistics()
    
    # Ask user for confirmation
    response = input("\nDo you want to proceed with copying sender fields? (y/n): ")
    
    if response.lower() in ['y', 'yes']:
        # Choose method based on your requirements
        print("\nChoose copy method:")
        print("1. Random assignment (assigns random sender info to each ticket)")
        print("2. Mapping-based assignment (requires relationship between collections)")
        
        method = input("Enter method (1 or 2): ")
        
        if method == "1":
            logger.info("Starting random assignment method...")
            copy_sender_fields()
        elif method == "2":
            logger.info("Starting mapping-based assignment method...")
            copy_sender_fields_with_mapping()
        else:
            logger.info("Invalid method selected. Exiting...")
    else:
        logger.info("Operation cancelled by user")
    
    # Get final statistics
    logger.info("\nGetting final statistics...")
    get_sender_statistics()