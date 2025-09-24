import random
import os
from pymongo import MongoClient
import logging
from dotenv import load_dotenv
from collections import Counter

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING', "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME', "sparzaai")

def connect_to_mongodb():
    """Connect to MongoDB database"""
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['chat']
        
        # Test connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        return collection
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}")
        return None
    

def assign_follow_up_required_to_chat_collection():
    """
    Assign follow_up_required field to chat collection: 35% yes, 65% no
    """
    collection = connect_to_mongodb()
    if collection is None:
        return
    
    try:
        # Get total count of documents
        total_docs = collection.count_documents({})
        logger.info(f"Total documents in chat collection: {total_docs}")
        
        if total_docs == 0:
            logger.info("No documents found in chat collection")
            return
        
        # Calculate target counts
        target_yes_count = int(total_docs * 0.35)  # 35% for yes
        target_no_count = total_docs - target_yes_count  # 65% for no
        
        logger.info(f"Target 'yes' documents (35%): {target_yes_count}")
        logger.info(f"Target 'no' documents (65%): {target_no_count}")
        
        # Get current follow_up_required statistics
        current_yes = collection.count_documents({"follow_up_required": "yes"})
        current_no = collection.count_documents({"follow_up_required": "no"})
        current_missing = collection.count_documents({
            "$or": [
                {"follow_up_required": {"$exists": False}},
                {"follow_up_required": None}
            ]
        })
        
        logger.info(f"Current 'yes' documents: {current_yes}")
        logger.info(f"Current 'no' documents: {current_no}")
        logger.info(f"Documents missing follow_up_required field: {current_missing}")
        
        # Strategy: Handle missing follow_up_required field first, then adjust existing ones
        updated_count = 0
        
        # Step 1: Handle documents without follow_up_required field
        if current_missing > 0:
            logger.info(f"\nStep 1: Processing {current_missing} documents without follow_up_required field...")
            
            # Get all documents without follow_up_required field
            docs_without_field = list(collection.find({
                "$or": [
                    {"follow_up_required": {"$exists": False}},
                    {"follow_up_required": None}
                ]
            }, {"_id": 1}))
            
            # Shuffle for random assignment
            random.shuffle(docs_without_field)
            
            # Calculate how many should be yes from the missing ones
            remaining_yes_needed = max(0, target_yes_count - current_yes)
            yes_from_missing = min(remaining_yes_needed, current_missing)
            no_from_missing = current_missing - yes_from_missing
            
            logger.info(f"  Assigning follow_up_required='yes' to {yes_from_missing} documents")
            logger.info(f"  Assigning follow_up_required='no' to {no_from_missing} documents")
            
            # Assign follow_up_required='yes' to first batch
            if yes_from_missing > 0:
                yes_ids = [doc["_id"] for doc in docs_without_field[:yes_from_missing]]
                for doc_id in yes_ids:
                    result = collection.update_one(
                        {"_id": doc_id},
                        {"$set": {"follow_up_required": "yes"}}
                    )
                    if result.modified_count > 0:
                        updated_count += 1
            
            # Assign follow_up_required='no' to remaining batch
            if no_from_missing > 0:
                no_ids = [doc["_id"] for doc in docs_without_field[yes_from_missing:]]
                for doc_id in no_ids:
                    result = collection.update_one(
                        {"_id": doc_id},
                        {"$set": {"follow_up_required": "no"}}
                    )
                    if result.modified_count > 0:
                        updated_count += 1
        
        # Step 2: Adjust existing follow_up_required assignments if needed
        # Check if we need to convert some yes to no or vice versa
        new_yes_count = collection.count_documents({"follow_up_required": "yes"})
        new_no_count = collection.count_documents({"follow_up_required": "no"})
        
        logger.info(f"\nAfter Step 1:")
        logger.info(f"  'Yes' documents: {new_yes_count}")
        logger.info(f"  'No' documents: {new_no_count}")
        
        # If we have too many yes, convert some to no
        if new_yes_count > target_yes_count:
            excess_yes = new_yes_count - target_yes_count
            logger.info(f"\nStep 2a: Converting {excess_yes} 'yes' documents to 'no'...")
            
            # Find random yes documents to convert
            yes_docs = list(collection.find({"follow_up_required": "yes"}, {"_id": 1}))
            random.shuffle(yes_docs)
            
            docs_to_convert = yes_docs[:excess_yes]
            for doc in docs_to_convert:
                result = collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"follow_up_required": "no"}}
                )
                if result.modified_count > 0:
                    updated_count += 1
        
        # If we have too few yes, convert some no to yes
        elif new_yes_count < target_yes_count:
            need_more_yes = target_yes_count - new_yes_count
            logger.info(f"\nStep 2b: Converting {need_more_yes} 'no' documents to 'yes'...")
            
            # Find random no documents to convert
            no_docs = list(collection.find({"follow_up_required": "no"}, {"_id": 1}))
            random.shuffle(no_docs)
            
            docs_to_convert = no_docs[:need_more_yes]
            for doc in docs_to_convert:
                result = collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"follow_up_required": "yes"}}
                )
                if result.modified_count > 0:
                    updated_count += 1
        
        logger.info(f"\nSuccessfully updated {updated_count} documents with follow_up_required field")
        
    except Exception as e:
        logger.error(f"Error updating follow_up_required field: {e}")

def verify_follow_up_distribution():
    """
    Verify follow_up_required distribution and generate statistics
    """
    collection = connect_to_mongodb()
    if collection is None:
        return
    
    try:
        logger.info("\n" + "="*50)
        logger.info("FOLLOW-UP VERIFICATION AND STATISTICS")
        logger.info("="*50)
        
        # Get total count
        total_docs = collection.count_documents({})
        logger.info(f"Total documents in collection: {total_docs}")
        
        # Check documents without follow_up_required field
        docs_without_field = collection.count_documents({
            "$or": [
                {"follow_up_required": {"$exists": False}},
                {"follow_up_required": None}
            ]
        })
        
        yes_count = collection.count_documents({"follow_up_required": "yes"})
        no_count = collection.count_documents({"follow_up_required": "no"})
        
        logger.info(f"Documents with follow_up_required='yes': {yes_count}")
        logger.info(f"Documents with follow_up_required='no': {no_count}")
        logger.info(f"Documents without follow_up_required field: {docs_without_field}")
        
        # Calculate percentages
        yes_percentage = (yes_count / total_docs * 100) if total_docs > 0 else 0
        no_percentage = (no_count / total_docs * 100) if total_docs > 0 else 0
        
        logger.info(f"\nFOLLOW-UP DISTRIBUTION:")
        logger.info("-" * 30)
        logger.info(f"'Yes' (35% target): {yes_count} ({yes_percentage:.1f}%)")
        logger.info(f"'No' (65% target): {no_count} ({no_percentage:.1f}%)")
        
        # Check if distribution is correct
        target_yes_percentage = 35.0
        yes_diff = abs(yes_percentage - target_yes_percentage)
        
        logger.info(f"\nTARGET vs ACTUAL:")
        logger.info(f"Target 'yes' percentage: {target_yes_percentage}%")
        logger.info(f"Actual 'yes' percentage: {yes_percentage:.1f}%")
        logger.info(f"Difference: {yes_diff:.1f}%")
        
        if yes_diff <= 2.0:
            logger.info("✓ Distribution is within acceptable range!")
        else:
            logger.warning(f"⚠ Distribution differs by {yes_diff:.1f}% from target")
        
        # Sample documents with follow_up_required
        logger.info(f"\nSAMPLE DOCUMENTS WITH FOLLOW-UP:")
        logger.info("-" * 40)
        sample_docs = list(collection.find({
            "follow_up_required": {"$exists": True}
        }).limit(5))
        
        for i, doc in enumerate(sample_docs, 1):
            logger.info(f"Document {i}:")
            logger.info(f"  ID: {doc.get('_id')}")
            logger.info(f"  Follow-up Required: {doc.get('follow_up_required')}")
            logger.info("")
        
    except Exception as e:
        logger.error(f"Error during verification: {e}")

def main():
    """Main function to execute the follow-up assignment and verification"""
    logger.info("Starting Chat Collection Follow-up Assignment Process")
    logger.info("="*60)
    
    # Step 1: Assign follow_up_required to documents
    logger.info("Step 1: Assigning follow_up_required to chat documents...")
    assign_follow_up_required_to_chat_collection()
    
    # Step 2: Verify and get statistics
    logger.info("\nStep 2: Verifying follow_up_required and generating statistics...")
    verify_follow_up_distribution()
    
    logger.info("\nProcess completed!")

if __name__ == "__main__":
    main()
