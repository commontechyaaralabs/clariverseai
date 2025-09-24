import os
import random
from pymongo import MongoClient
import logging
from dotenv import load_dotenv

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

def assign_urgency_to_chat_collection():
    """
    Assign urgency field to chat collection: 8% true, 92% false
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
        target_urgent_count = int(total_docs * 0.08)  # 8% for urgent
        target_non_urgent_count = total_docs - target_urgent_count  # 92% for non-urgent
        
        logger.info(f"Target urgent documents (8%): {target_urgent_count}")
        logger.info(f"Target non-urgent documents (92%): {target_non_urgent_count}")
        
        # Get current urgency statistics
        current_urgent = collection.count_documents({"urgency": True})
        current_non_urgent = collection.count_documents({"urgency": False})
        current_missing = collection.count_documents({
            "$or": [
                {"urgency": {"$exists": False}},
                {"urgency": None}
            ]
        })
        
        logger.info(f"Current urgent documents: {current_urgent}")
        logger.info(f"Current non-urgent documents: {current_non_urgent}")
        logger.info(f"Documents missing urgency field: {current_missing}")
        
        # Strategy: Handle missing urgency field first, then adjust existing ones
        updated_count = 0
        
        # Step 1: Handle documents without urgency field
        if current_missing > 0:
            logger.info(f"\nStep 1: Processing {current_missing} documents without urgency field...")
            
            # Get all documents without urgency field
            docs_without_urgency = list(collection.find({
                "$or": [
                    {"urgency": {"$exists": False}},
                    {"urgency": None}
                ]
            }, {"_id": 1}))
            
            # Shuffle for random assignment
            random.shuffle(docs_without_urgency)
            
            # Calculate how many should be urgent from the missing ones
            remaining_urgent_needed = max(0, target_urgent_count - current_urgent)
            urgent_from_missing = min(remaining_urgent_needed, current_missing)
            non_urgent_from_missing = current_missing - urgent_from_missing
            
            logger.info(f"  Assigning urgency=true to {urgent_from_missing} documents")
            logger.info(f"  Assigning urgency=false to {non_urgent_from_missing} documents")
            
            # Assign urgency=true to first batch
            if urgent_from_missing > 0:
                urgent_ids = [doc["_id"] for doc in docs_without_urgency[:urgent_from_missing]]
                result = collection.update_many(
                    {"_id": {"$in": urgent_ids}},
                    {"$set": {"urgency": True}}
                )
                updated_count += result.modified_count
                logger.info(f"  Updated {result.modified_count} documents with urgency=true")
            
            # Assign urgency=false to remaining batch
            if non_urgent_from_missing > 0:
                non_urgent_ids = [doc["_id"] for doc in docs_without_urgency[urgent_from_missing:]]
                result = collection.update_many(
                    {"_id": {"$in": non_urgent_ids}},
                    {"$set": {"urgency": False}}
                )
                updated_count += result.modified_count
                logger.info(f"  Updated {result.modified_count} documents with urgency=false")
        
        # Step 2: Adjust existing urgency assignments if needed
        # Check if we need to convert some urgent to non-urgent or vice versa
        new_urgent_count = collection.count_documents({"urgency": True})
        new_non_urgent_count = collection.count_documents({"urgency": False})
        
        logger.info(f"\nAfter Step 1:")
        logger.info(f"  Urgent documents: {new_urgent_count}")
        logger.info(f"  Non-urgent documents: {new_non_urgent_count}")
        
        # If we have too many urgent, convert some to non-urgent
        if new_urgent_count > target_urgent_count:
            excess_urgent = new_urgent_count - target_urgent_count
            logger.info(f"\nStep 2a: Converting {excess_urgent} urgent documents to non-urgent...")
            
            # Find random urgent documents to convert
            urgent_docs = list(collection.find({"urgency": True}, {"_id": 1}))
            random.shuffle(urgent_docs)
            
            docs_to_convert = urgent_docs[:excess_urgent]
            convert_ids = [doc["_id"] for doc in docs_to_convert]
            
            result = collection.update_many(
                {"_id": {"$in": convert_ids}},
                {"$set": {"urgency": False}}
            )
            updated_count += result.modified_count
            logger.info(f"  Converted {result.modified_count} documents from urgent to non-urgent")
        
        # If we have too few urgent, convert some non-urgent to urgent
        elif new_urgent_count < target_urgent_count:
            need_more_urgent = target_urgent_count - new_urgent_count
            logger.info(f"\nStep 2b: Converting {need_more_urgent} non-urgent documents to urgent...")
            
            # Find random non-urgent documents to convert
            non_urgent_docs = list(collection.find({"urgency": False}, {"_id": 1}))
            random.shuffle(non_urgent_docs)
            
            docs_to_convert = non_urgent_docs[:need_more_urgent]
            convert_ids = [doc["_id"] for doc in docs_to_convert]
            
            result = collection.update_many(
                {"_id": {"$in": convert_ids}},
                {"$set": {"urgency": True}}
            )
            updated_count += result.modified_count
            logger.info(f"  Converted {result.modified_count} documents from non-urgent to urgent")
        
        logger.info(f"\nSuccessfully updated {updated_count} documents with urgency field")
        
    except Exception as e:
        logger.error(f"Error updating urgency field: {e}")

def verify_urgency_distribution():
    """
    Verify urgency distribution and generate statistics
    """
    collection = connect_to_mongodb()
    if collection is None:
        return
    
    try:
        logger.info("\n" + "="*50)
        logger.info("URGENCY VERIFICATION AND STATISTICS")
        logger.info("="*50)
        
        # Get total count
        total_docs = collection.count_documents({})
        logger.info(f"Total documents in collection: {total_docs}")
        
        # Check documents without urgency field
        docs_without_urgency = collection.count_documents({
            "$or": [
                {"urgency": {"$exists": False}},
                {"urgency": None}
            ]
        })
        
        urgent_count = collection.count_documents({"urgency": True})
        non_urgent_count = collection.count_documents({"urgency": False})
        
        logger.info(f"Documents with urgency=true: {urgent_count}")
        logger.info(f"Documents with urgency=false: {non_urgent_count}")
        logger.info(f"Documents without urgency field: {docs_without_urgency}")
        
        # Calculate percentages
        urgent_percentage = (urgent_count / total_docs * 100) if total_docs > 0 else 0
        non_urgent_percentage = (non_urgent_count / total_docs * 100) if total_docs > 0 else 0
        
        logger.info(f"\nURGENCY DISTRIBUTION:")
        logger.info("-" * 30)
        logger.info(f"Urgent (true): {urgent_count} ({urgent_percentage:.1f}%)")
        logger.info(f"Non-urgent (false): {non_urgent_count} ({non_urgent_percentage:.1f}%)")
        
        # Check if distribution is correct
        target_urgent_percentage = 8.0
        urgent_diff = abs(urgent_percentage - target_urgent_percentage)
        
        logger.info(f"\nTARGET vs ACTUAL:")
        logger.info(f"Target urgent percentage: {target_urgent_percentage}%")
        logger.info(f"Actual urgent percentage: {urgent_percentage:.1f}%")
        logger.info(f"Difference: {urgent_diff:.1f}%")
        
        if urgent_diff <= 1.0:
            logger.info("✓ Distribution is within acceptable range!")
        else:
            logger.warning(f"⚠ Distribution differs by {urgent_diff:.1f}% from target")
        
        # Sample documents with urgency
        logger.info(f"\nSAMPLE DOCUMENTS WITH URGENCY:")
        logger.info("-" * 40)
        sample_docs = list(collection.find({
            "urgency": {"$exists": True}
        }).limit(5))
        
        for i, doc in enumerate(sample_docs, 1):
            logger.info(f"Document {i}:")
            logger.info(f"  ID: {doc.get('_id')}")
            logger.info(f"  Urgency: {doc.get('urgency')}")
            # Print a few other fields if they exist
            other_fields = {k: v for k, v in doc.items() if k not in ['_id', 'urgency']}
            if other_fields:
                logger.info(f"  Other fields: {list(other_fields.keys())[:3]}...")
            logger.info("")
        
    except Exception as e:
        logger.error(f"Error during verification: {e}")

def main():
    """Main function to execute the urgency assignment and verification"""
    logger.info("Starting Chat Collection Urgency Assignment Process")
    logger.info("="*60)
    
    # Step 1: Assign urgency to documents
    logger.info("Step 1: Assigning urgency to chat documents...")
    assign_urgency_to_chat_collection()
    
    # Step 2: Verify and get statistics
    logger.info("\nStep 2: Verifying urgency and generating statistics...")
    verify_urgency_distribution()
    
    logger.info("\nProcess completed!")

if __name__ == "__main__":
    main()
