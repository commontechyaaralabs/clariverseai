#!/usr/bin/env python3
"""
Script to update voice collection documents from domain: "voice" to domain: "banking"
This fixes the issue where voice documents have domain: "voice" but the API filters by domain: "banking"
"""

import pymongo
import os
from dotenv import load_dotenv
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Database configuration
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "sparzaai")
COLLECTION_NAME = "voice"

def update_voice_domain():
    """Update voice collection documents from domain: 'voice' to domain: 'banking'"""
    
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        voice_collection = db[COLLECTION_NAME]
        
        # Test connection
        client.admin.command('ping')
        logger.info("Successfully connected to MongoDB")
        
        # Check current domain values
        domain_stats = list(voice_collection.aggregate([
            {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))
        logger.info(f"Current domain values: {domain_stats}")
        
        # Count documents with domain: "voice"
        voice_domain_count = voice_collection.count_documents({"domain": "voice"})
        logger.info(f"Documents with domain: 'voice': {voice_domain_count}")
        
        # Count documents with domain: "banking"
        banking_domain_count = voice_collection.count_documents({"domain": "banking"})
        logger.info(f"Documents with domain: 'banking': {banking_domain_count}")
        
        if voice_domain_count == 0:
            logger.info("No documents found with domain: 'voice'. Nothing to update.")
            return
        
        # Update documents from domain: "voice" to domain: "banking"
        logger.info("Updating documents from domain: 'voice' to domain: 'banking'...")
        
        result = voice_collection.update_many(
            {"domain": "voice"},
            {"$set": {"domain": "banking"}}
        )
        
        logger.info(f"Updated {result.modified_count} documents")
        
        # Verify the update
        voice_domain_count_after = voice_collection.count_documents({"domain": "voice"})
        banking_domain_count_after = voice_collection.count_documents({"domain": "banking"})
        
        logger.info(f"After update:")
        logger.info(f"  Documents with domain: 'voice': {voice_domain_count_after}")
        logger.info(f"  Documents with domain: 'banking': {banking_domain_count_after}")
        
        # Check urgency field values after update
        urgent_count = voice_collection.count_documents({"domain": "banking", "urgency": True})
        logger.info(f"Urgent documents with domain: 'banking': {urgent_count}")
        
        # Sample a few documents to verify the update
        sample_docs = list(voice_collection.find({"domain": "banking"}).limit(3))
        logger.info(f"Sample documents after update:")
        for i, doc in enumerate(sample_docs):
            logger.info(f"  Doc {i+1}: domain={doc.get('domain')}, urgency={doc.get('urgency')}, kmeans_cluster_id={doc.get('kmeans_cluster_id')}")
        
        logger.info("Domain update completed successfully!")
        
    except Exception as e:
        logger.error(f"Error updating voice domain: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

if __name__ == "__main__":
    update_voice_domain()
