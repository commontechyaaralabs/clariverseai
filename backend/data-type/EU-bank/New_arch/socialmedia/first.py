# Import required libraries
from pymongo import MongoClient
from collections import defaultdict
import re
import os
from dotenv import load_dotenv
from bson import ObjectId
from datetime import datetime
import logging

# Load environment variables from .env file
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def connect_to_mongodb():
    """Connect to MongoDB and return client and database objects"""
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        logger.info(f"Successfully connected to MongoDB database: {MONGO_DATABASE_NAME}")
        return client, db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def copy_socialmedia_to_social_media():
    """Copy data from socialmedia collection to social_media collection field by field"""
    client, db = connect_to_mongodb()
    
    try:
        # Get source and target collections
        source_collection = db['socialmedia']
        target_collection = db['social_media']
        
        # Count documents in source collection
        total_docs = source_collection.count_documents({})
        logger.info(f"Found {total_docs} documents in source collection 'socialmedia'")
        
        if total_docs == 0:
            logger.warning("No documents found in source collection")
            return
        
        # Define all possible fields to check for
        possible_fields = [
            'email_id', 'username', 'channel', 'dominant_topic', 'subtopics', 
            'created_at', 'domain', 'user_id', 'post_id', 'tweet_id', 'review_id',
            'dominant_cluster_label', 'kmeans_cluster_id', 'subcluster_id', 
            'subcluster_label', 'kmeans_cluster_keyphrase'
        ]
        
        processed_count = 0
        inserted_count = 0
        error_count = 0
        
        # Process each document individually
        cursor = source_collection.find({})
        
        for doc in cursor:
            try:
                logger.info(f"Processing document {processed_count + 1}/{total_docs} - ID: {doc.get('_id', 'unknown')}")
                
                # Create new document starting with metadata
                new_doc = {
                    'original_id': str(doc.get('_id', '')),
                    'migration_timestamp': datetime.utcnow(),
                    'migration_source': 'socialmedia_collection'
                }
                
                # Check each possible field and copy only if it exists
                fields_copied = []
                for field in possible_fields:
                    if field in doc and doc[field] is not None:
                        new_doc[field] = doc[field]
                        fields_copied.append(field)
                        logger.info(f"  - Copied field '{field}': {doc[field]}")
                
                logger.info(f"  - Total fields copied: {len(fields_copied)} - {fields_copied}")
                
                # Insert the document immediately
                result = target_collection.insert_one(new_doc)
                if result.inserted_id:
                    inserted_count += 1
                    logger.info(f"  - Successfully inserted document with ID: {result.inserted_id}")
                else:
                    logger.error(f"  - Failed to insert document")
                    error_count += 1
                
                processed_count += 1
                
                # Log progress every 100 documents
                if processed_count % 100 == 0:
                    logger.info(f"Progress: {processed_count}/{total_docs} documents processed")
                
            except Exception as e:
                logger.error(f"Error processing document {doc.get('_id', 'unknown')}: {e}")
                error_count += 1
                processed_count += 1
                continue
        
        # Create indexes for better performance
        try:
            target_collection.create_index("email_id")
            target_collection.create_index("channel")
            target_collection.create_index("domain")
            target_collection.create_index("created_at")
            target_collection.create_index("user_id")
            target_collection.create_index("kmeans_cluster_id")
            logger.info("Created indexes on target collection")
        except Exception as e:
            logger.warning(f"Could not create indexes: {e}")
        
        # Summary
        logger.info(f"Migration completed:")
        logger.info(f"  - Total documents processed: {processed_count}")
        logger.info(f"  - Successfully inserted: {inserted_count}")
        logger.info(f"  - Errors encountered: {error_count}")
        
        # Verify the migration
        target_count = target_collection.count_documents({})
        logger.info(f"  - Final count in target collection: {target_count}")
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        raise
    finally:
        client.close()
        logger.info("MongoDB connection closed")

def verify_migration():
    """Verify the migration by checking sample documents"""
    client, db = connect_to_mongodb()
    
    try:
        target_collection = db['social_media']
        
        # Get sample documents
        sample_docs = list(target_collection.find({}).limit(3))
        
        logger.info("Sample migrated documents:")
        for i, doc in enumerate(sample_docs, 1):
            logger.info(f"Document {i}:")
            logger.info(f"  - email_id: {doc.get('email_id', 'N/A')}")
            logger.info(f"  - username: {doc.get('username', 'N/A')}")
            logger.info(f"  - channel: {doc.get('channel', 'N/A')}")
            logger.info(f"  - dominant_topic: {doc.get('dominant_topic', 'N/A')}")
            logger.info(f"  - domain: {doc.get('domain', 'N/A')}")
            logger.info(f"  - kmeans_cluster_id: {doc.get('kmeans_cluster_id', 'N/A')}")
            logger.info("  ---")
        
        # Count by channel
        pipeline = [
            {"$group": {"_id": "$channel", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        channel_counts = list(target_collection.aggregate(pipeline))
        
        logger.info("Documents by channel:")
        for item in channel_counts:
            logger.info(f"  - {item['_id']}: {item['count']}")
            
    except Exception as e:
        logger.error(f"Verification failed: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    try:
        logger.info("Starting socialmedia to social_media migration...")
        copy_socialmedia_to_social_media()
        logger.info("Migration completed successfully!")
        
        # Verify the migration
        logger.info("Verifying migration...")
        verify_migration()
        
    except Exception as e:
        logger.error(f"Script failed: {e}")
        exit(1)
