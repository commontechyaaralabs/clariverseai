#!/usr/bin/env python3
"""
Twitter Data Setup Script for SparzaAI Database
Generates 2000 random Twitter records and stores them in a 'twitter' collection
"""

import os
import random
import string
from datetime import datetime, timedelta
import pymongo
from pymongo import MongoClient
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('twitter_data_setup.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "sparzaai")
COLLECTION_NAME = "twitter"

class TwitterDataGenerator:
    def __init__(self, connection_string, database_name):
        """
        Initialize the Twitter data generator
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str): Database name
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
        self.collection = None
        self.used_tweet_ids = set()  # Track used tweet IDs to avoid duplicates
        
    def connect_to_mongodb(self):
        """Connect to MongoDB database"""
        try:
            logger.info("Connecting to MongoDB...")
            self.client = MongoClient(self.connection_string)
            
            # Test connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            
            # Get database and collection
            self.db = self.client[self.database_name]
            self.collection = self.db[COLLECTION_NAME]
            
            # Check if collection exists, if not create it
            if COLLECTION_NAME not in self.db.list_collection_names():
                logger.info(f"Creating collection '{COLLECTION_NAME}'")
                self.db.create_collection(COLLECTION_NAME)
            
            # Create unique index on tweet_id to prevent duplicates
            self.create_unique_index()
            
            # Load existing tweet IDs to avoid duplicates
            self.load_existing_tweet_ids()
            
            logger.info(f"Connected to database: {self.database_name}, collection: {COLLECTION_NAME}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def create_unique_index(self):
        """Create a unique index on tweet_id field to prevent duplicates at database level"""
        try:
            # Check if index already exists
            existing_indexes = self.collection.list_indexes()
            index_names = [index['name'] for index in existing_indexes]
            
            if 'tweet_id_1' not in index_names:
                self.collection.create_index("tweet_id", unique=True)
                logger.info("Created unique index on tweet_id field")
            else:
                logger.info("Unique index on tweet_id field already exists")
        except Exception as e:
            logger.warning(f"Could not create unique index: {e}")
    
    def load_existing_tweet_ids(self):
        """Load existing tweet IDs from the collection to avoid duplicates"""
        try:
            existing_ids = self.collection.distinct("tweet_id")
            self.used_tweet_ids.update(existing_ids)
            logger.info(f"Loaded {len(existing_ids)} existing tweet IDs to avoid duplicates")
        except Exception as e:
            logger.warning(f"Could not load existing tweet IDs: {e}")
    
    def generate_unique_tweet_id(self):
        """Generate a unique tweet ID with T prefix and 6 digits (TXXXXXX)"""
        max_attempts = 1000  # Prevent infinite loops
        
        for _ in range(max_attempts):
            # Use 6 digits instead of 4 for more combinations (1,000,000 possible IDs)
            digits = ''.join(random.choices(string.digits, k=6))
            tweet_id = f"T{digits}"
            
            if tweet_id not in self.used_tweet_ids:
                self.used_tweet_ids.add(tweet_id)
                return tweet_id
        
        # If we run out of attempts, use timestamp-based ID
        timestamp_id = f"T{int(datetime.now().timestamp())}"
        self.used_tweet_ids.add(timestamp_id)
        logger.warning(f"Using timestamp-based tweet ID: {timestamp_id}")
        return timestamp_id
    
    def generate_random_user_id(self):
        """Generate a random user ID with 6 digits (XXXXXX)"""
        return ''.join(random.choices(string.digits, k=6))
    
    def generate_random_timestamp(self):
        """Generate a random timestamp within 2023-2025 range"""
        start_date = datetime(2023, 1, 1, 0, 0, 0)  # January 1, 2023
        end_date = datetime(2025, 12, 31, 23, 59, 59)  # December 31, 2025
        
        # Generate random timestamp between start and end date
        time_between_dates = end_date - start_date
        days_between_dates = time_between_dates.days
        random_days = random.randrange(days_between_dates)
        random_seconds = random.randrange(86400)  # seconds in a day
        
        random_date = start_date + timedelta(days=random_days, seconds=random_seconds)
        return random_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    def generate_twitter_record(self, email_data):
        """Generate a single Twitter record"""
        return {
            "tweet_id": self.generate_unique_tweet_id(),
            "created_at": self.generate_random_timestamp(),
            "user_id": self.generate_random_user_id(),
            "username": email_data["sender_name"],
            "email_id": email_data["sender_id"]
        }
    
    def generate_batch_records(self, email_data_list, batch_size=100):
        """Generate a batch of Twitter records"""
        records = []
        for i in range(batch_size):
            # Get email data from the provided list, cycling through if needed
            email_data = email_data_list[i % len(email_data_list)]
            records.append(self.generate_twitter_record(email_data))
        return records
    
    def insert_records(self, records):
        """Insert records into MongoDB collection"""
        try:
            if records:
                result = self.collection.insert_many(records)
                logger.info(f"Successfully inserted {len(result.inserted_ids)} records")
                return len(result.inserted_ids)
            return 0
        except pymongo.errors.BulkWriteError as e:
            # Handle duplicate key errors
            duplicate_count = 0
            for error in e.details.get('writeErrors', []):
                if error.get('code') == 11000:  # Duplicate key error
                    duplicate_count += 1
                    # Remove the duplicate ID from our tracking set
                    duplicate_doc = error.get('op', {})
                    if 'tweet_id' in duplicate_doc:
                        self.used_tweet_ids.discard(duplicate_doc['tweet_id'])
            
            if duplicate_count > 0:
                logger.warning(f"Found {duplicate_count} duplicate tweet IDs, retrying with new IDs...")
                # Regenerate records with new IDs
                new_records = []
                for record in records:
                    new_record = record.copy()
                    new_record['tweet_id'] = self.generate_unique_tweet_id()
                    new_records.append(new_record)
                
                # Try to insert the new records
                try:
                    result = self.collection.insert_many(new_records)
                    logger.info(f"Successfully inserted {len(result.inserted_ids)} records after retry")
                    return len(result.inserted_ids)
                except Exception as retry_error:
                    logger.error(f"Failed to insert records after retry: {retry_error}")
                    return 0
            
            logger.error(f"Bulk write error: {e}")
            return 0
        except Exception as e:
            logger.error(f"Failed to insert records: {e}")
            return 0
    
    def generate_and_insert_data(self, email_data_list, total_records=2000, batch_size=100):
        """Generate and insert Twitter data in batches"""
        logger.info(f"Starting to generate {total_records} Twitter records...")
        
        total_inserted = 0
        batches = (total_records + batch_size - 1) // batch_size  # Calculate number of batches
        
        for batch_num in range(batches):
            # Calculate records for this batch
            current_batch_size = min(batch_size, total_records - total_inserted)
            
            logger.info(f"Generating batch {batch_num + 1}/{batches} ({current_batch_size} records)")
            
            # Generate batch records
            batch_records = self.generate_batch_records(email_data_list, current_batch_size)
            
            # Insert batch records
            inserted_count = self.insert_records(batch_records)
            total_inserted += inserted_count
            
            logger.info(f"Batch {batch_num + 1} completed. Total inserted: {total_inserted}/{total_records}")
            
            # Small delay to avoid overwhelming the database
            if batch_num < batches - 1:  # Don't delay after the last batch
                import time
                time.sleep(0.1)
        
        logger.info(f"Data generation completed. Total records inserted: {total_inserted}")
        return total_inserted
    
    def check_for_duplicates(self):
        """Check for any duplicate tweet IDs in the collection"""
        try:
            # Use aggregation to find duplicates
            pipeline = [
                {"$group": {
                    "_id": "$tweet_id",
                    "count": {"$sum": 1}
                }},
                {"$match": {
                    "count": {"$gt": 1}
                }},
                {"$sort": {"count": -1}}
            ]
            
            duplicates = list(self.collection.aggregate(pipeline))
            
            if duplicates:
                logger.warning(f"Found {len(duplicates)} duplicate tweet IDs:")
                for dup in duplicates[:10]:  # Show first 10 duplicates
                    logger.warning(f"  Tweet ID '{dup['_id']}' appears {dup['count']} times")
                if len(duplicates) > 10:
                    logger.warning(f"  ... and {len(duplicates) - 10} more duplicates")
                return len(duplicates)
            else:
                logger.info("✅ No duplicate tweet IDs found")
                return 0
                
        except Exception as e:
            logger.error(f"Failed to check for duplicates: {e}")
            return -1
    
    def verify_data(self):
        """Verify the data was inserted correctly"""
        try:
            count = self.collection.count_documents({})
            logger.info(f"Collection '{COLLECTION_NAME}' contains {count} documents")
            
            # Verify tweet ID uniqueness
            unique_tweet_ids = self.collection.distinct("tweet_id")
            if len(unique_tweet_ids) == count:
                logger.info("✅ All tweet IDs are unique!")
            else:
                logger.warning(f"⚠️  Found {count - len(unique_tweet_ids)} duplicate tweet IDs")
            
            # Perform comprehensive duplicate check
            duplicate_count = self.check_for_duplicates()
            
            # Show a sample record
            sample = self.collection.find_one()
            if sample:
                logger.info(f"Sample record: {sample}")
            
            return count
        except Exception as e:
            logger.error(f"Failed to verify data: {e}")
            return 0
    
    def get_random_email_data(self, count=2000):
        """Get random sender_id and sender_name from emailmessages collection"""
        try:
            # Get the emailmessages collection
            email_collection = self.db["emailmessages"]
            
            # Use MongoDB's $sample aggregation to get random documents
            pipeline = [
                {"$sample": {"size": count}},
                {"$project": {"sender_id": 1, "sender_name": 1}}
            ]
            
            random_emails = list(email_collection.aggregate(pipeline))
            email_data = [{"sender_id": email["sender_id"], "sender_name": email["sender_name"]} for email in random_emails]
            
            logger.info(f"Retrieved {len(email_data)} random email records from emailmessages collection")
            return email_data
            
        except Exception as e:
            logger.error(f"Failed to get random email data: {e}")
            return []
    
    def cleanup_duplicates(self):
        """Remove duplicate tweet IDs, keeping only the first occurrence"""
        try:
            # Find duplicates
            pipeline = [
                {"$group": {
                    "_id": "$tweet_id",
                    "ids": {"$push": "$_id"},
                    "count": {"$sum": 1}
                }},
                {"$match": {
                    "count": {"$gt": 1}
                }}
            ]
            
            duplicates = list(self.collection.aggregate(pipeline))
            
            if not duplicates:
                logger.info("No duplicates found to clean up")
                return 0
            
            total_removed = 0
            for dup in duplicates:
                # Keep the first document, remove the rest
                ids_to_remove = dup['ids'][1:]  # Skip first ID
                result = self.collection.delete_many({"_id": {"$in": ids_to_remove}})
                total_removed += result.deleted_count
                logger.info(f"Removed {result.deleted_count} duplicate records for tweet ID '{dup['_id']}'")
            
            logger.info(f"Cleanup completed. Total duplicate records removed: {total_removed}")
            return total_removed
            
        except Exception as e:
            logger.error(f"Failed to cleanup duplicates: {e}")
            return 0
    
    def close_connection(self):
        """Close MongoDB connection"""
        try:
            if self.client:
                self.client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")

def main():
    """Main function to run the Twitter data generation process"""
    logger.info("=== Twitter Data Setup Script Started ===")
    
    # Initialize the data generator
    generator = TwitterDataGenerator(MONGO_CONNECTION_STRING, MONGO_DATABASE_NAME)
    
    try:
        # Connect to MongoDB
        if not generator.connect_to_mongodb():
            logger.error("Failed to connect to MongoDB. Exiting.")
            return
        
        # Check for existing duplicates and clean them up
        logger.info("Checking for existing duplicates...")
        existing_duplicates = generator.check_for_duplicates()
        if existing_duplicates > 0:
            logger.info(f"Found {existing_duplicates} existing duplicates. Cleaning up...")
            generator.cleanup_duplicates()
        
        # Get random email data from emailmessages collection
        logger.info("Fetching random email data from emailmessages collection...")
        email_data = generator.get_random_email_data(2000)
        
        if not email_data:
            logger.error("Failed to retrieve email data. Exiting.")
            return
        
        # Generate and insert data
        total_inserted = generator.generate_and_insert_data(email_data, total_records=2000)
        
        if total_inserted > 0:
            # Verify the data
            generator.verify_data()
            logger.info("✅ Twitter data generation completed successfully!")
        else:
            logger.error("❌ No data was inserted. Check the logs for errors.")
            
    except Exception as e:
        logger.error(f"An error occurred during data generation: {e}")
        
    finally:
        # Clean up connections
        generator.close_connection()
        logger.info("=== Twitter Data Setup Script Completed ===")

if __name__ == "__main__":
    main()
