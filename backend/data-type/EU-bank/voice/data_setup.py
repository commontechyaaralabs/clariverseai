#!/usr/bin/env python3
"""
Voice Data Setup Script for SparzaAI Database
Generates 2000 voice call records with specific customer distribution:
- 1300 unique customers with 1 record each (65%)
- 250 unique customers with 2 records each (25%) 
- 30 unique customers with 4 records each (6%)
- 20 unique customers with 6 records each (4%)
Total: 1600 unique customers, 2000 total records
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
        logging.FileHandler('voice_data_setup.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "sparzaai")
COLLECTION_NAME = "voice"

class VoiceDataGenerator:
    def __init__(self, connection_string, database_name):
        """
        Initialize the Voice data generator
        
        Args:
            connection_string (str): MongoDB connection string
            database_name (str): Database name
        """
        self.connection_string = connection_string
        self.database_name = database_name
        self.client = None
        self.db = None
        self.collection = None
        
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
            
            logger.info(f"Connected to database: {self.database_name}, collection: {COLLECTION_NAME}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def generate_random_call_id(self):
        """Generate a random call ID with CALL prefix and 5 digits (CALLXXXXX)"""
        digits = ''.join(random.choices(string.digits, k=5))
        return f"CALL{digits}"
    
    def generate_random_customer_id(self):
        """Generate a random customer ID with CUST prefix and 3 digits (CUSTXXX)"""
        digits = ''.join(random.choices(string.digits, k=3))
        return f"CUST{digits}"
    
    def generate_sequential_customer_ids(self, count):
        """Generate a list of unique customer IDs efficiently"""
        customer_ids = []
        for i in range(count):
            # Use zero-padded sequential numbers to ensure uniqueness
            customer_id = f"CUST{str(i+1).zfill(4)}"  # CUST0001, CUST0002, etc.
            customer_ids.append(customer_id)
        return customer_ids
    
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
    
    def generate_voice_record(self, email_data, customer_id):
        """Generate a single voice record"""
        return {
            "call_id": self.generate_random_call_id(),
            "timestamp": self.generate_random_timestamp(),
            "customer_name": email_data["sender_name"],
            "customer_id": customer_id,
            "email": email_data["sender_id"]
        }
    
    def create_customer_distribution(self, email_data_list):
        """
        Create customer distribution according to requirements:
        - 1300 unique customers with 1 record each (65% = 1300 records)
        - 250 unique customers with 2 records each (25% = 500 records)
        - 30 unique customers with 4 records each (6% = 120 records)
        - 20 unique customers with 6 records each (4% = 80 records)
        Total: 1600 unique customers, 2000 total records
        """
        logger.info("Creating customer distribution...")
        
        # Track used customer IDs to ensure uniqueness
        used_customer_ids = set()
        distribution_records = []
        
        # Create a list to hold all unique customer data
        unique_customers = []
        
        # Pre-generate 1600 unique customers with their email data
        logger.info("Pre-generating 1600 unique customers...")
        
        # Generate all unique customer IDs at once (much faster)
        customer_ids = self.generate_sequential_customer_ids(1600)
        
        for i in range(1600):
            customer_id = customer_ids[i]
            email_data = email_data_list[i % len(email_data_list)]
            unique_customers.append({
                "customer_id": customer_id,
                "email_data": email_data
            })
        
        customer_index = 0
        
        # 1. Generate 1300 customers with 1 record each (1300 records)
        logger.info("Generating 1300 customers with 1 record each...")
        for i in range(1300):
            customer = unique_customers[customer_index]
            record = self.generate_voice_record(customer["email_data"], customer["customer_id"])
            distribution_records.append(record)
            customer_index += 1
        
        # 2. Generate 250 customers with 2 records each (500 records)
        logger.info("Generating 250 customers with 2 records each...")
        for i in range(250):
            customer = unique_customers[customer_index]
            # Generate 2 records for this customer
            for j in range(2):
                record = self.generate_voice_record(customer["email_data"], customer["customer_id"])
                distribution_records.append(record)
            customer_index += 1
        
        # 3. Generate 30 customers with 4 records each (120 records)
        logger.info("Generating 30 customers with 4 records each...")
        for i in range(30):
            customer = unique_customers[customer_index]
            # Generate 4 records for this customer
            for j in range(4):
                record = self.generate_voice_record(customer["email_data"], customer["customer_id"])
                distribution_records.append(record)
            customer_index += 1
        
        # 4. Generate 20 customers with 6 records each (80 records)
        logger.info("Generating 20 customers with 6 records each...")
        for i in range(20):
            customer = unique_customers[customer_index]
            # Generate 6 records for this customer
            for j in range(6):
                record = self.generate_voice_record(customer["email_data"], customer["customer_id"])
                distribution_records.append(record)
            customer_index += 1
        
        # Shuffle the records to randomize order
        random.shuffle(distribution_records)
        
        logger.info(f"Distribution created successfully:")
        logger.info(f"  - 1 record customers: 1300 unique customers × 1 record = 1300 records")
        logger.info(f"  - 2 record customers: 250 unique customers × 2 records = 500 records")
        logger.info(f"  - 4 record customers: 30 unique customers × 4 records = 120 records")
        logger.info(f"  - 6 record customers: 20 unique customers × 6 records = 80 records")
        logger.info(f"  - Total unique customers: {customer_index}")
        logger.info(f"  - Total records: {len(distribution_records)}")
        
        return distribution_records
    
    def insert_records(self, records, batch_size=100):
        """Insert records into MongoDB collection in batches"""
        try:
            total_inserted = 0
            batches = (len(records) + batch_size - 1) // batch_size
            
            for batch_num in range(batches):
                start_idx = batch_num * batch_size
                end_idx = min((batch_num + 1) * batch_size, len(records))
                batch_records = records[start_idx:end_idx]
                
                if batch_records:
                    result = self.collection.insert_many(batch_records)
                    batch_inserted = len(result.inserted_ids)
                    total_inserted += batch_inserted
                    logger.info(f"Batch {batch_num + 1}/{batches}: Inserted {batch_inserted} records (Total: {total_inserted})")
                    
                    # Small delay to avoid overwhelming the database
                    if batch_num < batches - 1:
                        import time
                        time.sleep(0.1)
            
            logger.info(f"Successfully inserted all {total_inserted} records")
            return total_inserted
            
        except Exception as e:
            logger.error(f"Failed to insert records: {e}")
            return 0
    
    def generate_and_insert_data(self, email_data_list):
        """Generate and insert voice data with proper customer distribution"""
        logger.info("Starting voice data generation with customer distribution...")
        
        # Create customer distribution
        records = self.create_customer_distribution(email_data_list)
        
        if not records:
            logger.error("No records generated")
            return 0
        
        # Insert all records
        total_inserted = self.insert_records(records)
        
        logger.info(f"Data generation completed. Total records inserted: {total_inserted}")
        return total_inserted
    
    def verify_data(self):
        """Verify the data was inserted correctly and show distribution"""
        try:
            total_count = self.collection.count_documents({})
            logger.info(f"Collection '{COLLECTION_NAME}' contains {total_count} documents")
            
            # Verify customer distribution
            pipeline = [
                {"$group": {"_id": "$customer_id", "count": {"$sum": 1}}},
                {"$group": {"_id": "$count", "customers": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            
            distribution = list(self.collection.aggregate(pipeline))
            logger.info("Customer distribution verification:")
            
            total_customers = 0
            total_records_check = 0
            for dist in distribution:
                records_per_customer = dist["_id"]
                customer_count = dist["customers"]
                total_records = records_per_customer * customer_count
                total_customers += customer_count
                total_records_check += total_records
                percentage = (total_records / total_count) * 100 if total_count > 0 else 0
                logger.info(f"  - {customer_count} customers with {records_per_customer} record(s) each = {total_records} records ({percentage:.1f}%)")
            
            logger.info(f"Total unique customers: {total_customers}")
            logger.info(f"Total records verification: {total_records_check}")
            
            # Show a sample record
            sample = self.collection.find_one()
            if sample:
                logger.info(f"Sample record: {sample}")
            
            return total_count
            
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
    
    def close_connection(self):
        """Close MongoDB connection"""
        try:
            if self.client:
                self.client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")

def main():
    """Main function to run the Voice data generation process"""
    logger.info("=== Voice Data Setup Script Started ===")
    
    # Initialize the data generator
    generator = VoiceDataGenerator(MONGO_CONNECTION_STRING, MONGO_DATABASE_NAME)
    
    try:
        # Connect to MongoDB
        if not generator.connect_to_mongodb():
            logger.error("Failed to connect to MongoDB. Exiting.")
            return
        
        # Get random email data from emailmessages collection
        logger.info("Fetching random email data from emailmessages collection...")
        email_data = generator.get_random_email_data(2000)
        
        if not email_data:
            logger.error("Failed to retrieve email data. Exiting.")
            return
        
        # Generate and insert data with proper distribution
        total_inserted = generator.generate_and_insert_data(email_data)
        
        if total_inserted > 0:
            # Verify the data and distribution
            generator.verify_data()
            logger.info("✅ Voice data generation completed successfully!")
            
            # Final summary
            logger.info("\n" + "="*50)
            logger.info("FINAL DISTRIBUTION SUMMARY:")
            logger.info("="*50)
            logger.info("1 record customers: 1300 unique customers × 1 record = 1300 records (65%)")
            logger.info("2 record customers: 250 unique customers × 2 records = 500 records (25%)")
            logger.info("4 record customers: 30 unique customers × 4 records = 120 records (6%)")
            logger.info("6 record customers: 20 unique customers × 6 records = 80 records (4%)")
            logger.info("-" * 50)
            logger.info(f"Total unique customers: 1600")
            logger.info(f"Total records: 2000")
            logger.info("="*50)
        else:
            logger.error("❌ No data was inserted. Check the logs for errors.")
            
    except Exception as e:
        logger.error(f"An error occurred during data generation: {e}")
        
    finally:
        # Clean up connections
        generator.close_connection()
        logger.info("=== Voice Data Setup Script Completed ===")

if __name__ == "__main__":
    main()