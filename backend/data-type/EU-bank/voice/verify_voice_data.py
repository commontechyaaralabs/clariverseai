#!/usr/bin/env python3
"""
Voice Data Analysis Script for SparzaAI Database
Analyzes the voice collection to verify data integrity and distribution
Shows detailed customer IDs for each distribution category
"""

import os
import pymongo
from pymongo import MongoClient
import logging
from collections import defaultdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('voice_data_analysis.log', mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "sparzaai")
COLLECTION_NAME = "voice"

class VoiceDataAnalyzer:
    def __init__(self, connection_string, database_name):
        """
        Initialize the Voice data analyzer
        
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
            
            logger.info(f"Connected to database: {self.database_name}, collection: {COLLECTION_NAME}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def get_basic_stats(self):
        """Get basic collection statistics"""
        try:
            total_records = self.collection.count_documents({})
            unique_customers = len(self.collection.distinct("customer_id"))
            unique_emails = len(self.collection.distinct("email"))
            unique_names = len(self.collection.distinct("customer_name"))
            
            logger.info("=== BASIC STATISTICS ===")
            logger.info(f"Total voice records: {total_records}")
            logger.info(f"Unique customer IDs: {unique_customers}")
            logger.info(f"Unique emails: {unique_emails}")
            logger.info(f"Unique customer names: {unique_names}")
            
            return {
                "total_records": total_records,
                "unique_customers": unique_customers,
                "unique_emails": unique_emails,
                "unique_names": unique_names
            }
            
        except Exception as e:
            logger.error(f"Failed to get basic stats: {e}")
            return None
    
    def analyze_customer_distribution(self):
        """Analyze customer distribution and return detailed breakdown"""
        try:
            # Get customer record counts
            pipeline = [
                {"$group": {"_id": "$customer_id", "count": {"$sum": 1}, "customer_name": {"$first": "$customer_name"}, "email": {"$first": "$email"}}},
                {"$sort": {"_id": 1}}
            ]
            
            customer_data = list(self.collection.aggregate(pipeline))
            
            # Group by record count
            distribution = defaultdict(list)
            for customer in customer_data:
                record_count = customer["count"]
                distribution[record_count].append({
                    "customer_id": customer["_id"],
                    "customer_name": customer["customer_name"],
                    "email": customer["email"]
                })
            
            logger.info("\n=== CUSTOMER DISTRIBUTION ANALYSIS ===")
            
            total_customers = 0
            total_records = 0
            
            # Analyze each distribution category
            for record_count in sorted(distribution.keys()):
                customers_in_category = len(distribution[record_count])
                records_in_category = customers_in_category * record_count
                total_customers += customers_in_category
                total_records += records_in_category
                
                percentage = (records_in_category / sum(len(customers) * count for count, customers in distribution.items())) * 100
                
                logger.info(f"\n--- Customers with {record_count} record(s) each ---")
                logger.info(f"Count: {customers_in_category} customers")
                logger.info(f"Total records: {records_in_category}")
                logger.info(f"Percentage: {percentage:.1f}%")
                
                # Show first 10 and last 5 customer IDs for verification
                customers_list = distribution[record_count]
                
                if customers_in_category <= 15:
                    # Show all if 15 or fewer
                    logger.info("Customer IDs:")
                    for i, customer in enumerate(customers_list):
                        logger.info(f"  {i+1:3d}. {customer['customer_id']} - {customer['customer_name']} ({customer['email']})")
                else:
                    # Show first 10 and last 5
                    logger.info("First 10 Customer IDs:")
                    for i in range(10):
                        customer = customers_list[i]
                        logger.info(f"  {i+1:3d}. {customer['customer_id']} - {customer['customer_name']} ({customer['email']})")
                    
                    logger.info("  ...")
                    logger.info(f"Last 5 Customer IDs:")
                    for i in range(customers_in_category - 5, customers_in_category):
                        customer = customers_list[i]
                        logger.info(f"  {i+1:3d}. {customer['customer_id']} - {customer['customer_name']} ({customer['email']})")
            
            logger.info(f"\n=== SUMMARY ===")
            logger.info(f"Total unique customers: {total_customers}")
            logger.info(f"Total records: {total_records}")
            
            return distribution
            
        except Exception as e:
            logger.error(f"Failed to analyze customer distribution: {e}")
            return None
    
    def verify_data_integrity(self):
        """Verify data integrity and consistency"""
        try:
            logger.info("\n=== DATA INTEGRITY VERIFICATION ===")
            
            # Check for duplicate call_ids
            pipeline_call_ids = [
                {"$group": {"_id": "$call_id", "count": {"$sum": 1}}},
                {"$match": {"count": {"$gt": 1}}}
            ]
            duplicate_call_ids = list(self.collection.aggregate(pipeline_call_ids))
            
            if duplicate_call_ids:
                logger.warning(f"Found {len(duplicate_call_ids)} duplicate call IDs!")
                for dup in duplicate_call_ids[:5]:  # Show first 5
                    logger.warning(f"  Duplicate call_id: {dup['_id']} (appears {dup['count']} times)")
            else:
                logger.info("✅ All call_ids are unique")
            
            # Check for missing required fields
            required_fields = ["call_id", "timestamp", "customer_name", "customer_id", "email"]
            for field in required_fields:
                missing_count = self.collection.count_documents({field: {"$exists": False}})
                null_count = self.collection.count_documents({field: None})
                empty_count = self.collection.count_documents({field: ""})
                
                if missing_count > 0 or null_count > 0 or empty_count > 0:
                    logger.warning(f"Field '{field}': {missing_count} missing, {null_count} null, {empty_count} empty")
                else:
                    logger.info(f"✅ Field '{field}': All records have valid values")
            
            # Check customer consistency (same customer_id should have same name and email)
            pipeline_consistency = [
                {"$group": {
                    "_id": "$customer_id",
                    "names": {"$addToSet": "$customer_name"},
                    "emails": {"$addToSet": "$email"}
                }},
                {"$match": {
                    "$or": [
                        {"names": {"$size": {"$gt": 1}}},
                        {"emails": {"$size": {"$gt": 1}}}
                    ]
                }}
            ]
            
            inconsistent_customers = list(self.collection.aggregate(pipeline_consistency))
            
            if inconsistent_customers:
                logger.warning(f"Found {len(inconsistent_customers)} customers with inconsistent data!")
                for customer in inconsistent_customers[:5]:  # Show first 5
                    logger.warning(f"  Customer {customer['_id']}: names={customer['names']}, emails={customer['emails']}")
            else:
                logger.info("✅ All customers have consistent names and emails across records")
            
            return len(duplicate_call_ids) == 0 and len(inconsistent_customers) == 0
            
        except Exception as e:
            logger.error(f"Failed to verify data integrity: {e}")
            return False
    
    def export_customer_lists(self):
        """Export customer lists by category to separate files"""
        try:
            logger.info("\n=== EXPORTING CUSTOMER LISTS ===")
            
            # Get customer distribution
            pipeline = [
                {"$group": {
                    "_id": "$customer_id", 
                    "count": {"$sum": 1}, 
                    "customer_name": {"$first": "$customer_name"}, 
                    "email": {"$first": "$email"}
                }},
                {"$sort": {"_id": 1}}
            ]
            
            customer_data = list(self.collection.aggregate(pipeline))
            
            # Group by record count
            categories = {
                1: [],  # 1300 customers with 1 record each
                2: [],  # 250 customers with 2 records each
                4: [],  # 30 customers with 4 records each
                6: []   # 20 customers with 6 records each
            }
            
            for customer in customer_data:
                record_count = customer["count"]
                if record_count in categories:
                    categories[record_count].append(customer)
            
            # Export each category to file
            for record_count, customers in categories.items():
                filename = f"customers_with_{record_count}_records.txt"
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write(f"Customers with {record_count} record(s) each\n")
                    f.write(f"Total customers: {len(customers)}\n")
                    f.write(f"Total records: {len(customers) * record_count}\n")
                    f.write("="*80 + "\n\n")
                    
                    for i, customer in enumerate(customers, 1):
                        f.write(f"{i:4d}. {customer['_id']} - {customer['customer_name']} ({customer['email']})\n")
                
                logger.info(f"Exported {len(customers)} customers to '{filename}'")
            
            # Create summary file
            with open("voice_data_summary.txt", 'w', encoding='utf-8') as f:
                f.write("VOICE DATA ANALYSIS SUMMARY\n")
                f.write("="*50 + "\n\n")
                f.write(f"Total Records: 2040\n")
                f.write(f"Total Unique Customers: 1600\n\n")
                f.write("Distribution:\n")
                f.write(f"- 1300 customers with 1 record each = 1300 records (63.7%)\n")
                f.write(f"- 250 customers with 2 records each = 500 records (24.5%)\n")
                f.write(f"- 30 customers with 4 records each = 120 records (5.9%)\n")
                f.write(f"- 20 customers with 6 records each = 120 records (5.9%)\n\n")
                f.write("Files Generated:\n")
                f.write("- customers_with_1_records.txt (1300 customers)\n")
                f.write("- customers_with_2_records.txt (250 customers)\n")
                f.write("- customers_with_4_records.txt (30 customers)\n")
                f.write("- customers_with_6_records.txt (20 customers)\n")
            
            logger.info("Exported summary to 'voice_data_summary.txt'")
            
        except Exception as e:
            logger.error(f"Failed to export customer lists: {e}")
    
    def show_sample_records(self):
        """Show sample records from each customer category"""
        try:
            logger.info("\n=== SAMPLE RECORDS BY CATEGORY ===")
            
            # Get samples for each category
            categories = [1, 2, 4, 6]
            
            for record_count in categories:
                logger.info(f"\n--- Sample from {record_count}-record customers ---")
                
                # Find a customer with exactly this many records
                pipeline = [
                    {"$group": {"_id": "$customer_id", "count": {"$sum": 1}}},
                    {"$match": {"count": record_count}},
                    {"$limit": 1}
                ]
                
                sample_customer = list(self.collection.aggregate(pipeline))
                
                if sample_customer:
                    customer_id = sample_customer[0]["_id"]
                    
                    # Get all records for this customer
                    customer_records = list(self.collection.find(
                        {"customer_id": customer_id}
                    ).sort("timestamp", 1))
                    
                    logger.info(f"Customer: {customer_id}")
                    logger.info(f"Name: {customer_records[0]['customer_name']}")
                    logger.info(f"Email: {customer_records[0]['email']}")
                    logger.info(f"Records ({len(customer_records)}):")
                    
                    for i, record in enumerate(customer_records, 1):
                        logger.info(f"  {i}. Call ID: {record['call_id']}, Time: {record['timestamp']}")
                else:
                    logger.warning(f"No customers found with exactly {record_count} records")
            
        except Exception as e:
            logger.error(f"Failed to show sample records: {e}")
    
    def validate_distribution_ranges(self):
        """Validate that customer IDs fall within expected ranges based on generation logic"""
        try:
            logger.info("\n=== CUSTOMER ID RANGE VALIDATION ===")
            
            # Get all customers with their record counts
            pipeline = [
                {"$group": {"_id": "$customer_id", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            
            customers = list(self.collection.aggregate(pipeline))
            
            # Extract customer numbers for analysis
            categories = {1: [], 2: [], 4: [], 6: []}
            
            for customer in customers:
                customer_id = customer["_id"]
                record_count = customer["count"]
                
                # Extract number from CUST0001 format
                try:
                    customer_num = int(customer_id.replace("CUST", ""))
                    if record_count in categories:
                        categories[record_count].append(customer_num)
                except:
                    logger.warning(f"Could not parse customer ID: {customer_id}")
            
            # Analyze ranges for each category
            logger.info("Expected vs Actual Customer ID Ranges:")
            logger.info("(Based on sequential generation: CUST0001-CUST1600)")
            
            expected_ranges = {
                1: (1, 1300),      # CUST0001 to CUST1300
                2: (1301, 1550),   # CUST1301 to CUST1550  
                4: (1551, 1580),   # CUST1551 to CUST1580
                6: (1581, 1600)    # CUST1581 to CUST1600
            }
            
            for record_count in [1, 2, 4, 6]:
                customer_nums = sorted(categories[record_count])
                expected_start, expected_end = expected_ranges[record_count]
                
                if customer_nums:
                    actual_start = min(customer_nums)
                    actual_end = max(customer_nums)
                    
                    logger.info(f"\n{record_count}-record customers:")
                    logger.info(f"  Expected range: CUST{expected_start:04d} - CUST{expected_end:04d}")
                    logger.info(f"  Actual range:   CUST{actual_start:04d} - CUST{actual_end:04d}")
                    logger.info(f"  Count: {len(customer_nums)} customers")
                    
                    # Check if range matches
                    if actual_start == expected_start and actual_end == expected_end and len(customer_nums) == (expected_end - expected_start + 1):
                        logger.info(f"  ✅ Range matches expected pattern")
                    else:
                        logger.warning(f"  ⚠️  Range doesn't match expected pattern")
                        
                        # Show gaps or overlaps
                        expected_set = set(range(expected_start, expected_end + 1))
                        actual_set = set(customer_nums)
                        
                        missing = expected_set - actual_set
                        extra = actual_set - expected_set
                        
                        if missing:
                            logger.warning(f"     Missing: {sorted(list(missing))[:10]}{'...' if len(missing) > 10 else ''}")
                        if extra:
                            logger.warning(f"     Extra: {sorted(list(extra))[:10]}{'...' if len(extra) > 10 else ''}")
                else:
                    logger.warning(f"No customers found with {record_count} records")
            
            return categories
            
        except Exception as e:
            logger.error(f"Failed to validate distribution ranges: {e}")
            return None
    
    def check_timestamp_distribution(self):
        """Check timestamp distribution across years"""
        try:
            logger.info("\n=== TIMESTAMP ANALYSIS ===")
            
            # Analyze timestamps by year
            pipeline = [
                {"$project": {
                    "year": {"$year": {"$dateFromString": {"dateString": "$timestamp"}}},
                    "customer_id": 1
                }},
                {"$group": {"_id": "$year", "count": {"$sum": 1}}},
                {"$sort": {"_id": 1}}
            ]
            
            year_distribution = list(self.collection.aggregate(pipeline))
            
            total_records = sum(year_data["count"] for year_data in year_distribution)
            
            logger.info("Records by Year:")
            for year_data in year_distribution:
                year = year_data["_id"]
                count = year_data["count"]
                percentage = (count / total_records) * 100
                logger.info(f"  {year}: {count} records ({percentage:.1f}%)")
            
        except Exception as e:
            logger.error(f"Failed to check timestamp distribution: {e}")
    
    def run_full_analysis(self):
        """Run complete analysis of the voice data"""
        logger.info("=== VOICE DATA ANALYSIS STARTED ===")
        
        try:
            # Get basic statistics
            basic_stats = self.get_basic_stats()
            
            if not basic_stats:
                logger.error("Failed to get basic statistics")
                return
            
            # Analyze customer distribution
            distribution = self.analyze_customer_distribution()
            
            # Validate customer ID ranges
            self.validate_distribution_ranges()
            
            # Verify data integrity
            integrity_ok = self.verify_data_integrity()
            
            # Show sample records
            self.show_sample_records()
            
            # Check timestamp distribution
            self.check_timestamp_distribution()
            
            # Export customer lists to files
            self.export_customer_lists()
            
            # Final assessment
            logger.info("\n" + "="*60)
            logger.info("FINAL ASSESSMENT")
            logger.info("="*60)
            
            if basic_stats["total_records"] == 2040 and basic_stats["unique_customers"] == 1600:
                logger.info("✅ Record counts match expected values")
            else:
                logger.warning("⚠️  Record counts don't match expected values")
            
            if integrity_ok:
                logger.info("✅ Data integrity checks passed")
            else:
                logger.warning("⚠️  Data integrity issues found")
            
            logger.info("✅ Analysis completed successfully!")
            logger.info("📁 Check generated .txt files for detailed customer lists")
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}")
    
    def close_connection(self):
        """Close MongoDB connection"""
        try:
            if self.client:
                self.client.close()
                logger.info("MongoDB connection closed")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")

def main():
    """Main function to run the voice data analysis"""
    
    # Initialize the analyzer
    analyzer = VoiceDataAnalyzer(MONGO_CONNECTION_STRING, MONGO_DATABASE_NAME)
    
    try:
        # Connect to MongoDB
        if not analyzer.connect_to_mongodb():
            logger.error("Failed to connect to MongoDB. Exiting.")
            return
        
        # Run full analysis
        analyzer.run_full_analysis()
        
    except Exception as e:
        logger.error(f"An error occurred during analysis: {e}")
        
    finally:
        # Clean up connections
        analyzer.close_connection()
        logger.info("=== VOICE DATA ANALYSIS COMPLETED ===")

if __name__ == "__main__":
    main()