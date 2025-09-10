import pymongo
import pandas as pd
import random
from itertools import combinations
import logging
import json

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin"
MONGO_DATABASE_NAME = "sparzaai"
COLLECTION_NAME = "voice"  # Changed to twitter

# Fields to remove from the collection (Twitter specific)
FIELDS_TO_REMOVE = [
    "dominant_topic",
    "subtopics"
]

class MongoDBProcessor:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None
    
    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
            self.db = self.client[MONGO_DATABASE_NAME]
            self.collection = self.db[COLLECTION_NAME]
            # Test connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False
    
    def check_fields_exist(self):
        """Check which fields exist in the collection"""
        try:
            # Get a sample document to check field existence
            sample_doc = self.collection.find_one()
            if not sample_doc:
                logger.warning("No documents found in the collection")
                return []
            
            existing_fields = []
            for field in FIELDS_TO_REMOVE:
                if field in sample_doc:
                    existing_fields.append(field)
            
            logger.info(f"Fields found in collection: {existing_fields}")
            logger.info(f"Fields not found: {set(FIELDS_TO_REMOVE) - set(existing_fields)}")
            return existing_fields
        except Exception as e:
            logger.error(f"Failed to check field existence: {e}")
            return []
    
    def find_records_without_fields(self, fields_to_check=None, limit=None, print_records=True):
        """Find and print records that don't have the specified fields"""
        try:
            if fields_to_check is None:
                fields_to_check = FIELDS_TO_REMOVE
            
            # Create query to find documents that don't have ANY of the specified fields
            # Using $and with $exists: false for each field
            query = {
                "$and": [
                    {field: {"$exists": False}} for field in fields_to_check
                ]
            }
            
            # Alternative query if you want documents that don't have ALL fields
            # query = {
            #     "$or": [
            #         {field: {"$exists": False}} for field in fields_to_check
            #     ]
            # }
            
            # Find documents matching the query
            cursor = self.collection.find(query)
            if limit:
                cursor = cursor.limit(limit)
            
            records = list(cursor)
            
            logger.info(f"Found {len(records)} records without fields: {fields_to_check}")
            
            if print_records and records:
                print("\n" + "="*80)
                print(f"RECORDS WITHOUT FIELDS: {fields_to_check}")
                print("="*80)
                
                for i, record in enumerate(records, 1):
                    print(f"\n--- Record {i} ---")
                    print(f"ID: {record.get('_id', 'No ID')}")
                    
                    # Print all fields in the record
                    for key, value in record.items():
                        if key != '_id':  # Skip ObjectId for cleaner output
                            # Truncate long values for better readability
                            if isinstance(value, str) and len(value) > 100:
                                display_value = value[:100] + "..."
                            else:
                                display_value = value
                            print(f"{key}: {display_value}")
                    
                    print("-" * 40)
            
            elif not records:
                print(f"\n✅ All records have the specified fields: {fields_to_check}")
            
            return records
            
        except Exception as e:
            logger.error(f"Failed to find records without fields: {e}")
            return []
    
    def delete_records_without_fields(self, fields_to_check=None, confirm=True):
        """Delete records that don't have the specified fields"""
        try:
            if fields_to_check is None:
                fields_to_check = FIELDS_TO_REMOVE
            
            # Create query to find documents that don't have ANY of the specified fields
            query = {
                "$and": [
                    {field: {"$exists": False}} for field in fields_to_check
                ]
            }
            
            # First, count how many records would be deleted
            count_to_delete = self.collection.count_documents(query)
            
            if count_to_delete == 0:
                logger.info("No records found to delete - all records have the specified fields")
                print("✅ No records to delete - all records have the required fields!")
                return True
            
            logger.info(f"Found {count_to_delete} records to delete (missing fields: {fields_to_check})")
            
            if confirm:
                print(f"\n⚠️  WARNING: About to DELETE {count_to_delete} records!")
                print(f"These records are missing the fields: {fields_to_check}")
                
                # Show a few sample records that will be deleted
                sample_records = list(self.collection.find(query).limit(3))
                if sample_records:
                    print(f"\nSample records that will be DELETED:")
                    print("-" * 50)
                    for i, record in enumerate(sample_records, 1):
                        print(f"Sample {i} - ID: {record.get('_id')}")
                        # Show a few fields to identify the record
                        for key, value in list(record.items())[:3]:
                            if key != '_id':
                                display_value = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                                print(f"  {key}: {display_value}")
                        print()
                
                confirmation = input("Are you sure you want to DELETE these records? (yes/no): ").strip().lower()
                if confirmation not in ['yes', 'y']:
                    print("❌ Operation cancelled by user")
                    return False
            
            # Perform the deletion
            print(f"\n🗑️  Deleting {count_to_delete} records...")
            result = self.collection.delete_many(query)
            
            if result.deleted_count > 0:
                logger.info(f"Successfully deleted {result.deleted_count} records")
                print(f"✅ Successfully deleted {result.deleted_count} records!")
                
                # Show updated collection stats
                remaining_count = self.collection.count_documents({})
                print(f"📊 Collection now contains {remaining_count} records")
                
                return True
            else:
                logger.warning("No records were deleted")
                print("⚠️  No records were deleted")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete records without fields: {e}")
            print(f"❌ Error occurred while deleting records: {e}")
            return False
    
    def count_records_without_fields(self, fields_to_check=None):
        """Count records that don't have the specified fields"""
        try:
            if fields_to_check is None:
                fields_to_check = FIELDS_TO_REMOVE
            
            # Query for documents that don't have ANY of the specified fields
            query = {
                "$and": [
                    {field: {"$exists": False}} for field in fields_to_check
                ]
            }
            
            count = self.collection.count_documents(query)
            logger.info(f"Count of records without fields {fields_to_check}: {count}")
            return count
            
        except Exception as e:
            logger.error(f"Failed to count records without fields: {e}")
            return 0
    
    def analyze_field_distribution(self, fields_to_check=None):
        """Analyze the distribution of specified fields across all records"""
        try:
            if fields_to_check is None:
                fields_to_check = FIELDS_TO_REMOVE
            
            total_docs = self.collection.count_documents({})
            
            print(f"\n{'='*60}")
            print(f"FIELD DISTRIBUTION ANALYSIS")
            print(f"{'='*60}")
            print(f"Total documents in collection: {total_docs}")
            
            for field in fields_to_check:
                with_field = self.collection.count_documents({field: {"$exists": True}})
                without_field = total_docs - with_field
                
                print(f"\nField: '{field}'")
                print(f"  Documents WITH field:    {with_field:6d} ({with_field/total_docs*100:.1f}%)")
                print(f"  Documents WITHOUT field: {without_field:6d} ({without_field/total_docs*100:.1f}%)")
            
            # Count documents missing ALL specified fields
            query = {
                "$and": [
                    {field: {"$exists": False}} for field in fields_to_check
                ]
            }
            missing_all = self.collection.count_documents(query)
            
            print(f"\nDocuments missing ALL specified fields: {missing_all} ({missing_all/total_docs*100:.1f}%)")
            
        except Exception as e:
            logger.error(f"Failed to analyze field distribution: {e}")
    
    def remove_fields(self, fields_to_remove=None):
        """Remove specified fields from all documents in the collection"""
        try:
            # Use provided fields or check which fields exist
            if fields_to_remove is None:
                fields_to_remove = self.check_fields_exist()
            
            if not fields_to_remove:
                logger.info("No fields to remove")
                return True
            
            # Create unset operation for existing fields only
            unset_fields = {field: "" for field in fields_to_remove}
            
            # Update all documents to remove the fields
            result = self.collection.update_many(
                {},  # Empty filter to match all documents
                {"$unset": unset_fields}
            )
            
            logger.info(f"Successfully removed fields {fields_to_remove} from {result.modified_count} documents")
            return True
        except Exception as e:
            logger.error(f"Failed to remove fields: {e}")
            return False
    
    def get_document_count(self):
        """Get total number of documents in the collection"""
        try:
            count = self.collection.count_documents({})
            logger.info(f"Total documents in collection: {count}")
            return count
        except Exception as e:
            logger.error(f"Failed to get document count: {e}")
            return 0
    
    def preview_documents(self, limit=3):
        """Preview some documents to see their structure"""
        try:
            docs = list(self.collection.find().limit(limit))
            logger.info(f"Preview of {len(docs)} documents:")
            for i, doc in enumerate(docs, 1):
                logger.info(f"Document {i} fields: {list(doc.keys())}")
            return docs
        except Exception as e:
            logger.error(f"Failed to preview documents: {e}")
            return []
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def delete_records_without_target_fields():
    """Function to delete records without dominant_topic and subtopics fields"""
    logger.info("Deleting records without 'dominant_topic' and 'subtopics' fields...")
    
    mongo_processor = MongoDBProcessor()
    
    try:
        # Connect to MongoDB
        if not mongo_processor.connect():
            return False
        
        # Get initial document count
        initial_count = mongo_processor.get_document_count()
        print(f"📊 Initial collection size: {initial_count} records")
        
        # Analyze field distribution first
        mongo_processor.analyze_field_distribution()
        
        # Count records without the target fields
        count = mongo_processor.count_records_without_fields()
        
        if count > 0:
            print(f"\n🎯 Found {count} records without the specified fields that will be deleted.")
            
            # Show some sample records before deletion (optional)
            print("\n📋 Sample records that will be deleted:")
            sample_records = mongo_processor.find_records_without_fields(limit=3, print_records=True)
            
            # Delete the records
            success = mongo_processor.delete_records_without_fields()
            
            if success:
                # Show final stats
                final_count = mongo_processor.get_document_count()
                deleted_count = initial_count - final_count
                print(f"\n📈 DELETION SUMMARY:")
                print(f"   Initial records: {initial_count}")
                print(f"   Final records:   {final_count}")
                print(f"   Deleted records: {deleted_count}")
                print(f"   Success rate:    {(deleted_count/count)*100:.1f}%" if count > 0 else "   Success rate:    100%")
        else:
            print("✅ All records have the specified fields! No records to delete.")
            
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Clean up connections
        mongo_processor.close_connection()

def find_records_without_target_fields():
    """Function to find and print records without dominant_topic and subtopics fields"""
    logger.info("Finding records without 'dominant_topic' and 'subtopics' fields...")
    
    mongo_processor = MongoDBProcessor()
    
    try:
        # Connect to MongoDB
        if not mongo_processor.connect():
            return False
        
        # Analyze field distribution first
        mongo_processor.analyze_field_distribution()
        
        # Count records without the target fields
        count = mongo_processor.count_records_without_fields()
        
        if count > 0:
            print(f"\nFound {count} records without the specified fields.")
            
            # Ask user if they want to see all records or limit the output
            if count > 10:
                response = input(f"\nFound {count} records. Display all? (y/n) or enter a number to limit: ").strip().lower()
                if response == 'n':
                    return True
                elif response.isdigit():
                    limit = int(response)
                    mongo_processor.find_records_without_fields(limit=limit)
                else:
                    mongo_processor.find_records_without_fields()
            else:
                # Show all records if count is manageable
                mongo_processor.find_records_without_fields()
        else:
            print("✅ All records have the specified fields!")
            
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Clean up connections
        mongo_processor.close_connection()

def main():
    """Main execution function"""
    logger.info("Starting Twitter collection field cleanup...")
    
    # Initialize processor
    mongo_processor = MongoDBProcessor()
    
    try:
        # Connect to MongoDB
        if not mongo_processor.connect():
            return False
        
        # Get initial document count
        initial_count = mongo_processor.get_document_count()
        if initial_count == 0:
            logger.warning("No documents found in the collection")
            return False
        
        # Preview document structure
        logger.info("Previewing document structure...")
        mongo_processor.preview_documents(3)
        
        # Check which fields exist and remove them
        logger.info("Checking which fields exist and removing them...")
        if not mongo_processor.remove_fields():
            return False
        
        logger.info("Field removal completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Clean up connections
        mongo_processor.close_connection()

def preview_collection_structure():
    """Preview function to see collection structure before making changes"""
    mongo_processor = MongoDBProcessor()
    try:
        if mongo_processor.connect():
            print("\n" + "="*60)
            print("TWITTER COLLECTION STRUCTURE PREVIEW")
            print("="*60)
            
            # Get document count
            count = mongo_processor.get_document_count()
            
            # Preview documents
            docs = mongo_processor.preview_documents(5)
            
            if docs:
                print("\nSample document fields:")
                print("-" * 40)
                all_fields = set()
                for doc in docs:
                    all_fields.update(doc.keys())
                
                print(f"All unique fields found: {sorted(all_fields)}")
                
                print("\nFields that will be removed (if they exist):")
                print("-" * 50)
                for field in FIELDS_TO_REMOVE:
                    exists = any(field in doc for doc in docs)
                    status = "✓ EXISTS" if exists else "✗ NOT FOUND"
                    print(f"{field:<15} - {status}")
                
                print(f"\nSample document structure:")
                print("-" * 30)
                if docs:
                    sample = docs[0]
                    for key, value in sample.items():
                        value_type = type(value).__name__
                        value_preview = str(value)[:50] + "..." if len(str(value)) > 50 else str(value)
                        print(f"{key:<15}: {value_type:<10} = {value_preview}")
            
    except Exception as e:
        print(f"Error during preview: {e}")
    finally:
        mongo_processor.close_connection()

if __name__ == "__main__":
    # Uncomment the line below to preview the collection structure before making changes
    # preview_collection_structure()
    
    # Uncomment the line below to find and print records without the target fields (without deleting)
    # print("🔍 FINDING RECORDS WITHOUT 'dominant_topic' AND 'subtopics' FIELDS")
    # print("="*70)
    # find_records_without_target_fields()
    
    # NEW: Delete records without the target fields
    print("🗑️ DELETING RECORDS WITHOUT 'dominant_topic' AND 'subtopics' FIELDS")
    print("="*70)
    delete_records_without_target_fields()
    
    # Uncomment the line below to run the original removal process
    # success = main()
    # if success:
    #     print("✅ Twitter collection field removal completed successfully!")
    # else:
    #     print("❌ Process failed. Check the logs for details.")