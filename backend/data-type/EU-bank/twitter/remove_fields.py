import pymongo
import pandas as pd
import random
from itertools import combinations
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin"
MONGO_DATABASE_NAME = "sparzaai"
COLLECTION_NAME = "twitter"  # Changed to twitter

# Fields to remove from the collection (Twitter specific)
FIELDS_TO_REMOVE = [
    "hashtags",
    "like_count", 
    "priority",
    "quote_count",
    "reply_count",
    "retweet_count",
    "sentiment",
    "text",
    "urgency"
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
    
    # Run the main process
    success = main()
    if success:
        print("✅ Twitter collection field removal completed successfully!")
    else:
        print("❌ Process failed. Check the logs for details.")