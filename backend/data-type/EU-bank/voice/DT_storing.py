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
COLLECTION_NAME = "voice"  # Changed from emailmessages to twitters

# Maximum number of combinations to generate
MAX_COMBINATIONS = 2000

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
    
    def get_document_count(self):
        """Get total number of documents in the collection"""
        try:
            count = self.collection.count_documents({})
            logger.info(f"Total documents in collection: {count}")
            return count
        except Exception as e:
            logger.error(f"Failed to get document count: {e}")
            return 0
    
    def add_topic_fields(self, topic_data):
        """This method is no longer needed since we create documents with topic fields directly"""
        logger.info("Topic fields are added during document creation")
        return True
    
    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

class CSVProcessor:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path
        self.df = None
    
    def load_csv(self):
        """Load CSV file"""
        try:
            self.df = pd.read_csv(self.csv_file_path)
            # Clean the DataFrame - remove rows where Dominant_topic is NaN
            initial_count = len(self.df)
            self.df = self.df.dropna(subset=['Dominant_topic'])
            final_count = len(self.df)
            
            if initial_count != final_count:
                logger.info(f"Removed {initial_count - final_count} rows with missing Dominant_topic")
            
            logger.info(f"Successfully loaded CSV with {final_count} valid records")
            return True
        except Exception as e:
            logger.error(f"Failed to load CSV: {e}")
            return False
    
    def generate_topic_combinations(self, max_combinations=MAX_COMBINATIONS):
        """Generate topic combinations limited to max_combinations"""
        all_combinations = []
        
        for _, row in self.df.iterrows():
            dominant_topic = row['Dominant_topic']
            subtopics_str = row['Subtopics']
            
            # Skip rows with missing or invalid subtopics
            if pd.isna(subtopics_str) or not isinstance(subtopics_str, str):
                logger.warning(f"Skipping row with invalid subtopics: {dominant_topic}")
                continue
            
            # Split subtopics by comma and clean them
            subtopics_list = [topic.strip() for topic in subtopics_str.split(',') if topic.strip()]
            
            # Skip if no valid subtopics found
            if not subtopics_list:
                logger.warning(f"No valid subtopics found for: {dominant_topic}")
                continue
            
            # Generate all possible combinations (1 to n subtopics)
            for r in range(1, len(subtopics_list) + 1):
                for combo in combinations(subtopics_list, r):
                    all_combinations.append({
                        'dominant_topic': dominant_topic,
                        'subtopics': ', '.join(combo)
                    })
        
        logger.info(f"Generated {len(all_combinations)} total combinations")
        
        # If we have more combinations than needed, randomly sample
        if len(all_combinations) > max_combinations:
            selected_combinations = random.sample(all_combinations, max_combinations)
            logger.info(f"Randomly selected {max_combinations} combinations from {len(all_combinations)} total")
        else:
            selected_combinations = all_combinations
            logger.info(f"Using all {len(selected_combinations)} combinations (less than maximum)")
        
        return selected_combinations

def update_existing_chat_records(mongo_processor, topic_data):
    """Update existing chat records by adding dominant_topic and subtopics fields"""
    try:
        # Get all existing chat records
        existing_chats = list(mongo_processor.collection.find({}))
        total_existing = len(existing_chats)
        
        if total_existing == 0:
            logger.error("No existing chat records found to update!")
            return False
        
        logger.info(f"Found {total_existing} existing chat records to update")
        
        # Limit topic_data to match existing records
        if len(topic_data) > total_existing:
            topic_data = topic_data[:total_existing]
            logger.info(f"Limited topic combinations to {len(topic_data)} to match existing records")
        elif len(topic_data) < total_existing:
            logger.warning(f"Only {len(topic_data)} topic combinations available for {total_existing} existing records")
            # Repeat topic combinations if needed
            while len(topic_data) < total_existing:
                topic_data.extend(topic_data[:min(len(topic_data), total_existing - len(topic_data))])
            logger.info(f"Extended topic combinations to {len(topic_data)}")
        
        # Update existing records with topic fields
        updated_count = 0
        batch_size = 100
        
        for i in range(0, total_existing, batch_size):
            end_idx = min(i + batch_size, total_existing)
            
            # Update records one by one in this batch
            for j in range(i, end_idx):
                chat_record = existing_chats[j]
                topic_combo = topic_data[j]
                
                try:
                    result = mongo_processor.collection.update_one(
                        {"_id": chat_record["_id"]},
                        {
                            "$set": {
                                "dominant_topic": topic_combo["dominant_topic"],
                                "subtopics": topic_combo["subtopics"]
                            }
                        }
                    )
                    
                    if result.modified_count > 0:
                        updated_count += 1
                    
                except Exception as e:
                    logger.error(f"Failed to update record {j+1}: {e}")
                    continue
            
            logger.info(f"Processed batch {i//batch_size + 1}: {end_idx - i} records")
        
        logger.info(f"Successfully updated {updated_count} existing chat records with topic fields")
        return True
        
    except Exception as e:
        logger.error(f"Failed to update existing chat records: {e}")
        return False

def main():
    """Main execution function"""
    logger.info("Starting twitters collection update with topic data...")
    
    # Initialize processors
    mongo_processor = MongoDBProcessor()
    csv_processor = CSVProcessor("voice.csv")  # Update path as needed
    
    try:
        # Connect to MongoDB
        if not mongo_processor.connect():
            return False
        
        # Get initial document count
        document_count = mongo_processor.get_document_count()
        
        if document_count == 0:
            logger.error("No existing chat records found! Please run data_creation.py first to create chat records.")
            return False
        
        logger.info(f"Found {document_count} existing chat records to update")
        
        # Load and process CSV
        logger.info("Loading CSV file...")
        if not csv_processor.load_csv():
            return False
        
        # Generate topic combinations (limited to 2000)
        logger.info(f"Generating up to {MAX_COMBINATIONS} topic-subtopic combinations...")
        topic_data = csv_processor.generate_topic_combinations(MAX_COMBINATIONS)
        
        if not topic_data:
            logger.error("No topic data generated from CSV")
            return False
        
        logger.info(f"Will update {document_count} existing chat records with topic combinations")
        
        # Update existing chat records with topic fields
        logger.info("Updating existing chat records with dominant_topic and subtopics fields...")
        if not update_existing_chat_records(mongo_processor, topic_data):
            return False
        
        # Verify final count and topic fields
        final_count = mongo_processor.get_document_count()
        records_with_topics = mongo_processor.collection.count_documents({"dominant_topic": {"$exists": True}})
        
        logger.info(f"Final document count: {final_count}")
        logger.info(f"Records with topic fields: {records_with_topics}")
        
        if records_with_topics == final_count:
            logger.info("✅ All existing chat records successfully updated with topic fields!")
        else:
            logger.warning(f"⚠️ Only {records_with_topics}/{final_count} records have topic fields")
        
        logger.info("Process completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Clean up connections
        mongo_processor.close_connection()

def preview_csv_combinations(csv_file_path, sample_size=10, max_combinations=MAX_COMBINATIONS):
    """Preview function to see what combinations will be generated"""
    csv_processor = CSVProcessor(csv_file_path)
    if csv_processor.load_csv():
        # Show CSV structure first
        print("\nCSV Structure:")
        print("-" * 40)
        print("Columns:", list(csv_processor.df.columns))
        print("Shape:", csv_processor.df.shape)
        print("\nFirst few rows:")
        print(csv_processor.df[['Dominant_topic', 'Subtopics']].head())
        
        # Check for missing values
        print(f"\nMissing values in Dominant_topic: {csv_processor.df['Dominant_topic'].isna().sum()}")
        print(f"Missing values in Subtopics: {csv_processor.df['Subtopics'].isna().sum()}")
        
        topic_data = csv_processor.generate_topic_combinations(max_combinations)
        
        if topic_data:
            print(f"\nTotal combinations generated: {len(topic_data)} (max: {max_combinations})")
            print(f"\nPreview of {min(sample_size, len(topic_data))} combinations:")
            print("-" * 80)
            for i, combo in enumerate(random.sample(topic_data, min(sample_size, len(topic_data)))):
                print(f"{i+1}. Dominant Topic: {combo['dominant_topic']}")
                print(f"   Subtopics: {combo['subtopics']}")
                print()
        else:
            print("No valid combinations generated!")

def check_collection_status():
    """Check the current status of the twitters collection"""
    mongo_processor = MongoDBProcessor()
    try:
        if mongo_processor.connect():
            count = mongo_processor.get_document_count()
            
            if count > 0:
                # Check if any documents already have topic fields
                sample_doc = mongo_processor.collection.find_one({})
                has_topic_fields = 'dominant_topic' in sample_doc if sample_doc else False
                
                print(f"\nCollection Status:")
                print(f"- Documents in twitters: {count}")
                print(f"- Topic fields present: {'Yes' if has_topic_fields else 'No'}")
                
                if has_topic_fields:
                    # Count documents with topic fields
                    with_topics = mongo_processor.collection.count_documents({"dominant_topic": {"$exists": True}})
                    print(f"- Documents with topic fields: {with_topics}")
                    
                    if with_topics == count:
                        print("✅ All records have topic fields - no update needed!")
                    else:
                        print(f"⚠️ {count - with_topics} records still need topic fields")
                else:
                    print("📝 Ready to add topic fields to existing chat records")
            else:
                print("\nCollection is empty. You need to run data_creation.py first to create chat records.")
                
    finally:
        mongo_processor.close_connection()

if __name__ == "__main__":
    print("twitters Topic Setup Tool")
    print("=" * 40)
    
    # Check collection status first
    check_collection_status()
    
    print("\nOptions:")
    print("1. Preview combinations from CSV")
    print("2. Run the setup process")
    print("3. Exit")
    
    choice = input("\nEnter your choice (1-3): ").strip()
    
    if choice == "1":
        # Preview combinations
        preview_csv_combinations("voice.csv", 15, MAX_COMBINATIONS)
    elif choice == "2":
        # Run the main process
        success = main()
        if success:
            print("✅ All existing chat records updated with topic fields successfully!")
        else:
            print("❌ Process failed. Check the logs for details.")
    else:
        print("Exiting...")