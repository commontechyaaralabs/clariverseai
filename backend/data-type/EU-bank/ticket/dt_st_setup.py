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
COLLECTION_NAME = "tickets"

# Fields to remove from the collection
FIELDS_TO_REMOVE = [
    "subject", "message_text", "time_taken", "domain", "cleaned_text",
    "lemmatized_text", "preprocessed_text", "dominant_topic", "model_used",
    "processed_at", "subtopics", "urgency", "was_summarized", "embeddings",
    "clustering_method", "clustering_updated_at", "kmeans_cluster_id",
    "kmeans_cluster_keyphrase", "dominant_cluster_label", "subcluster_label",
    "subcluster_id", "title", "description", "priority", "created"
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
    
    def remove_fields(self):
        """Remove specified fields from all documents in the collection"""
        try:
            # Create unset operation for all fields to remove
            unset_fields = {field: "" for field in FIELDS_TO_REMOVE}
            
            # Update all documents to remove the fields
            result = self.collection.update_many(
                {},  # Empty filter to match all documents
                {"$unset": unset_fields}
            )
            
            logger.info(f"Successfully removed fields from {result.modified_count} documents")
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
    
    def add_topic_fields(self, topic_data):
        """Add dominant_topic and subtopics fields to documents"""
        try:
            documents = list(self.collection.find({}))
            updated_count = 0
            
            for doc in documents:
                # Randomly select a topic-subtopic combination
                selected_combo = random.choice(topic_data)
                
                # Update the document
                result = self.collection.update_one(
                    {"_id": doc["_id"]},
                    {
                        "$set": {
                            "dominant_topic": selected_combo["dominant_topic"],
                            "subtopics": selected_combo["subtopics"]
                        }
                    }
                )
                
                if result.modified_count > 0:
                    updated_count += 1
            
            logger.info(f"Successfully added topic fields to {updated_count} documents")
            return True
        except Exception as e:
            logger.error(f"Failed to add topic fields: {e}")
            return False
    
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
    
    def generate_topic_combinations(self):
        """Generate all possible subtopic combinations with their dominant topics"""
        topic_data = []
        
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
                    topic_data.append({
                        'dominant_topic': dominant_topic,
                        'subtopics': ', '.join(combo)
                    })
        
        logger.info(f"Generated {len(topic_data)} topic-subtopic combinations")
        return topic_data

def main():
    """Main execution function"""
    logger.info("Starting MongoDB field cleanup and CSV processing...")
    
    # Initialize processors
    mongo_processor = MongoDBProcessor()
    csv_processor = CSVProcessor("ticket.csv")  # Update path as needed
    
    try:
        # Connect to MongoDB
        if not mongo_processor.connect():
            return False
        
        # Get initial document count
        initial_count = mongo_processor.get_document_count()
        if initial_count == 0:
            logger.warning("No documents found in the collection")
            return False
        
        # Remove specified fields
        logger.info("Removing specified fields from documents...")
        if not mongo_processor.remove_fields():
            return False
        
        # Load and process CSV
        logger.info("Loading CSV file...")
        if not csv_processor.load_csv():
            return False
        
        # Generate topic combinations
        logger.info("Generating topic-subtopic combinations...")
        topic_data = csv_processor.generate_topic_combinations()
        
        if not topic_data:
            logger.error("No topic data generated from CSV")
            return False
        
        # Add new topic fields to documents
        logger.info("Adding new topic fields to documents...")
        if not mongo_processor.add_topic_fields(topic_data):
            return False
        
        logger.info("Process completed successfully!")
        return True
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False
    
    finally:
        # Clean up connections
        mongo_processor.close_connection()

def preview_csv_combinations(csv_file_path, sample_size=10):
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
        
        topic_data = csv_processor.generate_topic_combinations()
        
        if topic_data:
            print(f"\nPreview of {min(sample_size, len(topic_data))} combinations:")
            print("-" * 80)
            for i, combo in enumerate(random.sample(topic_data, min(sample_size, len(topic_data)))):
                print(f"{i+1}. Dominant Topic: {combo['dominant_topic']}")
                print(f"   Subtopics: {combo['subtopics']}")
                print()
        else:
            print("No valid combinations generated!")

if __name__ == "__main__":
    # Uncomment the line below to preview combinations before running the main process
    # preview_csv_combinations("ticket.csv", 15)
    
    # Run the main process
    success = main()
    if success:
        print("✅ All operations completed successfully!")
    else:
        print("❌ Process failed. Check the logs for details.")