import warnings
import os
from dotenv import load_dotenv
from pymongo import MongoClient

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
COLLECTION_NAME = "emailmessages"

def drop_fields_from_collection():
    """
    Drop specific fields from all documents in the emailmessages collection
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Fields to drop
        fields_to_drop = {
            "clustering_method": 1,
            "kmeans_cluster_id": 1,
            "kmeans_cluster_keyphrase": 1,
            "clustering_updated_at": 1
        }
        
        # Get count of documents before update
        total_docs = collection.count_documents({})
        print(f"Total documents in collection: {total_docs}")
        
        # Drop the specified fields from all documents
        result = collection.update_many(
            {},  # Empty filter to match all documents
            {"$unset": fields_to_drop}
        )
        
        print(f"Modified {result.modified_count} documents")
        print(f"Matched {result.matched_count} documents")
        
        # Verify the fields have been removed by checking a sample document
        sample_doc = collection.find_one({})
        if sample_doc:
            removed_fields = []
            for field in ["clustering_method", "kmeans_cluster_id", "kmeans_cluster_keyphrase", "clustering_updated_at"]:
                if field not in sample_doc:
                    removed_fields.append(field)
            
            print(f"Successfully removed fields: {removed_fields}")
            
            # Show remaining fields in sample document
            print(f"Remaining fields in sample document: {list(sample_doc.keys())}")
        
        # Close the connection
        client.close()
        print("Operation completed successfully!")
        
    except Exception as e:
        print(f"Error occurred: {str(e)}")
        return False
    
    return True

if __name__ == "__main__":
    drop_fields_from_collection()