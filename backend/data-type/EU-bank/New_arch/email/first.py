# Import required libraries
from pymongo import MongoClient
from collections import defaultdict
import re
import os
from dotenv import load_dotenv
from bson import ObjectId

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def connect_to_mongodb():
    """Connect to MongoDB and return database instance"""
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        print("Successfully connected to MongoDB")
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None

def get_cluster_subclusters_map(db):
    """
    Create a mapping of cluster_id to its subclusters data
    Only for clusters with data: "email"
    """
    cluster_collection = db['cluster']
    
    # Find clusters with data: "email"
    clusters = cluster_collection.find({"data": "email"})
    
    cluster_map = {}
    for cluster in clusters:
        cluster_id = cluster.get('cluster_id')
        subclusters = cluster.get('subclusters', {})
        
        if cluster_id is not None and subclusters:
            cluster_map[cluster_id] = {
                'subclusters': subclusters,
                'dominant_label': cluster.get('dominant_label', '')
            }
    
    print(f"Found {len(cluster_map)} clusters with email data")
    return cluster_map

def match_dominant_topic_to_subcluster(dominant_topic, subclusters):
    """
    Match dominant_topic with keyphrases in subclusters
    Returns the matching subcluster object id and label
    """
    if not dominant_topic or not subclusters:
        return None, None
    
    # Normalize the dominant topic for comparison
    dominant_topic_lower = dominant_topic.lower().strip()
    
    # Iterate through subclusters to find matching keyphrases
    for subcluster_id, subcluster_data in subclusters.items():
        if isinstance(subcluster_data, dict):
            keyphrases = subcluster_data.get('keyphrases', [])
            label = subcluster_data.get('label', '')
            
            # Check if dominant_topic matches any keyphrase
            for keyphrase in keyphrases:
                if isinstance(keyphrase, str):
                    keyphrase_lower = keyphrase.lower().strip()
                    
                    # Exact match or partial match
                    if (dominant_topic_lower == keyphrase_lower or 
                        dominant_topic_lower in keyphrase_lower or 
                        keyphrase_lower in dominant_topic_lower):
                        
                        return subcluster_id, label
    
    return None, None

def update_email_subclusters(db):
    """
    Main function to update subcluster_id and subcluster_label in email collection
    """
    email_collection = db['email']
    
    # Get cluster subclusters mapping
    cluster_map = get_cluster_subclusters_map(db)
    
    if not cluster_map:
        print("No clusters found with email data")
        return
    
    # Find email records with subcluster_id as null
    email_query = {
        "subcluster_id": {"$in": [None, ""]},  # Handle both null and empty string
        "kmeans_cluster_id": {"$exists": True, "$ne": None},
        "dominant_topic": {"$exists": True, "$ne": None}
    }
    
    emails_to_update = list(email_collection.find(email_query))
    print(f"Found {len(emails_to_update)} email records to process")
    
    updated_count = 0
    not_matched_count = 0
    
    for email in emails_to_update:
        email_id = email.get('_id')
        kmeans_cluster_id = email.get('kmeans_cluster_id')
        dominant_topic = email.get('dominant_topic')
        
        print(f"\nProcessing email ID: {email_id}")
        print(f"K-means cluster ID: {kmeans_cluster_id}")
        print(f"Dominant topic: {dominant_topic}")
        
        # Check if we have cluster data for this kmeans_cluster_id
        if kmeans_cluster_id not in cluster_map:
            print(f"No cluster data found for kmeans_cluster_id: {kmeans_cluster_id}")
            not_matched_count += 1
            continue
        
        cluster_data = cluster_map[kmeans_cluster_id]
        subclusters = cluster_data['subclusters']
        
        # Match dominant_topic with subcluster keyphrases
        subcluster_id, subcluster_label = match_dominant_topic_to_subcluster(
            dominant_topic, subclusters
        )
        
        if subcluster_id and subcluster_label:
            # Update the email record
            update_data = {
                "subcluster_id": subcluster_id,
                "subcluster_label": subcluster_label
            }
            
            result = email_collection.update_one(
                {"_id": email_id},
                {"$set": update_data}
            )
            
            if result.modified_count > 0:
                print(f"✓ Updated email {email_id} with subcluster_id: {subcluster_id}, label: {subcluster_label}")
                updated_count += 1
            else:
                print(f"✗ Failed to update email {email_id}")
        else:
            print(f"✗ No matching subcluster found for dominant_topic: {dominant_topic}")
            not_matched_count += 1
    
    print(f"\n=== SUMMARY ===")
    print(f"Total emails processed: {len(emails_to_update)}")
    print(f"Successfully updated: {updated_count}")
    print(f"Not matched/updated: {not_matched_count}")

def verify_updates(db):
    """
    Verify the updates by checking how many records now have subcluster_id
    """
    email_collection = db['email']
    
    # Count records with null subcluster_id
    null_subcluster_count = email_collection.count_documents({
        "subcluster_id": {"$in": [None, ""]}
    })
    
    # Count records with non-null subcluster_id
    non_null_subcluster_count = email_collection.count_documents({
        "subcluster_id": {"$exists": True, "$ne": None, "$ne": ""}
    })
    
    print(f"\n=== VERIFICATION ===")
    print(f"Records with null subcluster_id: {null_subcluster_count}")
    print(f"Records with assigned subcluster_id: {non_null_subcluster_count}")

def main():
    """Main execution function"""
    print("Starting email subcluster assignment process...")
    
    # Connect to MongoDB
    db = connect_to_mongodb()
    if db is None:
        print("Failed to connect to database. Exiting.")
        return
    
    try:
        # Update email subclusters
        update_email_subclusters(db)
        
        # Verify updates
        verify_updates(db)
        
        print("\nProcess completed successfully!")
        
    except Exception as e:
        print(f"Error during processing: {e}")
    
    finally:
        # Close database connection
        if db is not None:
            db.client.close()
            print("Database connection closed.")

if __name__ == "__main__":
    main()