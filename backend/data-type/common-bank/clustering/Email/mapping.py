import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def connect_to_mongodb():
    """Connect to MongoDB using environment variables"""
    try:
        connection_string = os.getenv('MONGO_CONNECTION_STRING')
        database_name = os.getenv('MONGO_DATABASE_NAME')
        
        if not connection_string or not database_name:
            raise ValueError("Missing MongoDB connection details in environment variables")
        
        client = MongoClient(connection_string)
        db = client[database_name]
        
        # Test connection
        client.admin.command('ping')
        logger.info(f"Successfully connected to MongoDB database: {database_name}")
        
        return db
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def debug_cluster_structure(db):
    """Debug function to examine the actual structure of cluster documents"""
    try:
        cluster_collection = db['cluster']
        sample_cluster = cluster_collection.find_one({})
        
        if sample_cluster:
            logger.info("Sample cluster structure:")
            logger.info(f"cluster_id: {sample_cluster.get('cluster_id')}")
            logger.info(f"dominant_label: {sample_cluster.get('dominant_label')}")
            
            subclusters = sample_cluster.get('subclusters', {})
            logger.info(f"subclusters type: {type(subclusters)}")
            logger.info(f"subclusters keys: {list(subclusters.keys()) if isinstance(subclusters, dict) else 'Not a dict'}")
            
            if isinstance(subclusters, dict):
                for key, subcluster in list(subclusters.items())[:2]:  # Show first 2 subclusters
                    logger.info(f"Subcluster {key}:")
                    logger.info(f"  label: {subcluster.get('label') if isinstance(subcluster, dict) else 'N/A'}")
                    logger.info(f"  keyphrases: {subcluster.get('keyphrases') if isinstance(subcluster, dict) else 'N/A'}")
        
    except Exception as e:
        logger.error(f"Error debugging cluster structure: {str(e)}")

def get_cluster_data(db):
    """Fetch all cluster data and create lookup dictionaries"""
    try:
        cluster_collection = db['cluster']
        clusters = list(cluster_collection.find({}))
        
        logger.info(f"Found {len(clusters)} clusters")
        
        # Create lookup dictionaries
        cluster_dominant_labels = {}  # cluster_id -> dominant_label
        keyphrase_to_label = {}       # keyphrase -> label
        keyphrase_to_subcluster_id = {}  # keyphrase -> subcluster_id
        
        for cluster in clusters:
            cluster_id = cluster.get('cluster_id')
            dominant_label = cluster.get('dominant_label')
            subclusters = cluster.get('subclusters', [])
            
            if cluster_id is not None and dominant_label is not None:
                cluster_dominant_labels[cluster_id] = dominant_label
            
            # Process subclusters to create keyphrase -> label mapping
            # subclusters is an object with numbered keys (0, 1, 2, etc.)
            if isinstance(subclusters, dict):
                for subcluster_id, subcluster in subclusters.items():
                    if isinstance(subcluster, dict):
                        label = subcluster.get('label')
                        keyphrases = subcluster.get('keyphrases', [])
                        
                        if label and isinstance(keyphrases, list):
                            for keyphrase in keyphrases:
                                if isinstance(keyphrase, str):
                                    keyphrase_to_label[keyphrase] = label
                                    keyphrase_to_subcluster_id[keyphrase] = subcluster_id
                                    logger.debug(f"Mapped keyphrase '{keyphrase}' to label '{label}' and subcluster_id '{subcluster_id}'")
            elif isinstance(subclusters, list):
                # Handle case where subclusters might be an array
                for idx, subcluster in enumerate(subclusters):
                    if isinstance(subcluster, dict):
                        label = subcluster.get('label')
                        keyphrases = subcluster.get('keyphrases', [])
                        
                        if label and isinstance(keyphrases, list):
                            for keyphrase in keyphrases:
                                if isinstance(keyphrase, str):
                                    keyphrase_to_label[keyphrase] = label
                                    keyphrase_to_subcluster_id[keyphrase] = str(idx)
                                    logger.debug(f"Mapped keyphrase '{keyphrase}' to label '{label}' and subcluster_id '{idx}'")
        
        logger.info(f"Created lookup for {len(cluster_dominant_labels)} cluster dominant labels")
        logger.info(f"Created lookup for {len(keyphrase_to_label)} keyphrases")
        logger.info(f"Created lookup for {len(keyphrase_to_subcluster_id)} keyphrase-to-subcluster-id mappings")
        
        return cluster_dominant_labels, keyphrase_to_label, keyphrase_to_subcluster_id
        
    except Exception as e:
        logger.error(f"Error fetching cluster data: {str(e)}")
        raise

def update_emailmessages(db, cluster_dominant_labels, keyphrase_to_label, keyphrase_to_subcluster_id):
    """Update emailmessages collection with dominant_cluster_label, subcluster labels, and subcluster_id"""
    try:
        emailmessages_collection = db['emailmessages']
        
        # Get all email messages
        messages = list(emailmessages_collection.find({}))
        logger.info(f"Found {len(messages)} email messages to process")
        
        updated_count = 0
        dominant_label_updates = 0
        subcluster_label_updates = 0
        subcluster_id_updates = 0
        
        for message in messages:
            message_id = message.get('_id')
            updates = {}
            
            # Update dominant_cluster_label based on kmeans_cluster_id
            kmeans_cluster_id = message.get('kmeans_cluster_id')
            if kmeans_cluster_id is not None and kmeans_cluster_id in cluster_dominant_labels:
                dominant_label = cluster_dominant_labels[kmeans_cluster_id]
                updates['dominant_cluster_label'] = dominant_label
                dominant_label_updates += 1
            
            # Update subcluster label and subcluster_id based on kmeans_cluster_keyphrase
            kmeans_cluster_keyphrase = message.get('kmeans_cluster_keyphrase')
            if kmeans_cluster_keyphrase and kmeans_cluster_keyphrase in keyphrase_to_label:
                subcluster_label = keyphrase_to_label[kmeans_cluster_keyphrase]
                updates['subcluster_label'] = subcluster_label
                subcluster_label_updates += 1
                
                # Also add subcluster_id
                if kmeans_cluster_keyphrase in keyphrase_to_subcluster_id:
                    subcluster_id = keyphrase_to_subcluster_id[kmeans_cluster_keyphrase]
                    updates['subcluster_id'] = subcluster_id
                    subcluster_id_updates += 1
            
            # Perform update if there are changes
            if updates:
                result = emailmessages_collection.update_one(
                    {'_id': message_id},
                    {'$set': updates}
                )
                
                if result.modified_count > 0:
                    updated_count += 1
                    logger.debug(f"Updated message {message_id} with: {updates}")
        
        logger.info(f"Update completed:")
        logger.info(f"  - Total messages updated: {updated_count}")
        logger.info(f"  - Dominant cluster labels added: {dominant_label_updates}")
        logger.info(f"  - Subcluster labels added: {subcluster_label_updates}")
        logger.info(f"  - Subcluster IDs added: {subcluster_id_updates}")
        
        return updated_count, dominant_label_updates, subcluster_label_updates, subcluster_id_updates
        
    except Exception as e:
        logger.error(f"Error updating emailmessages: {str(e)}")
        raise

def main():
    """Main function to orchestrate the update process"""
    try:
        logger.info("Starting cluster label update process...")
        
        # Connect to MongoDB
        db = connect_to_mongodb()
        
        # Debug cluster structure
        debug_cluster_structure(db)
        
        # Get cluster data and create lookup dictionaries
        cluster_dominant_labels, keyphrase_to_label, keyphrase_to_subcluster_id = get_cluster_data(db)
        
        # Update emailmessages collection
        updated_count, dominant_updates, subcluster_updates, subcluster_id_updates = update_emailmessages(
            db, cluster_dominant_labels, keyphrase_to_label, keyphrase_to_subcluster_id
        )
        
        logger.info("Process completed successfully!")
        logger.info(f"Summary:")
        logger.info(f"  - Documents updated: {updated_count}")
        logger.info(f"  - Dominant cluster labels: {dominant_updates}")
        logger.info(f"  - Subcluster labels: {subcluster_updates}")
        logger.info(f"  - Subcluster IDs: {subcluster_id_updates}")
        
    except Exception as e:
        logger.error(f"Process failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()