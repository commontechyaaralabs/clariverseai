import os
import warnings
import spacy
import numpy as np
import pandas as pd
import time
import umap
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from kneed import KneeLocator
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
chat_chunks_COLLECTION_NAME = "chat-chunks"
CLUSTER_COLLECTION_NAME = "cluster"

# Initialize MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
chat_chunks_collection = db[chat_chunks_COLLECTION_NAME]
cluster_collection = db[CLUSTER_COLLECTION_NAME]

# Start timing
start_time = time.time()

print("Connecting to MongoDB...")
print(f"Database: {DB_NAME}")
print(f"chat-chunks Collection: {chat_chunks_COLLECTION_NAME}")
print(f"Cluster Collection: {CLUSTER_COLLECTION_NAME}")

# Check if cluster collection exists and has data
existing_cluster_count = cluster_collection.count_documents({})
print(f"Found {existing_cluster_count} existing documents in cluster collection")

# Load spaCy model with only necessary components for speed
nlp = spacy.load("en_core_web_sm", disable=["parser", "ner"])

def preprocess_text(text):
    """Lemmatize and filter text"""
    if not isinstance(text, str):
        text = str(text)
    doc = nlp(text.lower())  
    lemmatized_tokens = [
        token.lemma_ for token in doc if not token.is_stop and not token.is_punct
    ]
    return " ".join(lemmatized_tokens)

def get_next_cluster_id():
    """Get the next available cluster ID from existing clusters"""
    try:
        # Find the maximum cluster_id in the existing collection
        max_cluster = cluster_collection.find().sort("cluster_id", -1).limit(1)
        max_cluster_list = list(max_cluster)
        if max_cluster_list:
            return max_cluster_list[0]["cluster_id"] + 1
        else:
            return 0
    except Exception as e:
        print(f"Error getting next cluster ID: {e}")
        return 0

# Load data from MongoDB - only documents with embeddings, including domain field
print("Loading data from MongoDB...")
cursor = chat_chunks_collection.find({
    "dominant_topic": {"$exists": True, "$ne": None},
    "embeddings": {"$exists": True, "$ne": []}
}, {
    "_id": 1,
    "dominant_topic": 1,
    "embeddings": 1,
    "domain": 1  # Include domain field
})
documents = list(cursor)
print(f"Found {len(documents)} documents with both dominant_topic and embeddings")

if len(documents) == 0:
    print("No documents found with both dominant_topic and embeddings fields. Exiting...")
    client.close()
    exit()

# Extract keyphrases and embeddings from MongoDB documents
keyphrases = []
embeddings = []
document_data = []  # Store full document data including domain

print("Processing documents and extracting keyphrases with embeddings...")
for doc in documents:
    doc_id = doc["_id"]
    dominant_topic = doc.get("dominant_topic", "")
    embedding_data = doc.get("embeddings", [])
    domain = doc.get("domain", "")  # Extract domain field
    
    if not dominant_topic or not isinstance(dominant_topic, str) or len(dominant_topic.strip()) == 0:
        continue
    
    if not embedding_data or len(embedding_data) == 0:
        continue
        
    # Skip "Unknown Topic" entries
    if dominant_topic.strip().lower() == "unknown topic":
        continue
        
    # Preprocess the dominant topic
    lemmatized_phrase = preprocess_text(dominant_topic)
    if lemmatized_phrase.strip():
        # Convert embedding list to numpy array
        embedding_vector = np.array(embedding_data)
        
        # Store the data
        keyphrases.append(lemmatized_phrase)
        embeddings.append(embedding_vector)
        document_data.append({
            "_id": doc_id,
            "domain": domain,
            "original_keyphrase": dominant_topic,
            "processed_keyphrase": lemmatized_phrase
        })

# Convert embeddings to numpy array
embeddings = np.array(embeddings)

print(f"Total keyphrases with embeddings: {len(keyphrases)}")
print(f"Embedding shape: {embeddings.shape}")

# Verify embeddings are valid
if embeddings.size == 0:
    print("No valid embeddings found. Exiting...")
    client.close()
    exit()

# Apply UMAP dimensionality reduction using the successful parameters from HDBSCAN
print("Applying UMAP dimensionality reduction...")
umap_params = {
    "n_neighbors": 30,
    "min_dist": 0.05
}

reducer = umap.UMAP(
    n_components=25,
    metric='cosine',
    **umap_params
)

try:
    reduced_embeddings = reducer.fit_transform(embeddings)
    print(f"Reduced embedding dimensions from {embeddings.shape[1]} to {reduced_embeddings.shape[1]}")
except Exception as e:
    print(f"UMAP reduction failed: {e}")
    client.close()
    raise
  
# Define K range for optimization
min_k = 10
max_k = 50
step = 5
k_values = list(range(min_k, max_k + 1, step))

# Find optimal K using elbow method and silhouette score
print("Finding optimal K...")
results = []
inertia_values = []

for k in k_values:
    print(f"Testing K={k}...")
    kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
    kmeans.fit(reduced_embeddings)
    inertia_values.append(kmeans.inertia_)
    
    # Calculate silhouette score
    silhouette_avg = silhouette_score(reduced_embeddings, kmeans.labels_, metric="cosine")
    
    results.append({
        "k": k,
        "inertia": kmeans.inertia_,
        "silhouette": silhouette_avg
    })
    
    print(f"K={k}, Silhouette={silhouette_avg:.4f}")

# Find optimal K using elbow method
optimal_k = None
try:
    kneedle = KneeLocator(k_values, inertia_values, curve='convex', direction='decreasing')
    optimal_k = kneedle.elbow
    print(f"Automatically detected optimal K: {optimal_k}")
except Exception as e:
    print(f"Could not automatically detect optimal K: {e}")

# If automatic detection failed, find K with the best silhouette score
if optimal_k is None:
    best_result = max(results, key=lambda x: x["silhouette"])
    optimal_k = best_result["k"]
    print(f"Using K with best silhouette score: {optimal_k}")

# Run K-means with optimal K
print(f"Running final K-means with K={optimal_k}...")
kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
labels = kmeans.fit_predict(reduced_embeddings)

# Calculate final metrics
silhouette_value = silhouette_score(reduced_embeddings, labels, metric="cosine")
print(f"Final silhouette score: {silhouette_value:.4f}")

# Show cluster distribution
unique_labels, counts = np.unique(labels, return_counts=True)
print(f"Created {len(unique_labels)} clusters")
print("Cluster sizes:")
for label, count in zip(unique_labels, counts):
    print(f"  Cluster {label}: {count} items")

# Get the starting cluster ID for new clusters
next_cluster_id = get_next_cluster_id()
print(f"Next available cluster ID: {next_cluster_id}")

# Group keyphrases by cluster_id for cluster collection
print("Grouping keyphrases by cluster...")
cluster_data = defaultdict(lambda: {
    'keyphrases': set(),  # Use set to store unique keyphrases only
    'domains': set(),
    'chat-chunks_ids': []
})

# Collect data for each cluster
for idx, (doc_data, cluster_id) in enumerate(zip(document_data, labels)):
    # Adjust cluster_id to start from next_cluster_id
    adjusted_cluster_id = int(cluster_id) + next_cluster_id
    
    cluster_data[adjusted_cluster_id]['keyphrases'].add(doc_data["processed_keyphrase"])  # Add to set for uniqueness
    cluster_data[adjusted_cluster_id]['domains'].add(doc_data["domain"])
    cluster_data[adjusted_cluster_id]['chat-chunks_ids'].append(str(doc_data["_id"]))

# Generate cluster names (using most common pattern or first few keyphrases)
def generate_cluster_name(keyphrases, max_length=50):
    """Generate a representative cluster name from keyphrases"""
    if not keyphrases:
        return "Empty Cluster"
    
    # Convert set to list for processing
    unique_phrases = list(keyphrases)
    if len(unique_phrases) == 1:
        return unique_phrases[0][:max_length]
    
    # For multiple phrases, create a representative name
    words = []
    for phrase in unique_phrases[:3]:  # Take first 3 unique phrases
        phrase_words = phrase.split()[:2]  # Take first 2 words from each phrase
        words.extend(phrase_words)
    
    # Remove duplicates while preserving order
    seen = set()
    unique_words = []
    for word in words:
        if word not in seen:
            seen.add(word)
            unique_words.append(word)
    
    cluster_name = " / ".join(unique_words[:4])  # Max 4 words
    return cluster_name[:max_length] if len(cluster_name) > max_length else cluster_name

def check_cluster_exists(cluster_id):
    """Check if a cluster with the given ID already exists"""
    return cluster_collection.count_documents({"cluster_id": cluster_id}) > 0

def merge_cluster_data(existing_doc, new_data):
    """Merge new cluster data with existing cluster data"""
    # Merge keyphrases (maintain uniqueness)
    existing_keyphrases = set(existing_doc.get('keyphrases', []))
    new_keyphrases = set(new_data['keyphrases'])
    merged_keyphrases = list(existing_keyphrases.union(new_keyphrases))
    
    # Merge domains
    existing_domains = set(existing_doc.get('domains', []))
    new_domains = set(new_data['domains'])
    merged_domains = list(existing_domains.union(new_domains))
    
    # Merge ticket IDs (maintain uniqueness)
    existing_ticket_ids = set(existing_doc.get('chat-chunks_ids', []))
    new_ticket_ids = set(new_data['chat-chunks_ids'])
    merged_ticket_ids = list(existing_ticket_ids.union(new_ticket_ids))
    
    return {
        'keyphrases': merged_keyphrases,
        'keyphrase_count': len(merged_keyphrases),
        'domains': merged_domains,
        'chat-chunks_ids': merged_ticket_ids,
        'updated_at': time.time()
    }

# Prepare cluster collection documents
cluster_documents = []
cluster_updates = []
chat_chunks_updates = []

print("Preparing data for database updates...")

# Process each cluster and prepare for insert or update
for cluster_id, data in cluster_data.items():
    cluster_name = generate_cluster_name(data['keyphrases'])
    
    # Check if cluster already exists
    if check_cluster_exists(cluster_id):
        print(f"Cluster {cluster_id} already exists, preparing for merge...")
        # Get existing cluster data
        existing_cluster = cluster_collection.find_one({"cluster_id": cluster_id})
        
        # Merge data
        merged_data = merge_cluster_data(existing_cluster, data)
        
        # Prepare update operation
        cluster_update = {
            "filter": {"cluster_id": cluster_id},
            "update": {"$set": {
                "cluster_name": generate_cluster_name(set(merged_data['keyphrases'])),
                "keyphrases": merged_data['keyphrases'],
                "keyphrase_count": merged_data['keyphrase_count'],
                "domains": merged_data['domains'],
                "chat-chunks_ids": merged_data['chat-chunks_ids'],
                "updated_at": merged_data['updated_at']
            }}
        }
        cluster_updates.append(cluster_update)
        
    else:
        # Prepare new cluster document
        cluster_doc = {
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "keyphrases": list(data['keyphrases']),  # Convert set to list for MongoDB storage
            "keyphrase_count": len(data['keyphrases']),  # Count of unique keyphrases
            "domains": list(data['domains']),
            "chat-chunks_ids": data['chat-chunks_ids'],
            "data": "chat-chunks",
            "created_at": time.time()
        }
        cluster_documents.append(cluster_doc)

# Prepare chat-chunks collection updates
for idx, (doc_data, original_cluster_id) in enumerate(zip(document_data, labels)):
    # Use adjusted cluster ID
    adjusted_cluster_id = int(original_cluster_id) + next_cluster_id
    
    chat_chunks_update = {
        "filter": {"_id": doc_data["_id"]},
        "update": {"$set": {
            "kmeans_cluster_id": adjusted_cluster_id,
            "kmeans_cluster_keyphrase": doc_data["processed_keyphrase"],
            "clustering_method": "kmeans",
            "clustering_updated_at": time.time()
        }}
    }
    chat_chunks_updates.append(chat_chunks_update)

# Insert new cluster documents
cluster_inserts_successful = 0
cluster_inserts_failed = 0

if cluster_documents:
    print(f"Inserting {len(cluster_documents)} new cluster documents...")
    try:
        insert_result = cluster_collection.insert_many(cluster_documents)
        cluster_inserts_successful = len(insert_result.inserted_ids)
        print(f"Successfully inserted {cluster_inserts_successful} new cluster documents")
    except Exception as e:
        cluster_inserts_failed = len(cluster_documents)
        print(f"Error inserting new cluster documents: {e}")

# Update existing cluster documents
cluster_updates_successful = 0
cluster_updates_failed = 0

if cluster_updates:
    print(f"Updating {len(cluster_updates)} existing cluster documents...")
    for update_data in cluster_updates:
        try:
            result = cluster_collection.update_one(
                update_data["filter"],
                update_data["update"]
            )
            
            if result.modified_count > 0 or result.matched_count > 0:
                cluster_updates_successful += 1
            else:
                cluster_updates_failed += 1
                
        except Exception as e:
            cluster_updates_failed += 1
            print(f"Error updating cluster {update_data['filter']['cluster_id']}: {e}")

    print(f"Successfully updated {cluster_updates_successful} existing cluster documents")
    if cluster_updates_failed > 0:
        print(f"Failed to update {cluster_updates_failed} cluster documents")

# Update chat-chunks collection
print("Updating chat-chunks collection with K-means cluster information...")
chat_chunks_updates_successful = 0
chat_chunks_updates_failed = 0

for update_data in chat_chunks_updates:
    try:
        result = chat_chunks_collection.update_one(
            update_data["filter"],
            update_data["update"]
        )
        
        if result.modified_count > 0 or result.matched_count > 0:
            chat_chunks_updates_successful += 1
        else:
            chat_chunks_updates_failed += 1
            
    except Exception as e:
        chat_chunks_updates_failed += 1
        print(f"Error updating chat-chunks document {update_data['filter']['_id']}: {e}")

print(f"Successfully updated {chat_chunks_updates_successful} chat-chunks documents with K-means cluster information")
if chat_chunks_updates_failed > 0:
    print(f"Failed to update {chat_chunks_updates_failed} chat-chunks documents")

# Verify updates in both collections
chat_chunks_verification_count = chat_chunks_collection.count_documents({"kmeans_cluster_id": {"$exists": True}})
cluster_verification_count = cluster_collection.count_documents({})

print(f"Verification - chat-chunks collection: {chat_chunks_verification_count} documents now have kmeans_cluster_id field")
print(f"Verification - Cluster collection: {cluster_verification_count} cluster documents stored")

# Show sample cluster data
print("\nSample cluster collection documents:")
sample_clusters = cluster_collection.find().sort("created_at", -1).limit(5)
for i, cluster_doc in enumerate(sample_clusters, 1):
    keyphrases_preview = cluster_doc['keyphrases'][:3] if len(cluster_doc['keyphrases']) > 3 else cluster_doc['keyphrases']
    creation_time = "Recently created" if 'created_at' in cluster_doc else "Updated"
    print(f"  {i}. Cluster {cluster_doc['cluster_id']}: '{cluster_doc['cluster_name']}' ({cluster_doc['keyphrase_count']} unique keyphrases) - {creation_time}")
    print(f"     Sample keyphrases: {keyphrases_preview}")
    print(f"     Domains: {list(cluster_doc['domains'])}")

# Show cluster distribution by size
print("\nTop 10 largest clusters:")
largest_clusters = cluster_collection.find().sort("keyphrase_count", -1).limit(10)
for i, cluster_doc in enumerate(largest_clusters, 1):
    print(f"  {i}. Cluster {cluster_doc['cluster_id']}: '{cluster_doc['cluster_name']}' - {cluster_doc['keyphrase_count']} unique keyphrases")

# Show cluster distribution by domain
print("\nCluster distribution by domain:")
pipeline = [
    {"$unwind": "$domains"},
    {"$group": {
        "_id": "$domains",
        "cluster_count": {"$sum": 1},
        "total_unique_keyphrases": {"$sum": "$keyphrase_count"}
    }},
    {"$sort": {"cluster_count": -1}}
]

domain_stats = list(cluster_collection.aggregate(pipeline))
for stat in domain_stats[:10]:  # Show top 10 domains
    print(f"  {stat['_id'] or 'Unknown'}: {stat['cluster_count']} clusters, {stat['total_unique_keyphrases']} total unique keyphrases")

# Show statistics about new vs existing clusters
print(f"\nCluster Statistics:")
print(f"- Existing clusters found: {existing_cluster_count}")
print(f"- New clusters created: {cluster_inserts_successful}")
print(f"- Existing clusters updated: {cluster_updates_successful}")
print(f"- Total clusters now: {cluster_verification_count}")

# Close MongoDB connection
client.close()

# Report results
end_time = time.time()
execution_time = end_time - start_time

print("\n" + "="*50)
print("K-MEANS CLUSTERING COMPLETE")
print("="*50)
print(f"Execution time: {execution_time:.2f} seconds")
print(f"Documents processed: {len(documents)}")
print(f"Keyphrases clustered: {len(keyphrases)}")
print(f"New clusters created: {cluster_inserts_successful}")
print(f"Existing clusters updated: {cluster_updates_successful}")
print(f"Optimal K: {optimal_k}")
print(f"Silhouette score: {silhouette_value:.4f}")
print(f"chat-chunks documents updated: {chat_chunks_updates_successful}")
print("\nCollections updated:")
print(f"1. {chat_chunks_COLLECTION_NAME} - Added/Updated fields:")
print("   - kmeans_cluster_id: Cluster assignment")
print("   - kmeans_cluster_keyphrase: Processed keyphrase")
print("   - clustering_method: 'kmeans'")
print("   - clustering_updated_at: Timestamp")
print(f"2. {CLUSTER_COLLECTION_NAME} - New/Updated documents with fields:")
print("   - cluster_id: Cluster assignment")
print("   - cluster_name: Generated cluster name")
print("   - keyphrases: Array of unique keyphrases in cluster")
print("   - keyphrase_count: Number of unique keyphrases in cluster")
print("   - domains: Array of domains represented in cluster")
print("   - chat-chunks_ids: Array of chat-chunks document IDs in cluster")
print("   - data: 'chat-chunks'")
print("   - created_at: Timestamp (for new clusters)")
print("   - updated_at: Timestamp (for updated clusters)")
print("="*50)