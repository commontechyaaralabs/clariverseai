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

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
EMAIL_COLLECTION_NAME = "emailmessages"
CLUSTER_COLLECTION_NAME = "cluster"

# Initialize MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
email_collection = db[EMAIL_COLLECTION_NAME]
cluster_collection = db[CLUSTER_COLLECTION_NAME]

# Start timing
start_time = time.time()

print("Connecting to MongoDB...")
print(f"Database: {DB_NAME}")
print(f"Email Collection: {EMAIL_COLLECTION_NAME}")
print(f"Cluster Collection: {CLUSTER_COLLECTION_NAME}")

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

# Load data from MongoDB - only documents with embeddings, including domain field
print("Loading data from MongoDB...")
cursor = email_collection.find({
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
    "n_neighbors": 15,
    "min_dist": 0.1
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
min_k = 30
max_k = 100
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

# Group keyphrases by cluster_id for cluster collection
print("Grouping keyphrases by cluster...")
cluster_data = defaultdict(lambda: {
    'keyphrases': set(),  # Use set to store unique keyphrases only
    'domains': set(),
    'email_ids': []
})

# Collect data for each cluster
for idx, (doc_data, cluster_id) in enumerate(zip(document_data, labels)):
    cluster_data[int(cluster_id)]['keyphrases'].add(doc_data["processed_keyphrase"])  # Add to set for uniqueness
    cluster_data[int(cluster_id)]['domains'].add(doc_data["domain"])
    cluster_data[int(cluster_id)]['email_ids'].append(str(doc_data["_id"]))

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

# Prepare cluster collection documents
cluster_documents = []
email_updates = []

print("Preparing data for database updates...")

# Create cluster collection documents (one per cluster)
for cluster_id, data in cluster_data.items():
    cluster_name = generate_cluster_name(data['keyphrases'])
    
    cluster_doc = {
        "cluster_id": cluster_id,
        "cluster_name": cluster_name,
        "keyphrases": list(data['keyphrases']),  # Convert set to list for MongoDB storage
        "keyphrase_count": len(data['keyphrases']),  # Count of unique keyphrases
        "domains": list(data['domains']),
        "email_ids": data['email_ids'],
        "data": "email",
        "created_at": time.time()
    }
    cluster_documents.append(cluster_doc)

# Prepare email collection updates
for idx, (doc_data, cluster_id) in enumerate(zip(document_data, labels)):
    email_update = {
        "filter": {"_id": doc_data["_id"]},
        "update": {"$set": {
            "kmeans_cluster_id": int(cluster_id),
            "kmeans_cluster_keyphrase": doc_data["processed_keyphrase"],
            "clustering_method": "kmeans",
            "clustering_updated_at": time.time()
        }}
    }
    email_updates.append(email_update)

# Insert documents into cluster collection
print("Inserting documents into cluster collection...")
cluster_inserts_successful = 0
cluster_inserts_failed = 0

try:
    # Clear existing cluster data (optional - remove if you want to keep historical data)
    existing_count = cluster_collection.count_documents({})
    if existing_count > 0:
        print(f"Found {existing_count} existing documents in cluster collection. Clearing...")
        cluster_collection.delete_many({})
    
    # Insert new cluster documents
    if cluster_documents:
        insert_result = cluster_collection.insert_many(cluster_documents)
        cluster_inserts_successful = len(insert_result.inserted_ids)
        print(f"Successfully inserted {cluster_inserts_successful} cluster documents into cluster collection")
    
except Exception as e:
    cluster_inserts_failed = len(cluster_documents)
    print(f"Error inserting into cluster collection: {e}")

# Update emailmessages collection
print("Updating emailmessages collection with K-means cluster information...")
email_updates_successful = 0
email_updates_failed = 0

for update_data in email_updates:
    try:
        result = email_collection.update_one(
            update_data["filter"],
            update_data["update"]
        )
        
        if result.modified_count > 0 or result.matched_count > 0:
            email_updates_successful += 1
        else:
            email_updates_failed += 1
            
    except Exception as e:
        email_updates_failed += 1
        print(f"Error updating email document {update_data['filter']['_id']}: {e}")

print(f"Successfully updated {email_updates_successful} email documents with K-means cluster information")
if email_updates_failed > 0:
    print(f"Failed to update {email_updates_failed} email documents")

# Verify updates in both collections
email_verification_count = email_collection.count_documents({"kmeans_cluster_id": {"$exists": True}})
cluster_verification_count = cluster_collection.count_documents({})

print(f"Verification - Email collection: {email_verification_count} documents now have kmeans_cluster_id field")
print(f"Verification - Cluster collection: {cluster_verification_count} cluster documents stored")

# Show sample cluster data
print("\nSample cluster collection documents:")
sample_clusters = cluster_collection.find().limit(5)
for i, cluster_doc in enumerate(sample_clusters, 1):
    keyphrases_preview = cluster_doc['keyphrases'][:3] if len(cluster_doc['keyphrases']) > 3 else cluster_doc['keyphrases']
    print(f"  {i}. Cluster {cluster_doc['cluster_id']}: '{cluster_doc['cluster_name']}' ({cluster_doc['keyphrase_count']} unique keyphrases)")
    print(f"     Sample keyphrases: {keyphrases_preview}")
    print(f"     Domains: {cluster_doc['domains']}")

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
print(f"Clusters created: {len(cluster_documents)}")
print(f"Optimal K: {optimal_k}")
print(f"Silhouette score: {silhouette_value:.4f}")
print(f"Email documents updated: {email_updates_successful}")
print(f"Cluster documents inserted: {cluster_inserts_successful}")
print("\nCollections updated:")
print(f"1. {EMAIL_COLLECTION_NAME} - Added fields:")
print("   - kmeans_cluster_id: Cluster assignment")
print("   - kmeans_cluster_keyphrase: Processed keyphrase")
print("   - clustering_method: 'kmeans'")
print("   - clustering_updated_at: Timestamp")
print(f"2. {CLUSTER_COLLECTION_NAME} - New collection with fields:")
print("   - cluster_id: Cluster assignment")
print("   - cluster_name: Generated cluster name")
print("   - keyphrases: Array of unique keyphrases in cluster")
print("   - keyphrase_count: Number of unique keyphrases in cluster")
print("   - domains: Array of domains represented in cluster")
print("   - email_ids: Array of email document IDs in cluster")
print("   - data: 'email'")
print("   - created_at: Timestamp")
print("="*50)