import warnings
import re
import spacy
import gensim
import hdbscan
import numpy as np
import pandas as pd
import os
import time
import umap
from gensim.corpora.dictionary import Dictionary
from sklearn.metrics import silhouette_score
from sklearn.metrics.pairwise import cosine_similarity
from gensim.models.coherencemodel import CoherenceModel
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
import json

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
COLLECTION_NAME = "tickets"

# Initialize MongoDB connection
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Start timing
start_time = time.time()

print("Connecting to MongoDB...")
print(f"Database: {DB_NAME}")
print(f"Collection: {COLLECTION_NAME}")

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

# Load data from MongoDB - only documents with embeddings
print("Loading data from MongoDB...")
cursor = collection.find({
    "dominant_topic": {"$exists": True, "$ne": None},
    "embeddings": {"$exists": True, "$ne": []}
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
document_ids = []
keyphrase_to_documents = {}
document_to_keyphrase = {}

print("Processing documents and extracting keyphrases with embeddings...")
for doc in documents:
    doc_id = doc["_id"]
    dominant_topic = doc.get("dominant_topic", "")
    embedding_data = doc.get("embeddings", [])
    
    if not dominant_topic or not isinstance(dominant_topic, str) or len(dominant_topic.strip()) == 0:
        print(f"Skipping document {doc_id}: empty or invalid dominant_topic")
        continue
    
    if not embedding_data or len(embedding_data) == 0:
        print(f"Skipping document {doc_id}: empty embeddings")
        continue
        
    # Preprocess the dominant topic
    lemmatized_phrase = preprocess_text(dominant_topic)
    if lemmatized_phrase.strip():
        # Convert embedding list to numpy array
        embedding_vector = np.array(embedding_data)
        
        # Store the data
        keyphrases.append(lemmatized_phrase)
        embeddings.append(embedding_vector)
        document_ids.append(doc_id)
        
        # Create mappings
        document_to_keyphrase[doc_id] = lemmatized_phrase
        if lemmatized_phrase not in keyphrase_to_documents:
            keyphrase_to_documents[lemmatized_phrase] = []
        keyphrase_to_documents[lemmatized_phrase].append(doc_id)
    else:
        print(f"Skipping document {doc_id}: processed keyphrase is empty")

# Convert embeddings to numpy array
embeddings = np.array(embeddings)

print(f"Total keyphrases with embeddings: {len(keyphrases)}")
print(f"Embedding shape: {embeddings.shape}")

# Verify embeddings are valid
if embeddings.size == 0:
    print("No valid embeddings found. Exiting...")
    client.close()
    exit()

# Print sample keyphrases
print("\nSample keyphrases:")
for i, phrase in enumerate(keyphrases[:5]):
    print(f"  {i+1}. {phrase}")

# Create a results directory
os.makedirs("clustering_results", exist_ok=True)

# Save keyphrases for reference
with open("clustering_results/extracted_keyphrases.txt", "w", encoding="utf-8") as f:
    for phrase in keyphrases:
        f.write(f"{phrase}\n")

# We're NOT normalizing embeddings as requested
print("Using original embeddings without normalization...")

# Define parameter sets to try (from the original code)
umap_params_list = [
    {"n_neighbors": 15, "min_dist": 0.1},
    {"n_neighbors": 30, "min_dist": 0.05},
    {"n_neighbors": 50, "min_dist": 0.2}
]

hdbscan_params_list = [
    {"min_cluster_size": 3, "min_samples": 3, "cluster_selection_method": "eom"},
    {"min_cluster_size": 5, "min_samples": 2, "cluster_selection_method": "leaf"},
    {"min_cluster_size": 4, "min_samples": 4, "cluster_selection_method": "eom"}
]

# Track results for all experiments
all_experiment_results = []

# Run experiments with different parameter combinations
best_silhouette = -1
best_coherence = -1
best_avg_similarity = -1
best_umap_params = None
best_hdbscan_params = None
best_labels = None
best_reduced_embeddings = None

for umap_idx, umap_params in enumerate(umap_params_list):
    print(f"\n--- UMAP Experiment {umap_idx+1}: {umap_params} ---")
    
    # Apply UMAP dimensionality reduction
    reducer = umap.UMAP(
        n_components=20,
        metric='cosine',
        **umap_params
    )
    
    try:
        reduced_embeddings = reducer.fit_transform(embeddings)
        print(f"Reduced embedding dimensions from {embeddings.shape[1]} to {reduced_embeddings.shape[1]}")
    except Exception as e:
        print(f"UMAP reduction failed: {e}")
        continue
    
    for hdbscan_idx, hdbscan_params in enumerate(hdbscan_params_list):
        exp_id = f"umap_{umap_idx+1}_hdbscan_{hdbscan_idx+1}"
        print(f"\n-- HDBSCAN Experiment: {hdbscan_params} --")
        
        # Apply HDBSCAN clustering
        clusterer = hdbscan.HDBSCAN(
            metric='euclidean',
            core_dist_n_jobs=-1,
            **hdbscan_params
        )
        
        labels = clusterer.fit_predict(reduced_embeddings)
        
        # Calculate clustering metrics
        num_clusters = len(set(labels)) - (1 if -1 in labels else 0)
        noise_count = np.sum(labels == -1)
        noise_percent = noise_count / len(labels) * 100
        
        print(f"Found {num_clusters} clusters with {noise_count} noise points ({noise_percent:.1f}%)")
        
        # Skip further analysis if we found only noise or a single cluster
        if num_clusters <= 1:
            print("Insufficient clusters found, skipping this parameter combination")
            continue
        
        # Calculate silhouette score
        silhouette_value = np.nan
        try:
            valid_labels = labels != -1
            if valid_labels.sum() > 1 and num_clusters > 1:
                silhouette_value = silhouette_score(
                    reduced_embeddings[valid_labels], 
                    labels[valid_labels], 
                    metric="cosine"
                )
                print(f"Silhouette score: {silhouette_value:.4f}")
        except Exception as e:
            print(f"Could not calculate silhouette score: {e}")
        
        # Create dataframe with cluster assignments
        clustered_df = pd.DataFrame({
            'keyphrase': keyphrases, 
            'cluster_id': labels,
            'document_id': [str(doc_id) for doc_id in document_ids]
        })
        
        # Group by cluster
        final_df = clustered_df.groupby('cluster_id')['keyphrase'].apply(list).reset_index()
        final_df['cluster_name'] = final_df['cluster_id'].apply(
            lambda x: f'Cluster {x}' if x != -1 else 'Cluster -1 (Noise)'
        )
        final_df.rename(columns={'keyphrase': 'keywords'}, inplace=True)
        
        # Show cluster sizes
        cluster_sizes = clustered_df['cluster_id'].value_counts().sort_index()
        print("\nCluster sizes:")
        for cluster_id, size in cluster_sizes.items():
            cluster_name = "Noise" if cluster_id == -1 else f"Cluster {cluster_id}"
            print(f"{cluster_name}: {size} keyphrases")
        
        # Display cluster contents (up to 5 items per cluster)
        print("\nCluster contents:")
        for _, row in final_df.iterrows():
            cluster_id = row['cluster_id']
            keywords = row['keywords']
            
            print(f"\nCluster {cluster_id}")
            print("  Sample phrases (up to 5):")
            for phrase in keywords[:5]:
                print(f"  - {phrase}")
            if len(keywords) > 5:
                print(f"  - ... and {len(keywords) - 5} more phrases")
        
        # Calculate coherence score if valid clusters exist
        coherence_value = np.nan
        noise_cluster = -1 in final_df['cluster_id'].values
        valid_clusters = [row['keywords'] for _, row in final_df.iterrows() 
                         if row['cluster_id'] != -1 or not noise_cluster]
        
        if len(valid_clusters) > 1:
            print("\nCalculating coherence score...")
            try:
                # Prepare data for coherence model
                clustered_tokens = [[word for phrase in cluster for word in phrase.split()] 
                                    for cluster in valid_clusters]
                
                dictionary = Dictionary(clustered_tokens)
                coherence_model_cv = CoherenceModel(
                    topics=clustered_tokens, 
                    texts=clustered_tokens, 
                    dictionary=dictionary, 
                    coherence="c_v"
                )
                coherence_value = coherence_model_cv.get_coherence()
                print(f"C_V Coherence Score: {coherence_value:.4f}")
            except Exception as e:
                print(f"Error calculating coherence score: {e}")
        
        # Calculate average similarity within clusters
        print("\nCalculating average similarity within clusters...")
        
        def average_cluster_similarity(cluster_embeddings):
            """Calculate average cosine similarity within a cluster"""
            if len(cluster_embeddings) < 2:
                return np.nan  
            similarities = cosine_similarity(cluster_embeddings)
            np.fill_diagonal(similarities, 0)  # Exclude self-similarity
            return np.mean(similarities)
        
        # Use original embeddings for similarity calculations
        cluster_similarities = {}
        for cluster_id in set(labels):
            if cluster_id == -1:
                continue  # Skip noise cluster
            cluster_indices = np.where(labels == cluster_id)[0]
            cluster_embeddings = embeddings[cluster_indices]
            avg_sim = average_cluster_similarity(cluster_embeddings)
            cluster_similarities[cluster_id] = avg_sim
            print(f"Cluster {cluster_id}: Avg. Similarity = {avg_sim:.4f}")
        
        valid_similarities = [sim for sim in cluster_similarities.values() if not np.isnan(sim)]
        overall_avg_similarity = np.mean(valid_similarities) if valid_similarities else np.nan
        print(f"Overall Avg. Similarity Across Clusters: {overall_avg_similarity:.4f}")
        
        # Save experiment results
        experiment_result = {
            'experiment_id': exp_id,
            'umap_params': umap_params,
            'hdbscan_params': hdbscan_params,
            'num_clusters': num_clusters,
            'noise_percent': noise_percent,
            'silhouette_score': silhouette_value,
            'coherence_score': coherence_value,
            'avg_similarity': overall_avg_similarity
        }
        all_experiment_results.append(experiment_result)
        
        # Save cluster results for this experiment
        final_df['experiment_id'] = exp_id
        final_df['umap_params'] = str(umap_params)
        final_df['hdbscan_params'] = str(hdbscan_params)
        clustered_df['experiment_id'] = exp_id
        
        output_folder = f"clustering_results/exp_{exp_id}"
        os.makedirs(output_folder, exist_ok=True)
        final_df.to_csv(f"{output_folder}/clusters.csv", index=False)
        clustered_df.to_csv(f"{output_folder}/all_keyphrases_with_clusters.csv", index=False)
        
        # Save cluster visualization - create a 2D projection for visualization
        try:
            viz_reducer = umap.UMAP(n_components=2, **umap_params)
            viz_embeddings = viz_reducer.fit_transform(embeddings)
            
            viz_df = pd.DataFrame({
                'x': viz_embeddings[:, 0],
                'y': viz_embeddings[:, 1],
                'cluster': labels,
                'keyphrase': keyphrases,
                'document_id': [str(doc_id) for doc_id in document_ids]
            })
            viz_df.to_csv(f"{output_folder}/cluster_visualization.csv", index=False)
        except Exception as e:
            print(f"Error creating visualization data: {e}")
        
        # Check if this is the best result so far - prioritize silhouette score
        is_best = False
        if not np.isnan(silhouette_value) and silhouette_value > best_silhouette:
            best_silhouette = silhouette_value
            best_coherence = coherence_value
            best_avg_similarity = overall_avg_similarity
            best_umap_params = umap_params
            best_hdbscan_params = hdbscan_params
            best_labels = labels
            best_reduced_embeddings = reduced_embeddings
            is_best = True
        
        if is_best:
            print(f"\n*** New best result: {exp_id} ***")

# Save summary of all experiments
pd.DataFrame(all_experiment_results).to_csv("clustering_results/all_experiments_summary.csv", index=False)

# Use the best parameters if any valid results were found
if best_umap_params is not None:
    print("\n\n===== BEST CONFIGURATION FOUND =====")
    print(f"UMAP parameters: {best_umap_params}")
    print(f"HDBSCAN parameters: {best_hdbscan_params}")
    print(f"Silhouette score: {best_silhouette:.4f}")
    print(f"Coherence score: {best_coherence:.4f}")
    print(f"Average similarity: {best_avg_similarity:.4f}")
    
    # Create and save final results using the best configuration
    clustered_df = pd.DataFrame({
        'keyphrase': keyphrases, 
        'cluster_id': best_labels,
        'document_id': [str(doc_id) for doc_id in document_ids]
    })
    
    # Group by cluster and create meaningful names for clusters
    final_df = clustered_df.groupby('cluster_id')['keyphrase'].apply(list).reset_index()
    
    # Create more descriptive cluster names by using the most common words
    def get_descriptive_name(keywords, cluster_id):
        if cluster_id == -1:
            return "Uncategorized Topics"
        
        # Get all words from all phrases in this cluster
        all_words = []
        for phrase in keywords:
            all_words.extend(phrase.split())
        
        # Count word frequencies
        word_counts = {}
        for word in all_words:
            if len(word) > 3:  # Only consider words longer than 3 characters
                word_counts[word] = word_counts.get(word, 0) + 1
        
        # Get top 2-3 most frequent words
        top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
        
        if not top_words:
            return f"Cluster {cluster_id}"
        
        # Take up to 3 most common words
        name_words = [word for word, count in top_words[:3]]
        return " / ".join(name_words).title()
    
    final_df['cluster_name'] = final_df.apply(
        lambda row: get_descriptive_name(row['keyphrase'], row['cluster_id']), axis=1
    )
    final_df.rename(columns={'keyphrase': 'keywords'}, inplace=True)
    
    # Save final results
    final_df.to_csv("clustering_results/best_clusters.csv", index=False)
    clustered_df.to_csv("clustering_results/best_all_keyphrases_with_clusters.csv", index=False)
    
    # Save keyphrase to document mapping
    mapping_data = []
    for phrase, doc_ids in keyphrase_to_documents.items():
        for doc_id in doc_ids:
            mapping_data.append({
                'keyphrase': phrase,
                'document_id': str(doc_id)
            })
    
    mapping_df = pd.DataFrame(mapping_data)
    mapping_df.to_csv("clustering_results/keyphrase_document_mapping.csv", index=False)
    print("Saved keyphrase-document mapping to: clustering_results/keyphrase_document_mapping.csv")
    
    # Print final cluster contents
    print("\nFinal Clusters:")
    for _, row in final_df.iterrows():
        cluster_id = row['cluster_id']
        cluster_name = row['cluster_name']
        keywords = row['keywords']
        
        print(f"\n{cluster_name} (Cluster {cluster_id}):")
        for phrase in keywords[:10]:  # Show up to 10 phrases per cluster
            print(f"  - {phrase}")
        if len(keywords) > 10:
            print(f"  - ... and {len(keywords) - 10} more phrases")
    
    # Save final statistics
    stats = {
        'total_documents_processed': len(documents),
        'total_keyphrases_clustered': len(keyphrases),
        'best_num_clusters': len(set(best_labels)) - (1 if -1 in best_labels else 0),
        'best_silhouette_score': best_silhouette,
        'best_coherence_score': best_coherence,
        'best_avg_similarity': best_avg_similarity,
        'best_umap_params': best_umap_params,
        'best_hdbscan_params': best_hdbscan_params,
        'processing_time_seconds': time.time() - start_time,
        'noise_points': int(np.sum(best_labels == -1))
    }
    
    with open("clustering_results/clustering_statistics.json", "w") as f:
        json.dump(stats, f, indent=2, default=str)
    print("Saved clustering statistics to: clustering_results/clustering_statistics.json")

else:
    print("\nNo valid clustering results found with the given parameters.")
    print("Consider adjusting the UMAP or HDBSCAN parameters.")

# Close MongoDB connection
client.close()

# Report total execution time
end_time = time.time()
execution_time = end_time - start_time

print("\n" + "="*60)
print("CLUSTERING ANALYSIS COMPLETE")
print("="*60)
print(f"Total execution time: {execution_time:.2f} seconds")
print(f"Documents processed: {len(documents)}")
print(f"Keyphrases clustered: {len(keyphrases)}")
if best_umap_params is not None:
    print(f"Best configuration found with {len(set(best_labels)) - (1 if -1 in best_labels else 0)} clusters")
    print(f"Best silhouette score: {best_silhouette:.4f}")
print("\nAll results saved to 'clustering_results' directory:")
print("- all_experiments_summary.csv: Summary of all parameter combinations tested")
print("- best_clusters.csv: Final cluster results using best parameters")
print("- best_all_keyphrases_with_clusters.csv: All keyphrases with their cluster assignments")
print("- keyphrase_document_mapping.csv: Mapping between keyphrases and documents")
print("- clustering_statistics.json: Overall clustering statistics")
print("- exp_*/: Individual experiment results for each parameter combination")
print("="*60)