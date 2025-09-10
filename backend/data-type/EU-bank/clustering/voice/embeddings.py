import warnings
import spacy
import torch
import numpy as np
import pandas as pd
import os
import json
import time
from transformers import AutoTokenizer, AutoModel
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
COLLECTION_NAME = "voice"

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

# Load data from MongoDB
print("Loading data from MongoDB...")
cursor = collection.find({"dominant_topic": {"$exists": True, "$ne": None}})
documents = list(cursor)
print(f"Found {len(documents)} documents with dominant_topic in chat-chunks collection")

if len(documents) == 0:
    print("No documents found with dominant_topic field. Exiting...")
    exit()

# Create results directory
os.makedirs("embedding_results", exist_ok=True)

# Process documents and extract keyphrases
processed_data = []
document_to_keyphrase = {}  # Map each document to its keyphrase
keyphrase_to_documents = {}  # Map each keyphrase to its documents

print("Processing documents and extracting keyphrases...")
for doc in documents:
    doc_id = doc["_id"]
    dominant_topic = doc.get("dominant_topic", "")
    
    if not dominant_topic or not isinstance(dominant_topic, str) or len(dominant_topic.strip()) == 0:
        print(f"Skipping document {doc_id}: empty or invalid dominant_topic")
        continue
        
    # Preprocess the dominant topic
    lemmatized_phrase = preprocess_text(dominant_topic)
    if lemmatized_phrase.strip():
        # Store document information
        processed_data.append({
            'document_id': str(doc_id),
            'original_topic': dominant_topic,
            'processed_topic': lemmatized_phrase
        })
        
        # Create bidirectional mapping
        document_to_keyphrase[doc_id] = lemmatized_phrase
        
        if lemmatized_phrase not in keyphrase_to_documents:
            keyphrase_to_documents[lemmatized_phrase] = []
        keyphrase_to_documents[lemmatized_phrase].append(doc_id)
    else:
        print(f"Skipping document {doc_id}: processed keyphrase is empty")

# Get unique keyphrases
keyphrases = list(keyphrase_to_documents.keys())

keyphrases = list(keyphrase_to_documents.keys())
print(f"Total extracted keyphrases: {len(keyphrases)}")
print(f"Total documents with valid keyphrases: {len(document_to_keyphrase)}")

# Debug: Show sample mappings
print("\nSample document-keyphrase mappings:")
sample_count = 0
for doc_id, keyphrase in list(document_to_keyphrase.items())[:3]:
    print(f"  Document {doc_id} -> '{keyphrase}'")
    sample_count += 1

# Save processed data to CSV
processed_df = pd.DataFrame(processed_data)
processed_df.to_csv("embedding_results/processed_documents.csv", index=False)
print("Saved processed documents to: embedding_results/processed_documents.csv")

# Save keyphrases to text file
with open("embedding_results/extracted_keyphrases.txt", "w", encoding="utf-8") as f:
    for phrase in keyphrases:
        f.write(f"{phrase}\n")
print("Saved keyphrases to: embedding_results/extracted_keyphrases.txt")

# Check for existing embeddings
print("Checking for existing embeddings in MongoDB...")
existing_embeddings_count = collection.count_documents({"embeddings": {"$exists": True}})
print(f"Found {existing_embeddings_count} documents with existing embeddings")

# Load existing embeddings if any
keyphrase_to_embedding = {}
if existing_embeddings_count > 0:
    docs_with_embeddings = list(collection.find(
        {"embeddings": {"$exists": True}}, 
        {"_id": 1, "dominant_topic": 1, "embeddings": 1}
    ))
    
    for doc in docs_with_embeddings:
        dominant_topic = doc.get("dominant_topic", "")
        if dominant_topic:
            lemmatized_phrase = preprocess_text(dominant_topic)
            if lemmatized_phrase in keyphrases:
                embeddings_data = doc.get("embeddings", [])
                if embeddings_data:
                    keyphrase_to_embedding[lemmatized_phrase] = np.array(embeddings_data)

print(f"Loaded {len(keyphrase_to_embedding)} existing embeddings")

# Determine which keyphrases need new embeddings
keyphrases_needing_embeddings = [kp for kp in keyphrases if kp not in keyphrase_to_embedding]
print(f"Need to compute embeddings for {len(keyphrases_needing_embeddings)} keyphrases")

if keyphrases_needing_embeddings:
    print("Loading embedding model...")
    
    # Load model for embeddings
    model_name = "Alibaba-NLP/gte-Qwen2-7B-instruct"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    
    device = "cuda" if torch.cuda.is_available() else "cpu"
    print(f"Using device: {device}")
    
    try:
        model = AutoModel.from_pretrained(
            model_name, torch_dtype=torch.float16, device_map="auto"
        )
        print("Model loaded with automatic device mapping")
    except Exception as e:
        print(f"Auto device mapping failed: {e}")
        try:
            model = AutoModel.from_pretrained(model_name).to(device)
            print("Model loaded with manual device assignment")
        except Exception as e2:
            print(f"Model loading failed completely: {e2}")
            exit()
    
    def get_embedding(text):
        """Get embedding for a single text"""
        try:
            inputs = tokenizer(f"query: {text}", return_tensors="pt", truncation=True, max_length=512).to(device)
            with torch.no_grad():
                output = model(**inputs).last_hidden_state.mean(dim=1)  
            return output.cpu().numpy().flatten()
        except Exception as e:
            print(f"Error getting embedding for '{text}': {e}")
            return None
    
    def update_documents_with_embeddings(keyphrase_embedding_pairs):
        """Update MongoDB documents with embeddings for specific keyphrases"""
        if not keyphrase_embedding_pairs:
            return 0
            
        total_updated = 0
        failed_updates = 0
        
        for keyphrase, embedding in keyphrase_embedding_pairs:
            try:
                # Find all documents that have this keyphrase
                documents_to_update = keyphrase_to_documents.get(keyphrase, [])
                
                for doc_id in documents_to_update:
                    try:
                        result = collection.update_one(
                            {"_id": doc_id},
                            {"$set": {"embeddings": embedding.tolist()}}
                        )
                        
                        if result.modified_count > 0 or result.matched_count > 0:
                            total_updated += 1
                        else:
                            print(f"Warning: Document {doc_id} not found for update")
                            failed_updates += 1
                            
                    except Exception as e:
                        print(f"Error updating document {doc_id}: {e}")
                        failed_updates += 1
                        
            except Exception as e:
                print(f"Error processing keyphrase '{keyphrase}': {e}")
                failed_updates += 1
        
        if failed_updates > 0:
            print(f"Batch update completed with {failed_updates} failed updates")
            
        return total_updated
    
    # Compute embeddings in batches and store after each batch
    print("Computing embeddings and storing in batches...")
    batch_size = 16  # Reduced batch size for stability
    new_embeddings = {}
    total_documents_updated = 0
    embedding_summary = []
    
    for i in range(0, len(keyphrases_needing_embeddings), batch_size):
        batch = keyphrases_needing_embeddings[i:i+batch_size]
        batch_embeddings = []  # Store (keyphrase, embedding) pairs for this batch
        
        print(f"\nProcessing batch {i//batch_size + 1}/{(len(keyphrases_needing_embeddings) + batch_size - 1)//batch_size}")
        print(f"Keyphrases {i+1} to {min(i+batch_size, len(keyphrases_needing_embeddings))} of {len(keyphrases_needing_embeddings)}")
        
        # Compute embeddings for current batch
        for phrase in batch:
            embedding = get_embedding(phrase)
            if embedding is not None:
                new_embeddings[phrase] = embedding
                keyphrase_to_embedding[phrase] = embedding
                batch_embeddings.append((phrase, embedding))
                
                # Add to summary
                embedding_summary.append({
                    'keyphrase': phrase,
                    'embedding_dimension': len(embedding),
                    'document_count': len(keyphrase_to_documents.get(phrase, [])),
                    'batch_number': i//batch_size + 1
                })
            else:
                print(f"Failed to generate embedding for keyphrase: '{phrase}'")
        
        print(f"Generated {len(batch_embeddings)} embeddings in this batch")
        
        # Store embeddings in MongoDB immediately after batch completion
        if batch_embeddings:
            print("Storing batch embeddings in MongoDB...")
            batch_updates = update_documents_with_embeddings(batch_embeddings)
            total_documents_updated += batch_updates
            print(f"Updated {batch_updates} documents in this batch")
            print(f"Total documents updated so far: {total_documents_updated}")
            
            # Save intermediate progress
            if (i//batch_size + 1) % 5 == 0:  # Save every 5 batches
                print("Saving intermediate progress...")
                temp_summary_df = pd.DataFrame(embedding_summary)
                temp_summary_df.to_csv("embedding_results/embedding_summary_progress.csv", index=False)
        
        print(f"Batch {i//batch_size + 1} completed and stored successfully")
    
    print(f"\nAll batches completed!")
    print(f"Successfully computed {len(new_embeddings)} new embeddings")
    print(f"Total documents updated in MongoDB: {total_documents_updated}")
    
    # Save final embedding summary
    if embedding_summary:
        embedding_summary_df = pd.DataFrame(embedding_summary)
        embedding_summary_df.to_csv("embedding_results/embedding_summary.csv", index=False)
        print("Saved embedding summary to: embedding_results/embedding_summary.csv")
        
        # Remove progress file if it exists
        progress_file = "embedding_results/embedding_summary_progress.csv"
        if os.path.exists(progress_file):
            os.remove(progress_file)
            print("Removed intermediate progress file")

# Collect final results
print("\nCollecting final results...")

# Get all embeddings in the correct order
final_embeddings = []
final_keyphrases = []
embedding_info = []

for phrase in keyphrases:
    if phrase in keyphrase_to_embedding:
        embedding = keyphrase_to_embedding[phrase]
        final_embeddings.append(embedding)
        final_keyphrases.append(phrase)
        
        embedding_info.append({
            'keyphrase': phrase,
            'embedding_dimension': len(embedding),
            'document_count': len(keyphrase_to_documents.get(phrase, [])),
            'has_embedding': True
        })
    else:
        embedding_info.append({
            'keyphrase': phrase,
            'embedding_dimension': 0,
            'document_count': len(keyphrase_to_documents.get(phrase, [])),
            'has_embedding': False
        })

# Convert to numpy array
if final_embeddings:
    final_embeddings_array = np.array(final_embeddings)
    print(f"Final embeddings shape: {final_embeddings_array.shape}")
    
    # Save embeddings as numpy file
    np.save("embedding_results/all_embeddings.npy", final_embeddings_array)
    print("Saved embeddings array to: embedding_results/all_embeddings.npy")

# Save embedding information
embedding_info_df = pd.DataFrame(embedding_info)
embedding_info_df.to_csv("embedding_results/embedding_info.csv", index=False)
print("Saved embedding info to: embedding_results/embedding_info.csv")

# Save keyphrase to document mapping
mapping_data = []
for phrase, doc_ids in keyphrase_to_documents.items():
    for doc_id in doc_ids:
        mapping_data.append({
            'keyphrase': phrase,
            'document_id': str(doc_id),
            'has_embedding': phrase in keyphrase_to_embedding
        })

mapping_df = pd.DataFrame(mapping_data)
mapping_df.to_csv("embedding_results/keyphrase_document_mapping.csv", index=False)
print("Saved keyphrase-document mapping to: embedding_results/keyphrase_document_mapping.csv")

# Final verification from database with detailed checking
print("\nPerforming comprehensive final verification on chat-chunks collection...")

# Check total documents with embeddings
final_embedding_count = collection.count_documents({"embeddings": {"$exists": True}})
print(f"Total documents in chat-chunks collection with 'embeddings' field: {final_embedding_count}")

# Check total documents that should have embeddings
total_documents_should_have_embeddings = len(document_to_keyphrase)
print(f"Total documents that should have embeddings: {total_documents_should_have_embeddings}")

# Sample verification
print("\nSample document verification:")
sample_docs_with_embeddings = list(collection.find({"embeddings": {"$exists": True}}).limit(5))
for i, doc in enumerate(sample_docs_with_embeddings):
    doc_id = doc["_id"]
    dominant_topic = doc.get("dominant_topic", "N/A")
    embedding_length = len(doc.get("embeddings", []))
    print(f"  {i+1}. Doc {doc_id}: topic='{dominant_topic}', embedding_length={embedding_length}")

# Check for documents that should have embeddings but don't
missing_embeddings = []
for doc_id, keyphrase in list(document_to_keyphrase.items())[:10]:  # Check first 10
    doc_check = collection.find_one({"_id": doc_id}, {"embeddings": 1, "dominant_topic": 1})
    if not doc_check or "embeddings" not in doc_check:
        missing_embeddings.append((doc_id, keyphrase))

if missing_embeddings:
    print(f"\nWarning: {len(missing_embeddings)} documents missing embeddings (showing first few):")
    for doc_id, keyphrase in missing_embeddings[:3]:
        print(f"  Document {doc_id} with keyphrase '{keyphrase}' has no embeddings")
else:
    print("\nAll sampled documents have embeddings correctly stored!")

# Save final statistics
stats = {
    'total_documents_processed': len(documents),
    'total_keyphrases_extracted': len(keyphrases),
    'keyphrases_with_embeddings': len(final_keyphrases),
    'documents_with_embeddings': final_embedding_count,
    'embedding_dimension': len(final_embeddings[0]) if final_embeddings else 0,
    'processing_time_seconds': time.time() - start_time,
    'batch_size_used': 16,
    'total_batches_processed': (len(keyphrases_needing_embeddings) + 15) // 16 if keyphrases_needing_embeddings else 0
}

with open("embedding_results/processing_statistics.json", "w") as f:
    json.dump(stats, f, indent=2)

print("Saved processing statistics to: embedding_results/processing_statistics.json")

# Close MongoDB connection
client.close()

# Report final results
end_time = time.time()
execution_time = end_time - start_time

print("\n" + "="*60)
print("EMBEDDING GENERATION COMPLETE")
print("="*60)
print(f"Total execution time: {execution_time:.2f} seconds")
print(f"Documents processed: {len(documents)}")
print(f"Unique keyphrases: {len(keyphrases)}")
print(f"Keyphrases with embeddings: {len(final_keyphrases)}")
print(f"Documents updated in emailmessages collection: {final_embedding_count}")
print("\nAll results saved to 'embedding_results' directory:")
print("- processed_documents.csv: Original and processed document data")
print("- extracted_keyphrases.txt: List of all extracted keyphrases")
print("- embedding_summary.csv: Summary of generated embeddings")
print("- embedding_info.csv: Information about each keyphrase and its embedding")
print("- keyphrase_document_mapping.csv: Mapping between keyphrases and documents")
print("- all_embeddings.npy: Numpy array of all embeddings")
print("- processing_statistics.json: Overall processing statistics")
print(f"\nEmbeddings saved in 'embeddings' field in 'chat-chunks' collection")
print("="*60)