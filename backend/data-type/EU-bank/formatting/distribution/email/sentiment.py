# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import random

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def distribute_sentiment_scores():
    """
    Distribute sentiment scores (0-5) across all records based on the given percentages:
    0 - 15%
    1 - 17%
    2 - 16%
    3 - 18%
    4 - 19%
    5 - 15%
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # Get total count of documents
        total_docs = collection.count_documents({})
        print(f"Total documents in collection: {total_docs}")
        
        if total_docs == 0:
            print("No documents found in the collection")
            return None
        
        # Define sentiment distribution percentages
        sentiment_distribution = {
            0: 0.15,  # 15%
            1: 0.17,  # 17%
            2: 0.16,  # 16%
            3: 0.18,  # 18%
            4: 0.19,  # 19%
            5: 0.15   # 15%
        }
        
        # Calculate number of documents for each sentiment
        sentiment_counts = {}
        for sentiment, percentage in sentiment_distribution.items():
            count = int(total_docs * percentage)
            sentiment_counts[sentiment] = count
        
        # Adjust for any rounding differences
        total_assigned = sum(sentiment_counts.values())
        if total_assigned < total_docs:
            # Add remaining documents to sentiment 4 (highest percentage)
            sentiment_counts[4] += (total_docs - total_assigned)
        
        print(f"\nSentiment distribution plan:")
        for sentiment, count in sentiment_counts.items():
            percentage = (count / total_docs) * 100
            print(f"  Sentiment {sentiment}: {count} documents ({percentage:.1f}%)")
        
        # Get all document IDs
        print(f"\nFetching all document IDs...")
        all_docs = list(collection.find({}, {'_id': 1}))
        doc_ids = [doc['_id'] for doc in all_docs]
        
        # Shuffle the document IDs for random distribution
        random.shuffle(doc_ids)
        
        # Assign sentiment scores to documents
        print(f"\nAssigning sentiment scores...")
        current_index = 0
        
        for sentiment, count in sentiment_counts.items():
            if count > 0:
                # Get the next batch of documents for this sentiment
                end_index = current_index + count
                docs_for_sentiment = doc_ids[current_index:end_index]
                
                # Update documents with this sentiment score
                result = collection.update_many(
                    {'_id': {'$in': docs_for_sentiment}},
                    {'$set': {'overall_sentiment': sentiment}}
                )
                
                print(f"  Sentiment {sentiment}: Updated {result.modified_count} documents")
                current_index = end_index
        
        # Verification
        print(f"\n=== VERIFICATION ===")
        total_updated = 0
        for sentiment in range(6):
            count = collection.count_documents({'overall_sentiment': sentiment})
            percentage = (count / total_docs) * 100
            print(f"  Sentiment {sentiment}: {count} documents ({percentage:.1f}%)")
            total_updated += count
        
        print(f"\nTotal documents with sentiment scores: {total_updated}")
        print(f"Total documents in collection: {total_docs}")
        
        if total_updated == total_docs:
            print("✓ All documents successfully assigned sentiment scores!")
        else:
            print(f"⚠ Warning: {total_docs - total_updated} documents were not assigned sentiment scores")
        
        # Show sample documents
        print(f"\nSample documents with sentiment scores:")
        sample_docs = collection.find(
            {'overall_sentiment': {'$exists': True}},
            {'dominant_topic': 1, 'overall_sentiment': 1}
        ).limit(10)
        
        for doc in sample_docs:
            print(f"  {doc['dominant_topic']}: Sentiment {doc['overall_sentiment']}")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'updated_documents': total_updated,
            'sentiment_distribution': sentiment_counts
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting sentiment score distribution process...\n")
    print("Distribution percentages:")
    print("  0 - 15%")
    print("  1 - 17%")
    print("  2 - 16%")
    print("  3 - 18%")
    print("  4 - 19%")
    print("  5 - 15%")
    print()
    
    result = distribute_sentiment_scores()
    
    if result:
        print(f"\nProcess completed successfully!")
        print(f"Updated {result['updated_documents']} MongoDB documents")
    else:
        print("\nProcess failed!")