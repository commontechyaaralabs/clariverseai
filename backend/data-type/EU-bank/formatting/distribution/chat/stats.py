# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collections
chat_new_collection = db['chat_new']

def analyze_dominant_topics_with_follow_up():
    """
    Analyze unique dominant_topic values for records with follow_up_required: "yes"
    """
    
    print("="*80)
    print("DOMINANT TOPIC STATISTICS (follow_up_required: 'yes')")
    print("="*80)
    
    # Filter records with follow_up_required: "yes"
    query = {"follow_up_required": "yes"}
    
    # Get all documents with follow_up_required: "yes"
    follow_up_docs = list(chat_new_collection.find(query))
    total_follow_up_records = len(follow_up_docs)
    
    print(f"Total records with follow_up_required: 'yes': {total_follow_up_records}")
    print()
    
    if total_follow_up_records == 0:
        print("No records found with follow_up_required: 'yes'")
        return
    
    # Extract dominant_topic values
    dominant_topics = []
    for doc in follow_up_docs:
        dominant_topic = doc.get('dominant_topic')
        if dominant_topic is not None and dominant_topic != "":
            dominant_topics.append(dominant_topic)
    
    # Count occurrences of each dominant_topic
    from collections import Counter
    topic_counts = Counter(dominant_topics)
    
    # Sort by count (descending)
    sorted_topics = sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)
    
    print(f"Unique dominant_topic values found: {len(sorted_topics)}")
    print(f"Total records with valid dominant_topic: {len(dominant_topics)}")
    print()
    
    # Display statistics
    print("DOMINANT TOPIC DISTRIBUTION:")
    print("-" * 60)
    
    for i, (topic, count) in enumerate(sorted_topics, 1):
        percentage = (count / len(dominant_topics)) * 100
        print(f"{i:2d}. {topic}")
        print(f"    Count: {count} ({percentage:.1f}%)")
        print()
    
    # Summary statistics
    print("="*80)
    print("SUMMARY STATISTICS")
    print("="*80)
    print(f"Total records with follow_up_required: 'yes': {total_follow_up_records}")
    print(f"Records with valid dominant_topic: {len(dominant_topics)}")
    print(f"Records without dominant_topic: {total_follow_up_records - len(dominant_topics)}")
    print(f"Unique dominant_topics: {len(sorted_topics)}")
    
    if sorted_topics:
        most_common = sorted_topics[0]
        least_common = sorted_topics[-1]
        print(f"Most common topic: '{most_common[0]}' ({most_common[1]} occurrences)")
        print(f"Least common topic: '{least_common[0]}' ({least_common[1]} occurrence{'s' if least_common[1] > 1 else ''})")

def analyze_dominant_topics_by_category():
    """
    Analyze dominant_topics by category (Internal/External) for follow_up_required: "yes"
    """
    print("\n" + "="*80)
    print("DOMINANT TOPIC BY CATEGORY (follow_up_required: 'yes')")
    print("="*80)
    
    # Filter records with follow_up_required: "yes"
    query = {"follow_up_required": "yes"}
    
    # Get all documents with follow_up_required: "yes"
    follow_up_docs = list(chat_new_collection.find(query))
    
    if not follow_up_docs:
        print("No records found with follow_up_required: 'yes'")
        return
    
    # Separate by category
    internal_docs = [doc for doc in follow_up_docs if doc.get('category') == 'Internal']
    external_docs = [doc for doc in follow_up_docs if doc.get('category') == 'External']
    
    print(f"Internal records: {len(internal_docs)}")
    print(f"External records: {len(external_docs)}")
    print()
    
    # Analyze Internal category
    if internal_docs:
        print("INTERNAL CATEGORY:")
        print("-" * 40)
        internal_topics = [doc.get('dominant_topic') for doc in internal_docs if doc.get('dominant_topic')]
        internal_counts = Counter(internal_topics)
        sorted_internal = sorted(internal_counts.items(), key=lambda x: x[1], reverse=True)
        
        for i, (topic, count) in enumerate(sorted_internal, 1):
            percentage = (count / len(internal_topics)) * 100
            print(f"{i:2d}. {topic}: {count} ({percentage:.1f}%)")
        print()
    
    # Analyze External category
    if external_docs:
        print("EXTERNAL CATEGORY:")
        print("-" * 40)
        external_topics = [doc.get('dominant_topic') for doc in external_docs if doc.get('dominant_topic')]
        external_counts = Counter(external_topics)
        sorted_external = sorted(external_counts.items(), key=lambda x: x[1], reverse=True)
        
        for i, (topic, count) in enumerate(sorted_external, 1):
            percentage = (count / len(external_topics)) * 100
            print(f"{i:2d}. {topic}: {count} ({percentage:.1f}%)")
        print()

if __name__ == "__main__":
    try:
        analyze_dominant_topics_with_follow_up()
        analyze_dominant_topics_by_category()
        print("\n" + "="*80)
        print("ANALYSIS COMPLETED SUCCESSFULLY!")
        print("="*80)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Close the connection
        client.close()