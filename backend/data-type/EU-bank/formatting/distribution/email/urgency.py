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

def update_urgency_field():
    """
    Update urgency field based on specific dominant_topic matches and random selection
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # Define specific topics and their required urgent record counts
        urgent_topics = {
            "Data Breach Warning": 12,
            "Cybersecurity Incident Report": 9,
            "Security Incident Alert": 6,
            "System Outage Notification": 8,
            "Technology Emergency": 11,
            "Risk Management Alert": 16,
            "Sanctions Screening Alert": 10,
            "Covenant Breach Alert": 9,
            "Executive Escalation Email": 20,
            "Legal Escalation": 22,
            "Compliance Monitoring Alert": 8,
            "Quality Assurance Breach": 14,
            "Customer Service Escalation": 16,
            "Payment Service Problem": 8,
            "Processing Delay Complaint": 7,
            "Clearing House Problem": 5
        }
        
        # First, set all records to urgency: false
        print("Setting all records to urgency: false...")
        all_false_result = collection.update_many(
            {},
            {'$set': {'urgency': False}}
        )
        print(f"Updated {all_false_result.modified_count} records to urgency: false")
        
        total_urgent_from_topics = 0
        updated_topics = []
        
        # Update specific topics with urgency: true
        print(f"\nUpdating specific topics with urgency: true...")
        for topic, required_count in urgent_topics.items():
            # Get all documents with this dominant_topic
            docs_with_topic = list(collection.find(
                {'dominant_topic': topic},
                {'_id': 1}
            ))
            
            available_count = len(docs_with_topic)
            print(f"\nTopic: '{topic}'")
            print(f"  Available records: {available_count}")
            print(f"  Required urgent records: {required_count}")
            
            if available_count >= required_count:
                # Randomly select the required number of documents
                random.shuffle(docs_with_topic)
                selected_docs = docs_with_topic[:required_count]
                selected_ids = [doc['_id'] for doc in selected_docs]
                
                # Update selected documents to urgency: true
                result = collection.update_many(
                    {'_id': {'$in': selected_ids}},
                    {'$set': {'urgency': True}}
                )
                
                print(f"  Updated {result.modified_count} records to urgency: true")
                total_urgent_from_topics += result.modified_count
                updated_topics.append(topic)
            else:
                print(f"  ⚠ Warning: Only {available_count} records available, but {required_count} required")
                if available_count > 0:
                    # Update all available records
                    doc_ids = [doc['_id'] for doc in docs_with_topic]
                    result = collection.update_many(
                        {'_id': {'$in': doc_ids}},
                        {'$set': {'urgency': True}}
                    )
                    print(f"  Updated all {result.modified_count} available records to urgency: true")
                    total_urgent_from_topics += result.modified_count
                    updated_topics.append(topic)
        
        print(f"\nTotal urgent records from specific topics: {total_urgent_from_topics}")
        
        # Verification
        print(f"\n=== VERIFICATION ===")
        total_urgent = collection.count_documents({'urgency': True})
        total_false = collection.count_documents({'urgency': False})
        total_docs = collection.count_documents({})
        
        print(f"Total documents: {total_docs}")
        print(f"Documents with urgency: true: {total_urgent}")
        print(f"Documents with urgency: false: {total_false}")
        
        # Show breakdown by topic
        print(f"\nUrgent records by topic:")
        for topic in urgent_topics.keys():
            urgent_count = collection.count_documents({
                'dominant_topic': topic,
                'urgency': True
            })
            total_count = collection.count_documents({'dominant_topic': topic})
            print(f"  {topic}: {urgent_count}/{total_count} urgent")
        
        # Show sample urgent documents
        print(f"\nSample urgent documents:")
        sample_docs = collection.find(
            {'urgency': True},
            {'dominant_topic': 1, 'urgency': 1}
        ).limit(10)
        
        for doc in sample_docs:
            print(f"  {doc['dominant_topic']}: urgency = {doc['urgency']}")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'urgent_documents': total_urgent,
            'false_documents': total_false,
            'urgent_from_topics': total_urgent_from_topics,
            'updated_topics': updated_topics
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting urgency field update process...\n")
    print("Specific topics to be marked as urgent:")
    urgent_topics = {
        "Data Breach Warning": 12,
        "Cybersecurity Incident Report": 9,
        "Security Incident Alert": 6,
        "System Outage Notification": 8,
        "Technology Emergency": 11,
        "Risk Management Alert": 16,
        "Sanctions Screening Alert": 10,
        "Covenant Breach Alert": 9,
        "Executive Escalation Email": 20,
        "Legal Escalation": 22,
        "Compliance Monitoring Alert": 8,
        "Quality Assurance Breach": 14,
        "Customer Service Escalation": 16,
        "Payment Service Problem": 8,
        "Processing Delay Complaint": 7,
        "Clearing House Problem": 5
    }
    
    for topic, count in urgent_topics.items():
        print(f"  {topic}: {count} records")
    
    print(f"\nAll other records will be set to urgency: false")
    print()
    
    result = update_urgency_field()
    
    if result:
        print(f"\nProcess completed successfully!")
        print(f"Total urgent documents: {result['urgent_documents']}")
        print(f"Total false documents: {result['false_documents']}")
    else:
        print("\nProcess failed!")