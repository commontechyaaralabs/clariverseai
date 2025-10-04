# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def debug_unmatched_documents():
    """
    Debug script to identify documents that weren't assigned priority levels
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # Get total document count
        total_docs = collection.count_documents({})
        docs_with_priority = collection.count_documents({'priority': {'$exists': True}})
        docs_without_priority = collection.count_documents({'priority': {'$exists': False}})
        
        print(f"\n=== DOCUMENT COUNTS ===")
        print(f"Total documents: {total_docs}")
        print(f"Documents with priority field: {docs_with_priority}")
        print(f"Documents without priority field: {docs_without_priority}")
        print(f"Unmatched documents: {total_docs - docs_with_priority}")
        
        # Find documents without dominant_topic or with invalid topics
        docs_without_topic = collection.count_documents({'dominant_topic': {'$exists': False}})
        docs_with_null_topic = collection.count_documents({'dominant_topic': None})
        docs_with_empty_topic = collection.count_documents({'dominant_topic': ''})
        
        print(f"\n=== DOMINANT_TOPIC ANALYSIS ===")
        print(f"Documents without dominant_topic field: {docs_without_topic}")
        print(f"Documents with null dominant_topic: {docs_with_null_topic}")
        print(f"Documents with empty dominant_topic: {docs_with_empty_topic}")
        
        # Show documents that don't have priority (if any)
        if docs_without_priority > 0:
            print(f"\n=== DOCUMENTS WITHOUT PRIORITY ===")
            unmatched_docs = collection.find(
                {'priority': {'$exists': False}},
                {'dominant_topic': 1, '_id': 1, 'subject': 1}
            ).limit(20)
            
            for i, doc in enumerate(unmatched_docs, 1):
                print(f"{i}. ID: {doc['_id']}")
                print(f"   dominant_topic: {doc.get('dominant_topic', 'MISSING')}")
                print(f"   subject: {doc.get('subject', 'MISSING')[:100]}...")
                print()
        
        # Show documents without valid dominant_topic (if any)
        if docs_without_topic > 0 or docs_with_null_topic > 0 or docs_with_empty_topic > 0:
            print(f"\n=== DOCUMENTS WITHOUT VALID DOMINANT_TOPIC ===")
            invalid_topic_docs = collection.find(
                {'$or': [
                    {'dominant_topic': {'$exists': False}},
                    {'dominant_topic': None},
                    {'dominant_topic': ''}
                ]},
                {'dominant_topic': 1, '_id': 1, 'subject': 1}
            ).limit(20)
            
            for i, doc in enumerate(invalid_topic_docs, 1):
                print(f"{i}. ID: {doc['_id']}")
                print(f"   dominant_topic: {doc.get('dominant_topic', 'MISSING')}")
                print(f"   subject: {doc.get('subject', 'MISSING')[:100]}...")
                print()
        
        # Check for documents with priority but no dominant_topic
        docs_with_priority_no_topic = collection.count_documents({
            'priority': {'$exists': True},
            '$or': [
                {'dominant_topic': {'$exists': False}},
                {'dominant_topic': None},
                {'dominant_topic': ''}
            ]
        })
        
        if docs_with_priority_no_topic > 0:
            print(f"\n=== DOCUMENTS WITH PRIORITY BUT NO DOMINANT_TOPIC ===")
            print(f"Count: {docs_with_priority_no_topic}")
            
            priority_no_topic_docs = collection.find(
                {
                    'priority': {'$exists': True},
                    '$or': [
                        {'dominant_topic': {'$exists': False}},
                        {'dominant_topic': None},
                        {'dominant_topic': ''}
                    ]
                },
                {'dominant_topic': 1, '_id': 1, 'priority': 1, 'subject': 1}
            ).limit(10)
            
            for i, doc in enumerate(priority_no_topic_docs, 1):
                print(f"{i}. ID: {doc['_id']}")
                print(f"   priority: {doc.get('priority', 'MISSING')}")
                print(f"   dominant_topic: {doc.get('dominant_topic', 'MISSING')}")
                print(f"   subject: {doc.get('subject', 'MISSING')[:100]}...")
                print()
        
        # Summary of all priority levels
        print(f"\n=== PRIORITY DISTRIBUTION SUMMARY ===")
        for level in ["P1", "P2", "P3", "P4", "P5"]:
            count = collection.count_documents({'priority': level})
            print(f"Documents with priority {level}: {count}")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'docs_with_priority': docs_with_priority,
            'docs_without_priority': docs_without_priority,
            'unmatched_count': total_docs - docs_with_priority
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting debug analysis for unmatched documents...\n")
    
    result = debug_unmatched_documents()
    
    if result:
        print(f"\n=== FINAL SUMMARY ===")
        print(f"Total documents: {result['total_documents']}")
        print(f"Documents with priority: {result['docs_with_priority']}")
        print(f"Documents without priority: {result['docs_without_priority']}")
        print(f"Unmatched documents: {result['unmatched_count']}")
        
        if result['unmatched_count'] > 0:
            print(f"\n⚠️  Found {result['unmatched_count']} documents that weren't assigned priority!")
            print("Check the detailed output above to see what's wrong with these documents.")
        else:
            print("\n✅ All documents have been assigned priority levels!")
    else:
        print("\n❌ Debug analysis failed!")
