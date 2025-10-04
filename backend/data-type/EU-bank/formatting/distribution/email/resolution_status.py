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

def update_resolution_status():
    """
    Update resolution_status field based on stages field
    - Records with stages "Resolved | Close | Report" -> resolution_status: "closed"
    - Remaining records -> resolution_status: "open" or "inprogress" (randomly distributed)
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # First, remove all existing resolution_status fields
        print("Removing all existing resolution_status fields...")
        remove_status_result = collection.update_many(
            {},
            {'$unset': {'resolution_status': ""}}
        )
        print(f"Removed resolution_status field from {remove_status_result.modified_count} records")
        
        # Step 1: Handle records with stages "Resolved | Close | Report" -> "closed"
        print("\n=== ASSIGNING 'closed' STATUS ===")
        closed_stages = ["Resolved", "Close", "Report"]
        
        closed_query = {'stages': {'$in': closed_stages}}
        closed_docs = list(collection.find(closed_query, {'_id': 1, 'stages': 1}))
        closed_count = len(closed_docs)
        
        print(f"Found {closed_count} records with stages: {closed_stages}")
        
        if closed_count > 0:
            closed_ids = [doc['_id'] for doc in closed_docs]
            closed_result = collection.update_many(
                {'_id': {'$in': closed_ids}},
                {'$set': {'resolution_status': 'closed'}}
            )
            print(f"Updated {closed_result.modified_count} records to resolution_status: 'closed'")
        
        # Step 2: Handle remaining records -> "open" or "inprogress" (random distribution)
        print("\n=== ASSIGNING 'open' AND 'inprogress' STATUS ===")
        
        # Get remaining records (those without resolution_status)
        remaining_docs = list(collection.find(
            {'resolution_status': {'$exists': False}},
            {'_id': 1}
        ))
        
        remaining_count = len(remaining_docs)
        print(f"Found {remaining_count} remaining records to assign 'open' or 'inprogress'")
        
        if remaining_count > 0:
            # Random distribution: 60% open, 40% inprogress
            open_count = int(remaining_count * 0.6)
            inprogress_count = remaining_count - open_count
            
            print(f"Distribution: {open_count} records -> 'open', {inprogress_count} records -> 'inprogress'")
            
            # Shuffle and split the remaining documents
            random.shuffle(remaining_docs)
            
            # Assign "open" status
            if open_count > 0:
                open_docs = remaining_docs[:open_count]
                open_ids = [doc['_id'] for doc in open_docs]
                
                open_result = collection.update_many(
                    {'_id': {'$in': open_ids}},
                    {'$set': {'resolution_status': 'open'}}
                )
                print(f"Updated {open_result.modified_count} records to resolution_status: 'open'")
            
            # Assign "inprogress" status
            if inprogress_count > 0:
                inprogress_docs = remaining_docs[open_count:]
                inprogress_ids = [doc['_id'] for doc in inprogress_docs]
                
                inprogress_result = collection.update_many(
                    {'_id': {'$in': inprogress_ids}},
                    {'$set': {'resolution_status': 'inprogress'}}
                )
                print(f"Updated {inprogress_result.modified_count} records to resolution_status: 'inprogress'")
        
        # Verification
        print(f"\n=== VERIFICATION ===")
        total_docs = collection.count_documents({})
        docs_with_status = collection.count_documents({'resolution_status': {'$exists': True}})
        docs_without_status = collection.count_documents({'resolution_status': {'$exists': False}})
        
        print(f"Total documents: {total_docs}")
        print(f"Documents with resolution_status field: {docs_with_status}")
        print(f"Documents without resolution_status field: {docs_without_status}")
        
        # Count by status
        status_counts = {}
        for status in ["closed", "open", "inprogress"]:
            count = collection.count_documents({'resolution_status': status})
            status_counts[status] = count
            print(f"Documents with resolution_status '{status}': {count}")
        
        # Show sample documents by status
        print(f"\n=== SAMPLE DOCUMENTS BY STATUS ===")
        for status in ["closed", "open", "inprogress"]:
            sample_docs = collection.find(
                {'resolution_status': status},
                {'stages': 1, 'resolution_status': 1, 'subject': 1}
            ).limit(3)
            
            docs_list = list(sample_docs)
            if docs_list:
                print(f"\n{status.upper()} samples:")
                for doc in docs_list:
                    stages = doc.get('stages', 'MISSING')
                    subject = doc.get('subject', 'MISSING')[:50]
                    print(f"  stages: {stages} | subject: {subject}...")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'status_counts': status_counts,
            'docs_without_status': docs_without_status
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting resolution status assignment process...\n")
    
    # Display status structure
    status_structure = {
        "closed": "Records with stages: Resolved | Close | Report",
        "open": "Remaining records (60% random distribution)",
        "inprogress": "Remaining records (40% random distribution)"
    }
    
    for status, description in status_structure.items():
        print(f"{status.upper()}: {description}")
    
    print(f"\nProcess:")
    print("1. Remove all existing resolution_status fields")
    print("2. Assign 'closed' to records with stages: Resolved | Close | Report")
    print("3. Randomly distribute remaining records between 'open' and 'inprogress'")
    print()
    
    result = update_resolution_status()
    
    if result:
        print(f"\nProcess completed successfully!")
        print(f"Total documents: {result['total_documents']}")
        print(f"Status distribution:")
        for status, count in result['status_counts'].items():
            print(f"  {status}: {count} records")
        print(f"Documents without status: {result['docs_without_status']} records")
    else:
        print("\nProcess failed!")