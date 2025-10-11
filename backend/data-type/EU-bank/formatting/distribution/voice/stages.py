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
voice_new_collection = db['voice_new']

def update_stages_for_resolved_records():
    """
    Update stages field for records with follow_up_required: "no" and action_pending_status: "no"
    Apply random distribution for: Resolved, Close, Report
    """
    
    print("="*80)
    print("UPDATING STAGES FOR RESOLVED RECORDS")
    print("="*80)
    
    # Filter records with follow_up_required: "no" and action_pending_status: "no"
    query = {
        "follow_up_required": "no",
        "action_pending_status": "no"
    }
    
    resolved_docs = list(voice_new_collection.find(query))
    total_resolved = len(resolved_docs)
    
    print(f"Total records with follow_up_required: 'no' and action_pending_status: 'no': {total_resolved}")
    
    if total_resolved == 0:
        print("No resolved records found")
        return
    
    # Define stages for resolved records
    resolved_stages = ["Resolved", "Close", "Report"]
    
    # Shuffle documents for random distribution
    import random
    random.shuffle(resolved_docs)
    
    # Apply random distribution
    stage_counts = {"Resolved": 0, "Close": 0, "Report": 0}
    
    for i, doc in enumerate(resolved_docs):
        # Randomly assign stage
        assigned_stage = random.choice(resolved_stages)
        stage_counts[assigned_stage] += 1
        
        # Update the document
        voice_new_collection.update_one(
            {'_id': doc['_id']},
            {'$set': {'stages': assigned_stage}}
        )
    
    print("Random distribution completed for resolved records:")
    for stage, count in stage_counts.items():
        percentage = (count / total_resolved) * 100
        print(f"  {stage}: {count} records ({percentage:.1f}%)")
    
    return stage_counts

def update_stages_for_pending_records():
    """
    Update stages field for records with follow_up_required: "yes" and action_pending_status: "yes"
    Apply random distribution for: Receive, Authenticate, Categorize, Resolution, Escalation, Update
    """
    
    print("\n" + "="*80)
    print("UPDATING STAGES FOR PENDING RECORDS")
    print("="*80)
    
    # Filter records with follow_up_required: "yes" and action_pending_status: "yes"
    query = {
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    }
    
    pending_docs = list(voice_new_collection.find(query))
    total_pending = len(pending_docs)
    
    print(f"Total records with follow_up_required: 'yes' and action_pending_status: 'yes': {total_pending}")
    
    if total_pending == 0:
        print("No pending records found")
        return
    
    # Define stages for pending records
    pending_stages = ["Receive", "Authenticate", "Categorize", "Resolution", "Escalation", "Update"]
    
    # Shuffle documents for random distribution
    import random
    random.shuffle(pending_docs)
    
    # Apply random distribution
    stage_counts = {stage: 0 for stage in pending_stages}
    
    for i, doc in enumerate(pending_docs):
        # Randomly assign stage
        assigned_stage = random.choice(pending_stages)
        stage_counts[assigned_stage] += 1
        
        # Update the document
        voice_new_collection.update_one(
            {'_id': doc['_id']},
            {'$set': {'stages': assigned_stage}}
        )
    
    print("Random distribution completed for pending records:")
    for stage, count in stage_counts.items():
        percentage = (count / total_pending) * 100
        print(f"  {stage}: {count} records ({percentage:.1f}%)")
    
    return stage_counts

def verify_stages_distribution():
    """
    Verify the stages distribution results
    """
    print("\n" + "="*80)
    print("VERIFICATION - STAGES DISTRIBUTION RESULTS")
    print("="*80)
    
    # Check resolved records
    resolved_query = {
        "follow_up_required": "no",
        "action_pending_status": "no"
    }
    resolved_docs = list(voice_new_collection.find(resolved_query))
    
    print(f"RESOLVED RECORDS (follow_up_required: 'no', action_pending_status: 'no'): {len(resolved_docs)}")
    if resolved_docs:
        resolved_stages = [doc.get('stages') for doc in resolved_docs if doc.get('stages')]
        from collections import Counter
        resolved_counts = Counter(resolved_stages)
        
        for stage, count in sorted(resolved_counts.items()):
            percentage = (count / len(resolved_stages)) * 100
            print(f"  {stage}: {count} ({percentage:.1f}%)")
    
    # Check pending records
    pending_query = {
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    }
    pending_docs = list(voice_new_collection.find(pending_query))
    
    print(f"\nPENDING RECORDS (follow_up_required: 'yes', action_pending_status: 'yes'): {len(pending_docs)}")
    if pending_docs:
        pending_stages = [doc.get('stages') for doc in pending_docs if doc.get('stages')]
        pending_counts = Counter(pending_stages)
        
        for stage, count in sorted(pending_counts.items()):
            percentage = (count / len(pending_stages)) * 100
            print(f"  {stage}: {count} ({percentage:.1f}%)")
    
    # Check records without stages
    all_docs = list(voice_new_collection.find({}))
    docs_without_stages = [doc for doc in all_docs if not doc.get('stages')]
    
    print(f"\nRECORDS WITHOUT STAGES: {len(docs_without_stages)}")
    if docs_without_stages:
        print("These records don't match the specified filter criteria:")
        for doc in docs_without_stages[:5]:  # Show first 5
            follow_up = doc.get('follow_up_required', 'N/A')
            action_status = doc.get('action_pending_status', 'N/A')
            print(f"  follow_up_required: '{follow_up}', action_pending_status: '{action_status}'")
        if len(docs_without_stages) > 5:
            print(f"  ... and {len(docs_without_stages) - 5} more")

def analyze_stages_by_category():
    """
    Analyze stages distribution by category (Internal/External)
    """
    print("\n" + "="*80)
    print("STAGES DISTRIBUTION BY CATEGORY")
    print("="*80)
    
    # Analyze resolved records by category
    resolved_query = {
        "follow_up_required": "no",
        "action_pending_status": "no"
    }
    resolved_docs = list(voice_new_collection.find(resolved_query))
    
    if resolved_docs:
        print("RESOLVED RECORDS BY CATEGORY:")
        print("-" * 40)
        
        internal_resolved = [doc for doc in resolved_docs if doc.get('category') == 'Internal']
        external_resolved = [doc for doc in resolved_docs if doc.get('category') == 'External']
        
        print(f"Internal: {len(internal_resolved)} records")
        if internal_resolved:
            internal_stages = [doc.get('stages') for doc in internal_resolved if doc.get('stages')]
            from collections import Counter
            internal_counts = Counter(internal_stages)
            for stage, count in sorted(internal_counts.items()):
                percentage = (count / len(internal_stages)) * 100
                print(f"  {stage}: {count} ({percentage:.1f}%)")
        
        print(f"\nExternal: {len(external_resolved)} records")
        if external_resolved:
            external_stages = [doc.get('stages') for doc in external_resolved if doc.get('stages')]
            external_counts = Counter(external_stages)
            for stage, count in sorted(external_counts.items()):
                percentage = (count / len(external_stages)) * 100
                print(f"  {stage}: {count} ({percentage:.1f}%)")
    
    # Analyze pending records by category
    pending_query = {
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    }
    pending_docs = list(voice_new_collection.find(pending_query))
    
    if pending_docs:
        print("\nPENDING RECORDS BY CATEGORY:")
        print("-" * 40)
        
        internal_pending = [doc for doc in pending_docs if doc.get('category') == 'Internal']
        external_pending = [doc for doc in pending_docs if doc.get('category') == 'External']
        
        print(f"Internal: {len(internal_pending)} records")
        if internal_pending:
            internal_stages = [doc.get('stages') for doc in internal_pending if doc.get('stages')]
            internal_counts = Counter(internal_stages)
            for stage, count in sorted(internal_counts.items()):
                percentage = (count / len(internal_stages)) * 100
                print(f"  {stage}: {count} ({percentage:.1f}%)")
        
        print(f"\nExternal: {len(external_pending)} records")
        if external_pending:
            external_stages = [doc.get('stages') for doc in external_pending if doc.get('stages')]
            external_counts = Counter(external_stages)
            for stage, count in sorted(external_counts.items()):
                percentage = (count / len(external_stages)) * 100
                print(f"  {stage}: {count} ({percentage:.1f}%)")

if __name__ == "__main__":
    try:
        resolved_counts = update_stages_for_resolved_records()
        pending_counts = update_stages_for_pending_records()
        verify_stages_distribution()
        analyze_stages_by_category()
        print("\n" + "="*80)
        print("STAGES UPDATE COMPLETED SUCCESSFULLY!")
        print("="*80)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Close the connection
        client.close()

