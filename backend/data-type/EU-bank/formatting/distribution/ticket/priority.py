# Import required libraries
from pymongo import MongoClient
import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime
import random

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collection
tickets_collection = db['tickets_new']

# Priority distribution based on the provided data
# Format: (urgency, follow_up_required, action_pending_status, priority): percentage
PRIORITY_DISTRIBUTION = {
    (False, 'yes', 'yes', 'P3-Medium'): 23.5,
    (False, 'no', 'no', 'P5-Very Low'): 21.31,
    (False, 'yes', 'yes', 'P2-High'): 19.46,
    (False, 'yes', 'yes', 'P4-Low'): 14.17,
    (False, 'yes', 'yes', 'P1-Critical'): 8.13,
    (True, 'yes', 'yes', 'P1-Critical'): 4.39,
    (False, 'no', 'no', 'P4-Low'): 4.09,
    (True, 'yes', 'yes', 'P2-High'): 2.5,
    (True, 'yes', 'yes', 'P3-Medium'): 0.75,
    (True, 'no', 'no', 'P2-High'): 0.6,
    (True, 'no', 'no', 'P3-Medium'): 0.6,
    (False, 'yes', 'yes', 'P5-Very Low'): 0.3,
    (True, 'no', 'no', 'P4-Low'): 0.2
}

def rename_action_required_to_pending():
    """
    Rename action_required_status field to action_pending_status in all documents
    Returns: 'skipped', 'success', or 'failed'
    """
    print("=" * 80)
    print("CHECKING FOR action_required_status FIELD")
    print("=" * 80)
    
    # Check if action_required_status exists
    sample_doc = tickets_collection.find_one({'action_required_status': {'$exists': True}})
    if not sample_doc:
        print("ℹ️  No documents found with 'action_required_status' field")
        print("ℹ️  Skipping rename step - proceeding to distribution")
        return 'skipped'
    
    # Count documents with action_required_status
    total_docs = tickets_collection.count_documents({'action_required_status': {'$exists': True}})
    print(f"Found {total_docs} documents with 'action_required_status' field")
    
    if total_docs == 0:
        print("ℹ️  No documents to rename - skipping rename step")
        return 'skipped'
    
    print("Renaming action_required_status to action_pending_status...")
    
    # Rename the field using MongoDB's $rename operator
    result = tickets_collection.update_many(
        {'action_required_status': {'$exists': True}},
        {'$rename': {'action_required_status': 'action_pending_status'}}
    )
    
    print(f"✓ Renamed field in {result.modified_count} documents")
    
    # Verify the rename
    renamed_count = tickets_collection.count_documents({'action_pending_status': {'$exists': True}})
    old_count = tickets_collection.count_documents({'action_required_status': {'$exists': True}})
    
    print(f"Verification:")
    print(f"  - Documents with 'action_pending_status': {renamed_count}")
    print(f"  - Documents with 'action_required_status': {old_count}")
    
    if renamed_count == total_docs and old_count == 0:
        print("✓ Field rename completed successfully!")
        return 'success'
    else:
        print("❌ Field rename may have issues!")
        return 'failed'

def update_priority_distribution():
    """
    Update priority field in tickets based on urgency, follow_up_required, 
    and action_pending_status distribution
    """
    
    print("=" * 80)
    print("UPDATING PRIORITY DISTRIBUTION IN TICKETS")
    print("=" * 80)
    
    # Get total count
    total_docs = tickets_collection.count_documents({})
    print(f"\nTotal documents in tickets_new collection: {total_docs}")
    
    if total_docs == 0:
        print("No documents found in collection!")
        return
    
    # Check what fields exist in the collection
    sample_doc = tickets_collection.find_one()
    if sample_doc:
        print(f"\nSample document fields: {list(sample_doc.keys())}")
        
        # Show sample values for key fields
        key_fields = ['urgency', 'follow_up_required', 'action_pending_status', 'priority']
        print(f"\nSample values:")
        for field in key_fields:
            if field in sample_doc:
                print(f"  {field}: {sample_doc[field]} (type: {type(sample_doc[field])})")
            else:
                print(f"  {field}: NOT FOUND")
        
        # Check if required fields exist
        required_fields = ['urgency', 'follow_up_required', 'action_pending_status']
        missing_fields = [field for field in required_fields if field not in sample_doc]
        
        if missing_fields:
            print(f"\n❌ Missing required fields: {missing_fields}")
            print("You need to run F_a.py first to add follow_up_required and action_pending_status fields!")
            return
        else:
            print("✓ All required fields found")
    else:
        print("No sample document found!")
        return
    
    # First, let's see what combinations actually exist in the database
    print(f"\n{'='*80}")
    print("CHECKING ACTUAL COMBINATIONS IN DATABASE")
    print(f"{'='*80}")
    
    pipeline = [
        {
            '$group': {
                '_id': {
                    'urgency': '$urgency',
                    'follow_up_required': '$follow_up_required',
                    'action_pending_status': '$action_pending_status'
                },
                'count': {'$sum': 1}
            }
        },
        {'$sort': {'count': -1}}
    ]
    
    actual_combinations = list(tickets_collection.aggregate(pipeline))
    print(f"\nFound {len(actual_combinations)} unique combinations:")
    print(f"{'Urgency':<8} {'Follow-up':<12} {'Action':<12} {'Count':<10}")
    print("-" * 50)
    
    for combo in actual_combinations:
        urgency = combo['_id']['urgency']
        follow_up = combo['_id']['follow_up_required']
        action = combo['_id']['action_pending_status']
        count = combo['count']
        print(f"{str(urgency):<8} {follow_up:<12} {action:<12} {count:<10}")
    
    # Process each combination
    total_updated = 0
    stats = []
    
    # Group by urgency, follow_up_required, action_pending_status
    combinations = {}
    for (urgency, follow_up, action, priority), percentage in PRIORITY_DISTRIBUTION.items():
        key = (urgency, follow_up, action)
        if key not in combinations:
            combinations[key] = []
        combinations[key].append((priority, percentage))
    
    for (urgency, follow_up, action), priority_list in combinations.items():
        print(f"\n{'='*80}")
        print(f"Processing: urgency={urgency}, follow_up_required={follow_up}, action_pending_status={action}")
        print(f"{'='*80}")
        
        # Get all documents for this combination
        query = {
            'urgency': urgency, 
            'follow_up_required': follow_up, 
            'action_pending_status': action
        }
        docs = list(tickets_collection.find(query))
        
        if not docs:
            print(f"No documents found for this combination")
            continue
        
        doc_count = len(docs)
        print(f"Found {doc_count} documents")
        
        # Calculate target counts for each priority
        priority_counts = {}
        for priority, percentage in priority_list:
            count = int(doc_count * percentage / 100)
            priority_counts[priority] = count
        
        # Handle remainder
        assigned_count = sum(priority_counts.values())
        remainder = doc_count - assigned_count
        
        if remainder > 0:
            # Assign remainder to the highest percentage priority
            highest_priority = max(priority_list, key=lambda x: x[1])[0]
            priority_counts[highest_priority] += remainder
        
        print(f"Target distribution:")
        for priority, count in priority_counts.items():
            percentage = (count / doc_count * 100) if doc_count > 0 else 0
            print(f"  - {priority}: {count} documents ({percentage:.2f}%)")
        
        # Shuffle documents to randomize assignment
        random.shuffle(docs)
        
        # Update documents
        updated_counts = {}
        doc_index = 0
        
        for priority, target_count in priority_counts.items():
            if target_count > 0:
                for i in range(target_count):
                    if doc_index < len(docs):
                        tickets_collection.update_one(
                            {'_id': docs[doc_index]['_id']},
                            {'$set': {'priority': priority}}
                        )
                        updated_counts[priority] = updated_counts.get(priority, 0) + 1
                        doc_index += 1
        
        total_updated += doc_count
        
        print(f"\nUpdated:")
        for priority, count in updated_counts.items():
            print(f"  - {priority}: {count} documents")
        
        # Store stats
        stats.append({
            'urgency': urgency,
            'follow_up_required': follow_up,
            'action_pending_status': action,
            'total_documents': doc_count,
            **updated_counts
        })
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nTotal documents updated: {total_updated} / {total_docs}")
    
    # Verification
    print("\n" + "=" * 80)
    print("VERIFICATION - ACTUAL PRIORITY DISTRIBUTION")
    print("=" * 80)
    
    print(f"\n{'Urgency':<8} {'Follow-up':<12} {'Action':<12} {'Priority':<15} {'Count':<10} {'Percentage':<12}")
    print("-" * 90)
    
    for urgency in [True, False]:
        for follow_up in ['yes', 'no']:
            for action in ['yes', 'no']:
                for priority in ['P1-Critical', 'P2-High', 'P3-Medium', 'P4-Low', 'P5-Very Low']:
                    count = tickets_collection.count_documents({
                        'urgency': urgency,
                        'follow_up_required': follow_up,
                        'action_pending_status': action,
                        'priority': priority
                    })
                    percentage = (count / total_docs * 100) if total_docs > 0 else 0
                    if count > 0:
                        print(f"{str(urgency).upper():<8} {follow_up:<12} {action:<12} {priority:<15} {count:<10} {percentage:.2f}%")
    
    # Export stats to CSV
    print("\n" + "=" * 80)
    df_stats = pd.DataFrame(stats)
    csv_filename = f"ticket_priority_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_stats.to_csv(csv_filename, index=False)
    print(f"Statistics exported to: {csv_filename}")
    print("=" * 80)

if __name__ == "__main__":
    try:
        # Set random seed for reproducibility (optional)
        random.seed(42)
        
        # Step 1: Check and rename action_required_status to action_pending_status (if needed)
        print("STEP 1: Checking field rename requirement")
        rename_result = rename_action_required_to_pending()
        
        if rename_result == 'failed':
            print("❌ Field rename failed. Exiting...")
            exit(1)
        elif rename_result == 'skipped':
            print("ℹ️  Field rename skipped - proceeding to distribution")
        else:
            print("✓ Field rename completed successfully")
        
        print("\n" + "="*80)
        print("STEP 2: Updating priority distribution")
        print("="*80)
        
        # Step 2: Update priority distribution
        update_priority_distribution()
        
        print("\n✓ All operations completed successfully!")
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Close the connection
        client.close()
