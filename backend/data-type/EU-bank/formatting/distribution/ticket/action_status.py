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

# Distribution based on the provided data
# Format: (urgency, follow_up_required, action_pending_status, priority): {action_pending_from: percentage}
ACTION_PENDING_FROM_DISTRIBUTION = {
    (False, 'yes', 'yes', 'P2-High'): {'company': 14.32, 'customer': 5.14},
    (False, 'yes', 'yes', 'P3-Medium'): {'company': 12.28, 'customer': 11.23},
    (False, 'yes', 'yes', 'P4-Low'): {'company': 7.53, 'customer': 6.64},
    (False, 'yes', 'yes', 'P1-Critical'): {'company': 4.49, 'customer': 3.64},
    (False, 'yes', 'yes', 'P5-Very Low'): {'company': 0.2, 'customer': 0.1},
    (True, 'yes', 'yes', 'P1-Critical'): {'company': 2.45, 'customer': 1.95},
    (True, 'yes', 'yes', 'P2-High'): {'company': 1.45, 'customer': 1.05},
    (True, 'yes', 'yes', 'P3-Medium'): {'company': 0.2, 'customer': 0.55}
}

def add_action_pending_from_field():
    """
    Add action_pending_from field to tickets based on urgency, follow_up_required, 
    action_pending_status, and priority distribution
    """
    
    print("=" * 80)
    print("ADDING ACTION_PENDING_FROM FIELD TO TICKETS")
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
    
    # Process each combination
    total_updated = 0
    stats = []
    
    # First, handle records with follow_up_required="no" and action_pending_status="no" -> set to null
    print(f"\n{'='*80}")
    print("PROCESSING RECORDS WITH follow_up_required='no' AND action_pending_status='no'")
    print(f"{'='*80}")
    
    no_action_query = {
        'follow_up_required': 'no',
        'action_pending_status': 'no'
    }
    
    no_action_docs = list(tickets_collection.find(no_action_query))
    no_action_count = len(no_action_docs)
    print(f"Found {no_action_count} records with follow_up_required='no' and action_pending_status='no'")
    
    if no_action_count > 0:
        # Set action_pending_from to null for these records
        result = tickets_collection.update_many(
            no_action_query,
            {'$set': {'action_pending_from': None}}
        )
        print(f"✓ Set action_pending_from to null for {result.modified_count} records")
        total_updated += result.modified_count
    
    # Process records with follow_up_required="yes" and action_pending_status="yes"
    print(f"\n{'='*80}")
    print("PROCESSING RECORDS WITH follow_up_required='yes' AND action_pending_status='yes'")
    print(f"{'='*80}")
    
    for (urgency, follow_up, action, priority), distribution in ACTION_PENDING_FROM_DISTRIBUTION.items():
        print(f"\n{'='*60}")
        print(f"Processing: urgency={urgency}, follow_up_required={follow_up}, action_pending_status={action}, priority={priority}")
        print(f"{'='*60}")
        
        # Get all documents for this combination
        query = {
            'urgency': urgency,
            'follow_up_required': follow_up,
            'action_pending_status': action,
            'priority': priority
        }
        docs = list(tickets_collection.find(query))
        
        if not docs:
            print(f"No documents found for this combination")
            continue
        
        doc_count = len(docs)
        print(f"Found {doc_count} documents")
        
        # Calculate target counts for each action_pending_from value
        action_from_counts = {}
        for action_from, percentage in distribution.items():
            count = int(doc_count * percentage / 100)
            action_from_counts[action_from] = count
        
        # Handle remainder
        assigned_count = sum(action_from_counts.values())
        remainder = doc_count - assigned_count
        
        if remainder > 0:
            # Assign remainder to the highest percentage action_pending_from
            highest_action_from = max(distribution.items(), key=lambda x: x[1])[0]
            action_from_counts[highest_action_from] += remainder
        
        print(f"Target distribution:")
        for action_from, count in action_from_counts.items():
            percentage = (count / doc_count * 100) if doc_count > 0 else 0
            print(f"  - {action_from}: {count} documents ({percentage:.2f}%)")
        
        # Shuffle documents to randomize assignment
        random.shuffle(docs)
        
        # Update documents
        updated_counts = {}
        doc_index = 0
        
        for action_from, target_count in action_from_counts.items():
            if target_count > 0:
                for i in range(target_count):
                    if doc_index < len(docs):
                        tickets_collection.update_one(
                            {'_id': docs[doc_index]['_id']},
                            {'$set': {'action_pending_from': action_from}}
                        )
                        updated_counts[action_from] = updated_counts.get(action_from, 0) + 1
                        doc_index += 1
        
        total_updated += doc_count
        
        print(f"\nUpdated:")
        for action_from, count in updated_counts.items():
            print(f"  - {action_from}: {count} documents")
        
        # Store stats
        stats.append({
            'urgency': urgency,
            'follow_up_required': follow_up,
            'action_pending_status': action,
            'priority': priority,
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
    print("VERIFICATION - ACTION_PENDING_FROM DISTRIBUTION")
    print("=" * 80)
    
    print(f"\n{'Urgency':<8} {'Follow-up':<12} {'Action':<12} {'Priority':<15} {'Action_From':<15} {'Count':<10} {'Percentage':<12}")
    print("-" * 100)
    
    for urgency in [True, False]:
        for follow_up in ['yes', 'no']:
            for action in ['yes', 'no']:
                for priority in ['P1-Critical', 'P2-High', 'P3-Medium', 'P4-Low', 'P5-Very Low']:
                    for action_from in ['company', 'customer', None]:
                        count = tickets_collection.count_documents({
                            'urgency': urgency,
                            'follow_up_required': follow_up,
                            'action_pending_status': action,
                            'priority': priority,
                            'action_pending_from': action_from
                        })
                        percentage = (count / total_docs * 100) if total_docs > 0 else 0
                        if count > 0:
                            action_from_str = str(action_from) if action_from is not None else 'null'
                            print(f"{str(urgency).upper():<8} {follow_up:<12} {action:<12} {priority:<15} {action_from_str:<15} {count:<10} {percentage:.2f}%")
    
    # Export stats to CSV
    print("\n" + "=" * 80)
    df_stats = pd.DataFrame(stats)
    csv_filename = f"ticket_action_pending_from_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_stats.to_csv(csv_filename, index=False)
    print(f"Statistics exported to: {csv_filename}")
    print("=" * 80)

if __name__ == "__main__":
    try:
        # Set random seed for reproducibility (optional)
        random.seed(42)
        
        add_action_pending_from_field()
        print("\n✅ Operation completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Close the connection
        client.close()