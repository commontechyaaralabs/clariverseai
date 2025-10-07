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
# Format: (urgency, category): percentage_with_follow_up_yes
DISTRIBUTION = {
    (True, 'External'): {
        'follow_up_yes_percentage': 4.99 / (4.99 + 0.89),  # 84.87%
        'total_percentage': 4.99 + 0.89  # 5.88%
    },
    (True, 'Internal'): {
        'follow_up_yes_percentage': 2.64 / (2.64 + 0.49),  # 84.35%
        'total_percentage': 2.64 + 0.49  # 3.13%
    },
    (False, 'External'): {
        'follow_up_yes_percentage': 55.73 / (55.73 + 21.10),  # 72.53%
        'total_percentage': 55.73 + 21.10  # 76.83%
    },
    (False, 'Internal'): {
        'follow_up_yes_percentage': 9.83 / (9.83 + 4.29),  # 69.61%
        'total_percentage': 9.83 + 4.29  # 14.12%
    }
}

def add_follow_up_and_action_fields():
    """
    Add follow_up_required and action_required_status fields to tickets
    based on urgency and category distribution
    """
    
    print("=" * 80)
    print("ADDING FOLLOW-UP AND ACTION REQUIRED FIELDS TO TICKETS")
    print("=" * 80)
    
    # Get total count
    total_docs = tickets_collection.count_documents({})
    print(f"\nTotal documents in tickets_new collection: {total_docs}")
    
    if total_docs == 0:
        print("No documents found in collection!")
        return
    
    # Process each urgency-category combination
    total_updated = 0
    stats = []
    
    for (urgency, category), dist_info in DISTRIBUTION.items():
        print(f"\n{'='*80}")
        print(f"Processing: urgency={urgency}, category={category}")
        print(f"{'='*80}")
        
        # Get all documents for this combination
        query = {'urgency': urgency, 'category': category}
        docs = list(tickets_collection.find(query))
        
        if not docs:
            print(f"No documents found for this combination")
            continue
        
        doc_count = len(docs)
        print(f"Found {doc_count} documents")
        
        # Calculate how many should have follow_up_required = "yes"
        follow_up_yes_ratio = dist_info['follow_up_yes_percentage']
        num_follow_up_yes = int(doc_count * follow_up_yes_ratio)
        num_follow_up_no = doc_count - num_follow_up_yes
        
        print(f"Target distribution:")
        print(f"  - follow_up_required: 'yes' → {num_follow_up_yes} documents ({follow_up_yes_ratio*100:.2f}%)")
        print(f"  - follow_up_required: 'no'  → {num_follow_up_no} documents ({(1-follow_up_yes_ratio)*100:.2f}%)")
        
        # Shuffle documents to randomize assignment
        random.shuffle(docs)
        
        # Update documents
        updated_yes = 0
        updated_no = 0
        
        for i, doc in enumerate(docs):
            if i < num_follow_up_yes:
                # Assign follow_up_required = "yes" and action_required_status = "yes"
                tickets_collection.update_one(
                    {'_id': doc['_id']},
                    {'$set': {
                        'follow_up_required': 'yes',
                        'action_required_status': 'yes'
                    }}
                )
                updated_yes += 1
            else:
                # Assign follow_up_required = "no" and action_required_status = "no"
                tickets_collection.update_one(
                    {'_id': doc['_id']},
                    {'$set': {
                        'follow_up_required': 'no',
                        'action_required_status': 'no'
                    }}
                )
                updated_no += 1
        
        total_updated += (updated_yes + updated_no)
        
        print(f"\nUpdated:")
        print(f"  - {updated_yes} documents with follow_up_required='yes', action_required_status='yes'")
        print(f"  - {updated_no} documents with follow_up_required='no', action_required_status='no'")
        
        # Store stats
        stats.append({
            'urgency': urgency,
            'category': category,
            'total': doc_count,
            'follow_up_yes': updated_yes,
            'follow_up_no': updated_no,
            'follow_up_yes_pct': (updated_yes/doc_count*100) if doc_count > 0 else 0,
            'follow_up_no_pct': (updated_no/doc_count*100) if doc_count > 0 else 0
        })
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"\nTotal documents updated: {total_updated} / {total_docs}")
    
    # Verification
    print("\n" + "=" * 80)
    print("VERIFICATION - ACTUAL DISTRIBUTION IN DATABASE")
    print("=" * 80)
    
    print(f"\n{'Urgency':<10} {'Follow-up':<12} {'Category':<12} {'Count':<10} {'Percentage':<12}")
    print("-" * 80)
    
    for urgency in [True, False]:
        for follow_up in ['yes', 'no']:
            for category in ['External', 'Internal']:
                count = tickets_collection.count_documents({
                    'urgency': urgency,
                    'follow_up_required': follow_up,
                    'category': category
                })
                percentage = (count / total_docs * 100) if total_docs > 0 else 0
                print(f"{str(urgency).upper():<10} {follow_up:<12} {category:<12} {count:<10} {percentage:.2f}%")
    
    # Verify action_required_status matches follow_up_required
    print("\n" + "=" * 80)
    print("VERIFICATION - ACTION STATUS CONSISTENCY")
    print("=" * 80)
    
    # Check if all follow_up_required="yes" have action_required_status="yes"
    mismatch = tickets_collection.count_documents({
        'follow_up_required': 'yes',
        'action_required_status': {'$ne': 'yes'}
    })
    
    if mismatch == 0:
        print("✓ All documents with follow_up_required='yes' have action_required_status='yes'")
    else:
        print(f"✗ Found {mismatch} mismatched documents!")
    
    # Check follow_up="no" documents
    follow_up_no_count = tickets_collection.count_documents({'follow_up_required': 'no'})
    action_no_count = tickets_collection.count_documents({
        'follow_up_required': 'no',
        'action_required_status': 'no'
    })
    
    if follow_up_no_count == action_no_count:
        print("✓ All documents with follow_up_required='no' have action_required_status='no'")
    else:
        print(f"✗ Found {follow_up_no_count - action_no_count} mismatched documents!")
    
    # Export stats to CSV
    print("\n" + "=" * 80)
    df_stats = pd.DataFrame(stats)
    csv_filename = f"ticket_follow_up_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_stats.to_csv(csv_filename, index=False)
    print(f"Statistics exported to: {csv_filename}")
    print("=" * 80)

if __name__ == "__main__":
    try:
        # Set random seed for reproducibility (optional)
        random.seed(42)
        
        add_follow_up_and_action_fields()
        print("\nOperation completed successfully!")
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Close the connection
        client.close()
