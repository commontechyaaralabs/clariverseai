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

def distribute_follow_up_and_action_status():
    """
    Distribute follow_up_required and action_pending_status based on urgency and category combinations
    according to the specified distribution table
    """
    
    # Distribution configuration based on the table
    distributions = {
        # Urgent T, Internal: 43 records total
        # 36 records (83.7%) follow_up_required: "yes", action_pending_status: "yes"
        # 7 records (16.3%) follow_up_required: "no", action_pending_status: "no"
        ("urgent_true", "internal"): {
            "total": 43,
            "follow_up_yes": 36,
            "follow_up_no": 7,
            "action_yes": 36,
            "action_no": 7
        },
        
        # Urgent T, External: 5 records total
        # 4 records (80.0%) follow_up_required: "yes", action_pending_status: "yes"
        # 1 record (20.0%) follow_up_required: "no", action_pending_status: "no"
        ("urgent_true", "external"): {
            "total": 5,
            "follow_up_yes": 4,
            "follow_up_no": 1,
            "action_yes": 4,
            "action_no": 1
        },
        
        # Urgent F, Internal: 459 records total
        # 369 records (80.4%) follow_up_required: "yes", action_pending_status: "yes"
        # 90 records (19.6%) follow_up_required: "no", action_pending_status: "no"
        ("urgent_false", "internal"): {
            "total": 459,
            "follow_up_yes": 369,
            "follow_up_no": 90,
            "action_yes": 369,
            "action_no": 90
        },
        
        # Urgent F, External: 93 records total
        # 75 records (80.6%) follow_up_required: "yes", action_pending_status: "yes"
        # 18 records (19.4%) follow_up_required: "no", action_pending_status: "no"
        ("urgent_false", "external"): {
            "total": 93,
            "follow_up_yes": 75,
            "follow_up_no": 18,
            "action_yes": 75,
            "action_no": 18
        }
    }
    
    print("Starting distribution process...")
    
    for (urgency_key, category_key), config in distributions.items():
        print(f"\nProcessing {urgency_key}, {category_key} combination...")
        
        # Determine filter criteria
        urgency_value = urgency_key == "urgent_true"
        category_value = "Internal" if category_key == "internal" else "External"
        
        # Get documents matching the criteria
        query = {
            "urgency": urgency_value,
            "category": category_value
        }
        
        documents = list(chat_new_collection.find(query))
        actual_count = len(documents)
        
        print(f"Found {actual_count} documents (expected: {config['total']})")
        
        if actual_count != config['total']:
            print(f"Warning: Document count mismatch for {urgency_key}, {category_key}")
            print(f"Expected: {config['total']}, Found: {actual_count}")
            continue
        
        # Shuffle documents to ensure random distribution
        import random
        random.shuffle(documents)
        
        # Apply follow_up_required: "yes" and action_pending_status: "yes" to first N documents
        follow_up_yes_count = config['follow_up_yes']
        for i in range(follow_up_yes_count):
            doc_id = documents[i]['_id']
            chat_new_collection.update_one(
                {'_id': doc_id},
                {
                    '$set': {
                        'follow_up_required': 'yes',
                        'action_pending_status': 'yes'
                    }
                }
            )
        
        # Apply follow_up_required: "no" and action_pending_status: "no" to remaining documents
        follow_up_no_count = config['follow_up_no']
        for i in range(follow_up_yes_count, follow_up_yes_count + follow_up_no_count):
            doc_id = documents[i]['_id']
            chat_new_collection.update_one(
                {'_id': doc_id},
                {
                    '$set': {
                        'follow_up_required': 'no',
                        'action_pending_status': 'no'
                    }
                }
            )
        
        print(f"Updated {follow_up_yes_count} documents with follow_up_required: 'yes', action_pending_status: 'yes'")
        print(f"Updated {follow_up_no_count} documents with follow_up_required: 'no', action_pending_status: 'no'")

def verify_distribution():
    """
    Verify the distribution results and print statistics
    """
    print("\n" + "="*80)
    print("FINAL VERIFICATION")
    print("="*80)
    
    # Define combinations to check
    combinations = [
        ("Urgent T, Internal", {"urgency": True, "category": "Internal"}),
        ("Urgent T, External", {"urgency": True, "category": "External"}),
        ("Urgent F, Internal", {"urgency": False, "category": "Internal"}),
        ("Urgent F, External", {"urgency": False, "category": "External"})
    ]
    
    total_follow_up_yes = 0
    total_follow_up_no = 0
    total_action_yes = 0
    total_action_no = 0
    grand_total = 0
    
    for combo_name, query in combinations:
        # Get all documents for this combination
        docs = list(chat_new_collection.find(query))
        total_count = len(docs)
        
        # Count follow_up_required
        follow_up_yes = len([d for d in docs if d.get('follow_up_required') == 'yes'])
        follow_up_no = len([d for d in docs if d.get('follow_up_required') == 'no'])
        
        # Count action_pending_status
        action_yes = len([d for d in docs if d.get('action_pending_status') == 'yes'])
        action_no = len([d for d in docs if d.get('action_pending_status') == 'no'])
        
        # Calculate percentages
        follow_up_yes_pct = (follow_up_yes / total_count * 100) if total_count > 0 else 0
        follow_up_no_pct = (follow_up_no / total_count * 100) if total_count > 0 else 0
        action_yes_pct = (action_yes / total_count * 100) if total_count > 0 else 0
        action_no_pct = (action_no / total_count * 100) if total_count > 0 else 0
        total_pct = (total_count / 600 * 100) if total_count > 0 else 0
        
        print(f"\n{combo_name}:")
        print(f"  Follow-up Yes: {follow_up_yes} ({follow_up_yes_pct:.1f}%)")
        print(f"  Follow-up No: {follow_up_no} ({follow_up_no_pct:.1f}%)")
        print(f"  Action Yes: {action_yes} ({action_yes_pct:.1f}%)")
        print(f"  Action No: {action_no} ({action_no_pct:.1f}%)")
        print(f"  Total: {total_count} ({total_pct:.1f}%)")
        
        # Add to grand totals
        total_follow_up_yes += follow_up_yes
        total_follow_up_no += follow_up_no
        total_action_yes += action_yes
        total_action_no += action_no
        grand_total += total_count
    
    # Print grand total
    print(f"\nGrand Total:")
    print(f"  Follow-up Yes: {total_follow_up_yes} ({total_follow_up_yes/grand_total*100:.1f}%)")
    print(f"  Follow-up No: {total_follow_up_no} ({total_follow_up_no/grand_total*100:.1f}%)")
    print(f"  Action Yes: {total_action_yes} ({total_action_yes/grand_total*100:.1f}%)")
    print(f"  Action No: {total_action_no} ({total_action_no/grand_total*100:.1f}%)")
    print(f"  Total: {grand_total} (100.0%)")

if __name__ == "__main__":
    try:
        distribute_follow_up_and_action_status()
        verify_distribution()
        print("\nOperation completed successfully!")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Close the connection
        client.close()