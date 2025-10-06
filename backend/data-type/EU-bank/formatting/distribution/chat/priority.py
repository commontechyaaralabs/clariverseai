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

def distribute_priorities():
    """
    Distribute priorities across the 4 categories based on the specified table
    """
    
    print("="*100)
    print("CHAT_NEW COLLECTION PRIORITY DISTRIBUTION (600 records)")
    print("="*100)
    
    # Define the priority distribution for each category
    priority_distributions = {
        # Category 1: Urgency: True, Follow-up: Yes, Action: Yes (40 records)
        "category_1": {
            "total": 40,
            "filters": {
                "urgency": True,
                "follow_up_required": "yes",
                "action_pending_status": "yes"
            },
            "priorities": {
                "P1-Critical": 23,  # 57.5%
                "P2-High": 13,      # 32.5%
                "P3-Medium": 4,     # 10.0%
                "P4-Low": 0,        # 0%
                "P5-Very Low": 0    # 0%
            }
        },
        
        # Category 2: Urgency: True, Follow-up: No, Action: No (8 records)
        "category_2": {
            "total": 8,
            "filters": {
                "urgency": True,
                "follow_up_required": "no",
                "action_pending_status": "no"
            },
            "priorities": {
                "P1-Critical": 0,    # 0%
                "P2-High": 3,        # 37.5%
                "P3-Medium": 4,      # 50.0%
                "P4-Low": 1,         # 12.5%
                "P5-Very Low": 0     # 0%
            }
        },
        
        # Category 3: Urgency: False, Follow-up: Yes, Action: Yes (444 records)
        "category_3": {
            "total": 444,
            "filters": {
                "urgency": False,
                "follow_up_required": "yes",
                "action_pending_status": "yes"
            },
            "priorities": {
                "P1-Critical": 55,   # 12.4%
                "P2-High": 132,      # 29.7%
                "P3-Medium": 159,    # 35.8%
                "P4-Low": 96,        # 21.6%
                "P5-Very Low": 2     # 0.5%
            }
        },
        
        # Category 4: Urgency: False, Follow-up: No, Action: No (108 records)
        "category_4": {
            "total": 108,
            "filters": {
                "urgency": False,
                "follow_up_required": "no",
                "action_pending_status": "no"
            },
            "priorities": {
                "P1-Critical": 0,    # 0%
                "P2-High": 0,        # 0%
                "P3-Medium": 0,      # 0%
                "P4-Low": 17,        # 15.7%
                "P5-Very Low": 91    # 84.3%
            }
        }
    }
    
    print("Starting priority distribution process...")
    
    for category_key, config in priority_distributions.items():
        category_num = category_key.split("_")[1]
        filters = config["filters"]
        total_records = config["total"]
        priorities = config["priorities"]
        
        print(f"\nProcessing Category {category_num}:")
        print(f"  Filters: urgency={filters['urgency']}, follow_up_required='{filters['follow_up_required']}', action_pending_status='{filters['action_pending_status']}'")
        print(f"  Total records: {total_records}")
        
        # Get documents matching this category
        documents = list(chat_new_collection.find(filters))
        actual_count = len(documents)
        
        print(f"  Found {actual_count} documents (expected: {total_records})")
        
        if actual_count != total_records:
            print(f"  Warning: Document count mismatch for Category {category_num}")
            print(f"  Expected: {total_records}, Found: {actual_count}")
            continue
        
        # Shuffle documents to ensure random distribution
        import random
        random.shuffle(documents)
        
        # Apply priority distribution
        start_index = 0
        for priority, count in priorities.items():
            if count > 0:
                end_index = start_index + count
                for i in range(start_index, end_index):
                    doc_id = documents[i]['_id']
                    chat_new_collection.update_one(
                        {'_id': doc_id},
                        {'$set': {'priority': priority}}
                    )
                print(f"  Applied {priority}: {count} records")
                start_index = end_index
        
        print(f"  Category {category_num} completed")
    
    print("\nPriority distribution completed!")

def verify_priority_distribution():
    """
    Verify the priority distribution results
    """
    print("\n" + "="*100)
    print("VERIFICATION - PRIORITY DISTRIBUTION RESULTS")
    print("="*100)
    
    # Define categories for verification
    categories = [
        ("Category 1", {"urgency": True, "follow_up_required": "yes", "action_pending_status": "yes"}),
        ("Category 2", {"urgency": True, "follow_up_required": "no", "action_pending_status": "no"}),
        ("Category 3", {"urgency": False, "follow_up_required": "yes", "action_pending_status": "yes"}),
        ("Category 4", {"urgency": False, "follow_up_required": "no", "action_pending_status": "no"})
    ]
    
    total_p1 = 0
    total_p2 = 0
    total_p3 = 0
    total_p4 = 0
    total_p5 = 0
    grand_total = 0
    
    for category_name, filters in categories:
        docs = list(chat_new_collection.find(filters))
        total_count = len(docs)
        
        # Count priorities
        p1_count = len([d for d in docs if d.get('priority') == 'P1-Critical'])
        p2_count = len([d for d in docs if d.get('priority') == 'P2-High'])
        p3_count = len([d for d in docs if d.get('priority') == 'P3-Medium'])
        p4_count = len([d for d in docs if d.get('priority') == 'P4-Low'])
        p5_count = len([d for d in docs if d.get('priority') == 'P5-Very Low'])
        
        # Calculate percentages
        p1_pct = (p1_count / total_count * 100) if total_count > 0 else 0
        p2_pct = (p2_count / total_count * 100) if total_count > 0 else 0
        p3_pct = (p3_count / total_count * 100) if total_count > 0 else 0
        p4_pct = (p4_count / total_count * 100) if total_count > 0 else 0
        p5_pct = (p5_count / total_count * 100) if total_count > 0 else 0
        total_pct = (total_count / 600 * 100) if total_count > 0 else 0
        
        print(f"\n{category_name} (Total: {total_count} records, {total_pct:.1f}% of 600):")
        print(f"  P1-Critical: {p1_count} ({p1_pct:.1f}%)")
        print(f"  P2-High: {p2_count} ({p2_pct:.1f}%)")
        print(f"  P3-Medium: {p3_count} ({p3_pct:.1f}%)")
        print(f"  P4-Low: {p4_count} ({p4_pct:.1f}%)")
        print(f"  P5-Very Low: {p5_count} ({p5_pct:.1f}%)")
        
        # Add to totals
        total_p1 += p1_count
        total_p2 += p2_count
        total_p3 += p3_count
        total_p4 += p4_count
        total_p5 += p5_count
        grand_total += total_count
    
    # Print grand totals
    print(f"\n{'='*50}")
    print("GRAND TOTALS:")
    print(f"{'='*50}")
    print(f"Total Records: {grand_total}")
    print(f"Total P1-Critical: {total_p1}")
    print(f"Total P2-High: {total_p2}")
    print(f"Total P3-Medium: {total_p3}")
    print(f"Total P4-Low: {total_p4}")
    print(f"Total P5-Very Low: {total_p5}")

if __name__ == "__main__":
    try:
        distribute_priorities()
        verify_priority_distribution()
        print("\n" + "="*100)
        print("PRIORITY DISTRIBUTION COMPLETED SUCCESSFULLY!")
        print("="*100)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Close the connection
        client.close()