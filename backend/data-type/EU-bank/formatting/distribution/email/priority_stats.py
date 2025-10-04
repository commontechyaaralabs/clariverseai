# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]
email_collection = db["email_new"]

def find_records_by_urgency_and_priority():
    """Find records with urgency=true/false and different priority levels"""
    
    print("=" * 80)
    print("FINDING RECORDS BY URGENCY AND PRIORITY LEVEL")
    print("=" * 80)
    
    # Priority levels to search for
    priority_levels = [
        "P1-Critical",
        "P2-High", 
        "P3-Medium",
        "P4-Low",
        "P5-Very Low"
    ]
    
    # Get total records with urgency field
    total_with_urgency = email_collection.count_documents({"urgency": {"$exists": True}})
    urgent_count = email_collection.count_documents({"urgency": True})
    non_urgent_count = email_collection.count_documents({"urgency": False})
    
    print(f"Total records with urgency field: {total_with_urgency}")
    print(f"Urgent records (urgency=true): {urgent_count}")
    print(f"Non-urgent records (urgency=false): {non_urgent_count}")
    
    if total_with_urgency == 0:
        print("No records with urgency field found. Checking if urgency field exists...")
        
        # Check for urgency as string values
        urgency_string_true = email_collection.count_documents({"urgency": "true"})
        urgency_string_false = email_collection.count_documents({"urgency": "false"})
        print(f"Records with urgency='true' (string): {urgency_string_true}")
        print(f"Records with urgency='false' (string): {urgency_string_false}")
        
        # Show sample record to understand structure
        sample_record = email_collection.find_one({})
        if sample_record:
            print(f"Sample record fields: {list(sample_record.keys())}")
            if "urgency" in sample_record:
                print(f"Sample urgency value: {sample_record['urgency']} (type: {type(sample_record['urgency'])})")
        return
    
    # URGENT RECORDS ANALYSIS
    print(f"\n" + "=" * 50)
    print("URGENT RECORDS (urgency=true)")
    print("=" * 50)
    
    urgent_priority_stats = {}
    total_urgent_with_priority = 0
    
    for priority in priority_levels:
        # Query for urgency=true AND priority=current_priority
        query = {
            "urgency": True,
            "priority": priority
        }
        
        count = email_collection.count_documents(query)
        urgent_priority_stats[priority] = count
        total_urgent_with_priority += count
        
        print(f"{priority}: {count} records")
    
    # Check for urgent records without priority field
    urgent_no_priority = email_collection.count_documents({
        "urgency": True,
        "$or": [
            {"priority": {"$exists": False}},
            {"priority": None},
            {"priority": ""}
        ]
    })
    
    print("-" * 50)
    print(f"Total urgent records with priority: {total_urgent_with_priority}")
    print(f"Urgent records without priority field: {urgent_no_priority}")
    
    # NON-URGENT RECORDS ANALYSIS
    print(f"\n" + "=" * 50)
    print("NON-URGENT RECORDS (urgency=false)")
    print("=" * 50)
    
    non_urgent_priority_stats = {}
    total_non_urgent_with_priority = 0
    
    for priority in priority_levels:
        # Query for urgency=false AND priority=current_priority
        query = {
            "urgency": False,
            "priority": priority
        }
        
        count = email_collection.count_documents(query)
        non_urgent_priority_stats[priority] = count
        total_non_urgent_with_priority += count
        
        print(f"{priority}: {count} records")
    
    # Check for non-urgent records without priority field
    non_urgent_no_priority = email_collection.count_documents({
        "urgency": False,
        "$or": [
            {"priority": {"$exists": False}},
            {"priority": None},
            {"priority": ""}
        ]
    })
    
    print("-" * 50)
    print(f"Total non-urgent records with priority: {total_non_urgent_with_priority}")
    print(f"Non-urgent records without priority field: {non_urgent_no_priority}")
    
    # SUMMARY
    print(f"\n" + "=" * 80)
    print("SUMMARY BY URGENCY AND PRIORITY")
    print("=" * 80)
    
    print(f"\nURGENT RECORDS (urgency=true):")
    print("-" * 40)
    for priority, count in urgent_priority_stats.items():
        percentage = (count / urgent_count * 100) if urgent_count > 0 else 0
        print(f"{priority}: {count} records ({percentage:.1f}%)")
    
    print(f"\nNON-URGENT RECORDS (urgency=false):")
    print("-" * 40)
    for priority, count in non_urgent_priority_stats.items():
        percentage = (count / non_urgent_count * 100) if non_urgent_count > 0 else 0
        print(f"{priority}: {count} records ({percentage:.1f}%)")
    
    # OVERALL PRIORITY DISTRIBUTION
    print(f"\nOVERALL PRIORITY DISTRIBUTION:")
    print("-" * 40)
    for priority in priority_levels:
        urgent_count_priority = urgent_priority_stats[priority]
        non_urgent_count_priority = non_urgent_priority_stats[priority]
        total_priority = urgent_count_priority + non_urgent_count_priority
        
        urgent_pct = (urgent_count_priority / total_priority * 100) if total_priority > 0 else 0
        non_urgent_pct = (non_urgent_count_priority / total_priority * 100) if total_priority > 0 else 0
        
        print(f"{priority}: {total_priority} total (Urgent: {urgent_count_priority} [{urgent_pct:.1f}%], Non-urgent: {non_urgent_count_priority} [{non_urgent_pct:.1f}%])")
    
    return {
        "urgent": urgent_priority_stats,
        "non_urgent": non_urgent_priority_stats,
        "total_urgent": urgent_count,
        "total_non_urgent": non_urgent_count
    }

def get_records_sample(urgency_value, priority, limit=5):
    """Get sample records for a specific urgency and priority level"""
    
    urgency_label = "URGENT" if urgency_value else "NON-URGENT"
    print(f"\n" + "=" * 80)
    print(f"SAMPLE RECORDS FOR {urgency_label} - {priority}")
    print("=" * 80)
    
    query = {
        "urgency": urgency_value,
        "priority": priority
    }
    
    records = list(email_collection.find(query).limit(limit))
    
    if not records:
        print(f"No records found for urgency={urgency_value} and priority={priority}")
        return []
    
    print(f"Showing {len(records)} sample records:")
    print("-" * 70)
    
    for i, record in enumerate(records, 1):
        print(f"\nRecord {i}:")
        print(f"  ID: {record.get('_id')}")
        print(f"  Topic: {record.get('dominant_topic', 'N/A')}")
        print(f"  Category: {record.get('category', 'N/A')}")
        print(f"  Urgency: {record.get('urgency')}")
        print(f"  Priority: {record.get('priority')}")
        print(f"  Follow-up Required: {record.get('follow_up_required', 'N/A')}")
        print(f"  Action Pending: {record.get('action_pending_status', 'N/A')}")
    
    return records

def find_records_by_topic_and_urgency():
    """Find records grouped by topic, urgency, and priority"""
    
    print(f"\n" + "=" * 80)
    print("RECORDS BY TOPIC, URGENCY, AND PRIORITY")
    print("=" * 80)
    
    # Aggregate records by topic, urgency, and priority
    pipeline = [
        {"$match": {"urgency": {"$exists": True}}},
        {"$group": {
            "_id": {
                "topic": "$dominant_topic",
                "urgency": "$urgency",
                "priority": "$priority"
            },
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.urgency": 1, "_id.priority": 1, "count": -1}}
    ]
    
    results = list(email_collection.aggregate(pipeline))
    
    if not results:
        print("No records found for topic analysis")
        return
    
    print(f"Found records across {len(results)} topic-urgency-priority combinations")
    print("-" * 70)
    
    current_urgency = None
    current_priority = None
    
    for result in results:
        topic = result["_id"]["topic"] or "Unknown Topic"
        urgency = result["_id"]["urgency"]
        priority = result["_id"]["priority"] or "No Priority"
        count = result["count"]
        
        urgency_label = "URGENT" if urgency else "NON-URGENT"
        
        # Print urgency header when it changes
        if urgency != current_urgency:
            if current_urgency is not None:
                print()  # Add spacing between urgency groups
            print(f"\n{'='*20} {urgency_label} RECORDS {'='*20}")
            current_urgency = urgency
            current_priority = None
        
        # Print priority header when it changes within urgency group
        if priority != current_priority:
            if current_priority is not None:
                print()  # Add spacing between priority groups
            print(f"\n{priority}:")
            current_priority = priority
        
        print(f"  {topic}: {count} records")

if __name__ == "__main__":
    try:
        # Find records by urgency and priority
        stats = find_records_by_urgency_and_priority()
        
        if stats:
            # Get samples for urgent records
            urgent_stats = stats["urgent"]
            non_urgent_stats = stats["non_urgent"]
            
            print(f"\n" + "=" * 80)
            print("SAMPLE RECORDS")
            print("=" * 80)
            
            # Show samples for urgent records
            for priority, count in urgent_stats.items():
                if count > 0:
                    get_records_sample(True, priority, limit=2)
            
            # Show samples for non-urgent records
            for priority, count in non_urgent_stats.items():
                if count > 0:
                    get_records_sample(False, priority, limit=2)
            
            # Show records by topic, urgency, and priority
            find_records_by_topic_and_urgency()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print(f"\nDatabase connection closed.")
