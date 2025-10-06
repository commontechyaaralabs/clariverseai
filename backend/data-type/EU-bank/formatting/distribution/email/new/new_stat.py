# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from collections import defaultdict

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]
email_collection = db["email_new"]

def get_priority_stats(query, description):
    """
    Get priority distribution for a given query
    """
    pipeline = [
        {"$match": query},
        {"$group": {
            "_id": "$priority",
            "count": {"$sum": 1}
        }},
        {"$sort": {"count": -1}}
    ]
    
    results = list(email_collection.aggregate(pipeline))
    total = sum(item['count'] for item in results)
    
    print(f"\n{'='*60}")
    print(f"{description}")
    print(f"{'='*60}")
    print(f"Total Emails: {total}")
    
    if results:
        print("\nPriority Breakdown:")
        for item in results:
            priority = item['_id'] if item['_id'] else "None/Null"
            count = item['count']
            percentage = (count / total * 100) if total > 0 else 0
            print(f"  {priority}: {count} ({percentage:.1f}%)")
    else:
        print("No emails found matching this criteria")
    
    return results, total

# Define all combinations to analyze
combinations = [
    # Single field combinations
    {
        "query": {"urgency": True},
        "desc": "Urgent Emails"
    },
    {
        "query": {"urgency": False},
        "desc": "Not Urgent Emails"
    },
    {
        "query": {"follow_up_required": "yes"},
        "desc": "Follow-up Required"
    },
    {
        "query": {"follow_up_required": "no"},
        "desc": "No Follow-up Required"
    },
    {
        "query": {"action_pending_status": "yes"},
        "desc": "Action Pending"
    },
    {
        "query": {"action_pending_status": "no"},
        "desc": "No Action Pending"
    },
    
    # Two field combinations
    {
        "query": {"urgency": True, "follow_up_required": "yes"},
        "desc": "Urgent + Follow-up Required"
    },
    {
        "query": {"urgency": True, "follow_up_required": "no"},
        "desc": "Urgent + No Follow-up Required"
    },
    {
        "query": {"urgency": True, "action_pending_status": "yes"},
        "desc": "Urgent + Action Pending"
    },
    {
        "query": {"urgency": True, "action_pending_status": "no"},
        "desc": "Urgent + No Action Pending"
    },
    {
        "query": {"urgency": False, "follow_up_required": "yes"},
        "desc": "Not Urgent + Follow-up Required"
    },
    {
        "query": {"urgency": False, "follow_up_required": "no"},
        "desc": "Not Urgent + No Follow-up Required"
    },
    {
        "query": {"urgency": False, "action_pending_status": "yes"},
        "desc": "Not Urgent + Action Pending"
    },
    {
        "query": {"urgency": False, "action_pending_status": "no"},
        "desc": "Not Urgent + No Action Pending"
    },
    {
        "query": {"follow_up_required": "yes", "action_pending_status": "yes"},
        "desc": "Follow-up Required + Action Pending"
    },
    {
        "query": {"follow_up_required": "yes", "action_pending_status": "no"},
        "desc": "Follow-up Required + No Action Pending"
    },
    {
        "query": {"follow_up_required": "no", "action_pending_status": "yes"},
        "desc": "No Follow-up Required + Action Pending"
    },
    {
        "query": {"follow_up_required": "no", "action_pending_status": "no"},
        "desc": "No Follow-up Required + No Action Pending"
    },
    
    # Three field combinations (all 8 possible combinations)
    {
        "query": {"urgency": True, "follow_up_required": "yes", "action_pending_status": "yes"},
        "desc": "Urgent + Follow-up Required + Action Pending"
    },
    {
        "query": {"urgency": True, "follow_up_required": "yes", "action_pending_status": "no"},
        "desc": "Urgent + Follow-up Required + No Action Pending"
    },
    {
        "query": {"urgency": True, "follow_up_required": "no", "action_pending_status": "yes"},
        "desc": "Urgent + No Follow-up + Action Pending"
    },
    {
        "query": {"urgency": True, "follow_up_required": "no", "action_pending_status": "no"},
        "desc": "Urgent + No Follow-up + No Action Pending"
    },
    {
        "query": {"urgency": False, "follow_up_required": "yes", "action_pending_status": "yes"},
        "desc": "Not Urgent + Follow-up Required + Action Pending"
    },
    {
        "query": {"urgency": False, "follow_up_required": "yes", "action_pending_status": "no"},
        "desc": "Not Urgent + Follow-up Required + No Action Pending"
    },
    {
        "query": {"urgency": False, "follow_up_required": "no", "action_pending_status": "yes"},
        "desc": "Not Urgent + No Follow-up + Action Pending"
    },
    {
        "query": {"urgency": False, "follow_up_required": "no", "action_pending_status": "no"},
        "desc": "Not Urgent + No Follow-up + No Action Pending"
    },
]

# Run analysis for all combinations
print("\n" + "="*60)
print("EMAIL PRIORITY STATISTICS ANALYSIS")
print("="*60)

all_stats = []
for combo in combinations:
    results, total = get_priority_stats(combo["query"], combo["desc"])
    all_stats.append({
        "description": combo["desc"],
        "query": combo["query"],
        "results": results,
        "total": total
    })

# Summary Statistics
print("\n\n" + "="*60)
print("SUMMARY - HIGHEST PRIORITY COUNTS BY COMBINATION")
print("="*60)

# Sort by total count
sorted_stats = sorted(all_stats, key=lambda x: x['total'], reverse=True)
for stat in sorted_stats[:10]:  # Top 10
    print(f"\n{stat['description']}: {stat['total']} emails")
    if stat['results']:
        top_priority = stat['results'][0]
        priority_name = top_priority['_id'] if top_priority['_id'] else "None/Null"
        print(f"  → Most common priority: {priority_name} ({top_priority['count']} emails)")

# Close MongoDB connection
client.close()
print("\n" + "="*60)
print("Analysis Complete!")
print("="*60)