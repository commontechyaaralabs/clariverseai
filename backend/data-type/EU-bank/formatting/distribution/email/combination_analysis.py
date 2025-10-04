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

def explore_field_values():
    """Explore all possible values for key fields in the database"""
    
    print("=" * 80)
    print("EXPLORING FIELD VALUES IN DATABASE")
    print("=" * 80)
    
    fields_to_explore = [
        "urgency",
        "follow_up_required", 
        "action_pending_status",
        "action_pending_from",
        "category",
        "stages",
        "priority",
        "resolution_status"
    ]
    
    field_values = {}
    
    for field in fields_to_explore:
        print(f"\n{field.upper()}:")
        print("-" * 40)
        
        # Get distinct values for this field
        distinct_values = email_collection.distinct(field)
        
        if not distinct_values:
            print(f"  No distinct values found for {field}")
            continue
            
        field_values[field] = distinct_values
        
        # Show values with counts
        for value in sorted(distinct_values):
            count = email_collection.count_documents({field: value})
            print(f"  {value}: {count} records")
        
        # Check for null/empty values
        null_count = email_collection.count_documents({
            "$or": [
                {field: {"$exists": False}},
                {field: None},
                {field: ""}
            ]
        })
        
        if null_count > 0:
            print(f"  (null/empty): {null_count} records")
    
    return field_values

def analyze_combinations():
    """Analyze combinations of key fields"""
    
    print("\n" + "=" * 80)
    print("ANALYZING FIELD COMBINATIONS")
    print("=" * 80)
    
    # Key combinations to analyze
    combinations = [
        # Basic urgency combinations
        ["urgency", "follow_up_required"],
        ["urgency", "action_pending_status"],
        ["urgency", "category"],
        ["urgency", "priority"],
        
        # Follow-up and action combinations
        ["follow_up_required", "action_pending_status"],
        ["follow_up_required", "action_pending_from"],
        ["follow_up_required", "category"],
        ["follow_up_required", "stages"],
        
        # Action pending combinations
        ["action_pending_status", "action_pending_from"],
        ["action_pending_status", "category"],
        ["action_pending_status", "stages"],
        ["action_pending_status", "priority"],
        
        # Category and stage combinations
        ["category", "stages"],
        ["category", "priority"],
        ["stages", "priority"],
        
        # Three-field combinations
        ["urgency", "follow_up_required", "action_pending_status"],
        ["urgency", "category", "priority"],
        ["follow_up_required", "action_pending_status", "category"],
        ["action_pending_status", "action_pending_from", "category"],
        
        # Four-field combinations
        ["urgency", "follow_up_required", "action_pending_status", "category"],
        ["urgency", "category", "priority", "action_pending_status"],
    ]
    
    for combo in combinations:
        print(f"\n{'+'.join(combo).upper()}:")
        print("-" * 60)
        
        # Create aggregation pipeline
        pipeline = [
            {
                "$group": {
                    "_id": {field: f"${field}" for field in combo},
                    "count": {"$sum": 1}
                }
            },
            {"$sort": {"count": -1}},
            {"$limit": 20}  # Show top 20 combinations
        ]
        
        results = list(email_collection.aggregate(pipeline))
        
        if not results:
            print("  No combinations found")
            continue
        
        for result in results:
            combo_values = result["_id"]
            count = result["count"]
            
            # Format the combination
            combo_str = " | ".join([f"{field}:{value}" for field, value in combo_values.items()])
            print(f"  {combo_str}: {count} records")

def analyze_specific_combinations():
    """Analyze specific important combinations requested by user"""
    
    print("\n" + "=" * 80)
    print("SPECIFIC COMBINATION ANALYSIS")
    print("=" * 80)
    
    # Combination 1: urgency=true + follow_up_required=yes
    print("\n1. URGENCY=TRUE + FOLLOW_UP_REQUIRED=YES:")
    print("-" * 50)
    
    combo1_pipeline = [
        {"$match": {"urgency": True, "follow_up_required": "yes"}},
        {
            "$group": {
                "_id": {
                    "category": "$category",
                    "stages": "$stages", 
                    "priority": "$priority",
                    "action_pending_status": "$action_pending_status",
                    "action_pending_from": "$action_pending_from"
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}}
    ]
    
    combo1_results = list(email_collection.aggregate(combo1_pipeline))
    
    if combo1_results:
        print(f"Total combinations found: {len(combo1_results)}")
        for result in combo1_results:
            combo = result["_id"]
            count = result["count"]
            
            category = combo.get("category", "N/A")
            stages = combo.get("stages", "N/A")
            priority = combo.get("priority", "N/A")
            action_status = combo.get("action_pending_status", "N/A")
            action_from = combo.get("action_pending_from", "N/A")
            
            print(f"  Category: {category} | Stages: {stages} | Priority: {priority} | Action: {action_status} | From: {action_from} → {count} records")
    else:
        print("  No records found with urgency=true and follow_up_required=yes")
    
    # Combination 2: urgency=true + action_pending_status=yes
    print("\n2. URGENCY=TRUE + ACTION_PENDING_STATUS=YES:")
    print("-" * 50)
    
    combo2_pipeline = [
        {"$match": {"urgency": True, "action_pending_status": "yes"}},
        {
            "$group": {
                "_id": {
                    "category": "$category",
                    "stages": "$stages",
                    "priority": "$priority", 
                    "follow_up_required": "$follow_up_required",
                    "action_pending_from": "$action_pending_from"
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}}
    ]
    
    combo2_results = list(email_collection.aggregate(combo2_pipeline))
    
    if combo2_results:
        print(f"Total combinations found: {len(combo2_results)}")
        for result in combo2_results:
            combo = result["_id"]
            count = result["count"]
            
            category = combo.get("category", "N/A")
            stages = combo.get("stages", "N/A")
            priority = combo.get("priority", "N/A")
            follow_up = combo.get("follow_up_required", "N/A")
            action_from = combo.get("action_pending_from", "N/A")
            
            print(f"  Category: {category} | Stages: {stages} | Priority: {priority} | Follow-up: {follow_up} | From: {action_from} → {count} records")
    else:
        print("  No records found with urgency=true and action_pending_status=yes")
    
    # Combination 3: All urgent records with full breakdown
    print("\n3. ALL URGENT RECORDS - COMPLETE BREAKDOWN:")
    print("-" * 50)
    
    combo3_pipeline = [
        {"$match": {"urgency": True}},
        {
            "$group": {
                "_id": {
                    "follow_up_required": "$follow_up_required",
                    "action_pending_status": "$action_pending_status", 
                    "action_pending_from": "$action_pending_from",
                    "category": "$category",
                    "stages": "$stages",
                    "priority": "$priority"
                },
                "count": {"$sum": 1}
            }
        },
        {"$sort": {"count": -1}},
        {"$limit": 30}
    ]
    
    combo3_results = list(email_collection.aggregate(combo3_pipeline))
    
    if combo3_results:
        print(f"Showing top {len(combo3_results)} combinations:")
        for result in combo3_results:
            combo = result["_id"]
            count = result["count"]
            
            follow_up = combo.get("follow_up_required", "N/A")
            action_status = combo.get("action_pending_status", "N/A")
            action_from = combo.get("action_pending_from", "N/A")
            category = combo.get("category", "N/A")
            stages = combo.get("stages", "N/A")
            priority = combo.get("priority", "N/A")
            
            print(f"  Follow-up: {follow_up} | Action: {action_status} | From: {action_from} | Category: {category} | Stages: {stages} | Priority: {priority} → {count} records")
    else:
        print("  No urgent records found")

def analyze_category_breakdown():
    """Detailed breakdown by category"""
    
    print("\n" + "=" * 80)
    print("DETAILED CATEGORY BREAKDOWN")
    print("=" * 80)
    
    # Get all categories
    categories = email_collection.distinct("category")
    
    for category in sorted(categories):
        if not category:
            category = "Unknown"
            
        print(f"\n{category.upper()} CATEGORY:")
        print("-" * 40)
        
        # Total count for this category
        total_count = email_collection.count_documents({"category": category})
        print(f"Total records: {total_count}")
        
        # Urgency breakdown
        urgent_count = email_collection.count_documents({"category": category, "urgency": True})
        non_urgent_count = email_collection.count_documents({"category": category, "urgency": False})
        
        print(f"Urgency breakdown:")
        print(f"  Urgent: {urgent_count} ({urgent_count/total_count*100:.1f}%)")
        print(f"  Non-urgent: {non_urgent_count} ({non_urgent_count/total_count*100:.1f}%)")
        
        # Follow-up and action breakdown
        followup_yes = email_collection.count_documents({"category": category, "follow_up_required": "yes"})
        followup_no = email_collection.count_documents({"category": category, "follow_up_required": "no"})
        
        action_yes = email_collection.count_documents({"category": category, "action_pending_status": "yes"})
        action_no = email_collection.count_documents({"category": category, "action_pending_status": "no"})
        
        print(f"Follow-up breakdown:")
        print(f"  Yes: {followup_yes} ({followup_yes/total_count*100:.1f}%)")
        print(f"  No: {followup_no} ({followup_no/total_count*100:.1f}%)")
        
        print(f"Action pending breakdown:")
        print(f"  Yes: {action_yes} ({action_yes/total_count*100:.1f}%)")
        print(f"  No: {action_no} ({action_no/total_count*100:.1f}%)")
        
        # Priority breakdown
        priority_pipeline = [
            {"$match": {"category": category}},
            {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        priority_results = list(email_collection.aggregate(priority_pipeline))
        
        print(f"Priority breakdown:")
        for result in priority_results:
            priority = result["_id"] or "Unknown"
            count = result["count"]
            percentage = count/total_count*100
            print(f"  {priority}: {count} ({percentage:.1f}%)")
        
        # Stage breakdown
        stage_pipeline = [
            {"$match": {"category": category}},
            {"$group": {"_id": "$stages", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        stage_results = list(email_collection.aggregate(stage_pipeline))
        
        print(f"Stages breakdown:")
        for result in stage_results:
            stages = result["_id"] or "Unknown"
            count = result["count"]
            percentage = count/total_count*100
            print(f"  {stages}: {count} ({percentage:.1f}%)")

if __name__ == "__main__":
    try:
        # Step 1: Explore all field values
        field_values = explore_field_values()
        
        # Step 2: Analyze general combinations
        analyze_combinations()
        
        # Step 3: Analyze specific combinations requested
        analyze_specific_combinations()
        
        # Step 4: Detailed category breakdown
        analyze_category_breakdown()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print(f"\nDatabase connection closed.")
