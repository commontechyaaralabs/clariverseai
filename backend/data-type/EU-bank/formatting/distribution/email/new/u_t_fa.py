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

# Topic mapping with exact distribution percentages
# Internal Topics with specific follow-up rates
INTERNAL_TOPICS = {
    "Legal Escalation": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Quality Assurance Breach": {"yes_rate": 0.79, "no_rate": 0.21},  # 79% yes, 21% no
    "Risk Management Alert": {"yes_rate": 0.88, "no_rate": 0.12},  # 88% yes, 12% no
    "Technology Emergency": {"yes_rate": 0.91, "no_rate": 0.09}  # 91% yes, 9% no
}

# External Topics with specific follow-up rates
EXTERNAL_TOPICS = {
    "Clearing House Problem": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Compliance Monitoring Alert": {"yes_rate": 0.88, "no_rate": 0.12},  # 88% yes, 12% no
    "Covenant Breach Alert": {"yes_rate": 0.89, "no_rate": 0.11},  # 89% yes, 11% no
    "Customer Service Escalation": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Cybersecurity Incident Report": {"yes_rate": 1.00, "no_rate": 0.00},  # 100% yes, 0% no
    "Data Breach Warning": {"yes_rate": 1.00, "no_rate": 0.00},  # 100% yes, 0% no
    "Executive Escalation Email": {"yes_rate": 0.85, "no_rate": 0.15},  # 85% yes, 15% no
    "Payment Service Problem": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Processing Delay Complaint": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Sanctions Screening Alert": {"yes_rate": 0.90, "no_rate": 0.10},  # 90% yes, 10% no
    "Security Incident Alert": {"yes_rate": 1.00, "no_rate": 0.00},  # 100% yes, 0% no
    "System Outage Notification": {"yes_rate": 0.75, "no_rate": 0.25}  # 75% yes, 25% no
}

# Combine all topics
ALL_TOPICS = {**INTERNAL_TOPICS, **EXTERNAL_TOPICS}

def add_follow_up_fields():
    """Add follow_up_required and action_pending_status fields based on dominant_topic"""
    
    print("=" * 80)
    print("ADDING FOLLOW_UP_REQUIRED AND ACTION_PENDING_STATUS FIELDS")
    print("=" * 80)
    
    # First, filter records where urgency is true
    print("Step 1: Filtering records where urgency = true...")
    
    urgent_query = {"urgency": True}
    urgent_count = email_collection.count_documents(urgent_query)
    print(f"Found {urgent_count} records with urgency = true")
    
    if urgent_count == 0:
        print("No urgent records found. Exiting...")
        return
    
    # Show current field status
    print(f"\nStep 2: Checking current field status...")
    follow_up_exists = email_collection.count_documents({"follow_up_required": {"$exists": True}})
    action_pending_exists = email_collection.count_documents({"action_pending_status": {"$exists": True}})
    
    print(f"Records with follow_up_required field: {follow_up_exists}")
    print(f"Records with action_pending_status field: {action_pending_exists}")
    
    # Get urgent records with dominant_topic
    print(f"\nStep 3: Processing urgent records...")
    
    urgent_records = list(email_collection.find(
        {"urgency": True, "dominant_topic": {"$exists": True}},
        {"_id": 1, "dominant_topic": 1, "category": 1, "follow_up_required": 1, "action_pending_status": 1}
    ))
    
    print(f"Found {len(urgent_records)} urgent records with dominant_topic")
    
    # Process each record with proper distribution
    updated_count = 0
    skipped_count = 0
    topic_stats = {}
    
    # Group records by topic for proper distribution
    topic_groups = {}
    for record in urgent_records:
        dominant_topic = record.get("dominant_topic")
        if dominant_topic not in topic_groups:
            topic_groups[dominant_topic] = []
        topic_groups[dominant_topic].append(record)
    
    print(f"Processing {len(topic_groups)} unique topics...")
    
    for dominant_topic, records in topic_groups.items():
        category = records[0].get("category", "Unknown")
        total_records = len(records)
        
        # Check if topic is in our mapping
        if dominant_topic in ALL_TOPICS:
            topic_config = ALL_TOPICS[dominant_topic]
            yes_rate = topic_config["yes_rate"]
            no_rate = topic_config["no_rate"]
            
            # Calculate how many records should get "yes" vs "no"
            yes_count = int(total_records * yes_rate)
            no_count = total_records - yes_count  # Remaining records get "no"
            
            print(f"  {dominant_topic}: {total_records} records → {yes_count} 'yes', {no_count} 'no'")
            
            # Process records for this topic
            for i, record in enumerate(records):
                # Determine follow-up value based on position in the list
                if i < yes_count:
                    follow_up_value = "yes"
                else:
                    follow_up_value = "no"
                
                action_pending_value = follow_up_value  # Same as follow_up_required
                
                # Check if fields already exist and have different values
                current_follow_up = record.get("follow_up_required")
                current_action_pending = record.get("action_pending_status")
                
                # Skip if fields already exist with correct values
                if (current_follow_up == follow_up_value and 
                    current_action_pending == action_pending_value):
                    skipped_count += 1
                    continue
                
                # Update the record
                update_result = email_collection.update_one(
                    {"_id": record["_id"]},
                    {
                        "$set": {
                            "follow_up_required": follow_up_value,
                            "action_pending_status": action_pending_value
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    updated_count += 1
                    
                    # Track statistics
                    if dominant_topic not in topic_stats:
                        topic_stats[dominant_topic] = {
                            "total": 0, "yes_count": 0, "no_count": 0, 
                            "updated": 0, "category": category
                        }
                    topic_stats[dominant_topic]["total"] += 1
                    topic_stats[dominant_topic]["updated"] += 1
                    if follow_up_value == "yes":
                        topic_stats[dominant_topic]["yes_count"] += 1
                    else:
                        topic_stats[dominant_topic]["no_count"] += 1
        else:
            # Topic not in mapping - set default values to "no"
            print(f"  {dominant_topic}: {total_records} records → 0 'yes', {total_records} 'no' (unknown topic)")
            
            for record in records:
                update_result = email_collection.update_one(
                    {"_id": record["_id"]},
                    {
                        "$set": {
                            "follow_up_required": "no",
                            "action_pending_status": "no"
                        }
                    }
                )
                
                if update_result.modified_count > 0:
                    updated_count += 1
                    print(f"⚠️  Unknown topic '{dominant_topic}' - set to 'no'")
    
    # Display results
    print(f"\n" + "=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    
    print(f"Total urgent records processed: {len(urgent_records)}")
    print(f"Records updated: {updated_count}")
    print(f"Records skipped (already correct): {skipped_count}")
    
    print(f"\nTopic Statistics:")
    print("-" * 50)
    
    # Sort topics by category and name
    internal_topics = [(topic, stats) for topic, stats in topic_stats.items() 
                      if stats["category"] == "Internal"]
    external_topics = [(topic, stats) for topic, stats in topic_stats.items() 
                      if stats["category"] == "External"]
    
    if internal_topics:
        print("Internal Topics:")
        for topic, stats in sorted(internal_topics):
            yes_pct = (stats['yes_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            no_pct = (stats['no_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {topic}: {stats['total']} total → {stats['yes_count']} 'yes' ({yes_pct:.1f}%), {stats['no_count']} 'no' ({no_pct:.1f}%)")
    
    if external_topics:
        print("External Topics:")
        for topic, stats in sorted(external_topics):
            yes_pct = (stats['yes_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            no_pct = (stats['no_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {topic}: {stats['total']} total → {stats['yes_count']} 'yes' ({yes_pct:.1f}%), {stats['no_count']} 'no' ({no_pct:.1f}%)")
    
    # Verify final counts
    print(f"\nFinal Verification:")
    final_follow_up_yes = email_collection.count_documents({"follow_up_required": "yes"})
    final_follow_up_no = email_collection.count_documents({"follow_up_required": "no"})
    final_action_yes = email_collection.count_documents({"action_pending_status": "yes"})
    final_action_no = email_collection.count_documents({"action_pending_status": "no"})
    
    print(f"follow_up_required = 'yes': {final_follow_up_yes}")
    print(f"follow_up_required = 'no': {final_follow_up_no}")
    print(f"action_pending_status = 'yes': {final_action_yes}")
    print(f"action_pending_status = 'no': {final_action_no}")
    
    return updated_count

def show_topic_mapping():
    """Display the topic mapping that will be used"""
    
    print("=" * 80)
    print("TOPIC MAPPING FOR FOLLOW_UP_REQUIRED AND ACTION_PENDING_STATUS")
    print("=" * 80)
    
    print("Internal Topics with Distribution:")
    print("-" * 60)
    for i, (topic, config) in enumerate(sorted(INTERNAL_TOPICS.items()), 1):
        yes_pct = config["yes_rate"] * 100
        no_pct = config["no_rate"] * 100
        print(f"{i:2d}. {topic}: {yes_pct:.0f}% 'yes', {no_pct:.0f}% 'no'")
    
    print(f"\nExternal Topics with Distribution:")
    print("-" * 60)
    for i, (topic, config) in enumerate(sorted(EXTERNAL_TOPICS.items()), 1):
        yes_pct = config["yes_rate"] * 100
        no_pct = config["no_rate"] * 100
        print(f"{i:2d}. {topic}: {yes_pct:.0f}% 'yes', {no_pct:.0f}% 'no'")
    
    print(f"\nTotal mapped topics: {len(ALL_TOPICS)}")
    print("Distribution Logic:")
    print("  - Each topic will be distributed according to the percentages above")
    print("  - follow_up_required = action_pending_status (same value)")
    print("  - Unknown topics will get 'no' for both fields")

if __name__ == "__main__":
    try:
        # Show the mapping first
        show_topic_mapping()
        
        # Confirm before proceeding
        print(f"\nProceed with adding fields to urgent records? (y/n): ", end="")
        proceed = input().lower().strip() == 'y'
        
        if proceed:
            updated_count = add_follow_up_fields()
            if updated_count > 0:
                print(f"\n✅ Successfully updated {updated_count} records!")
            else:
                print(f"\n⚠️  No records were updated.")
        else:
            print(f"\nOperation cancelled.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print(f"\nDatabase connection closed.")