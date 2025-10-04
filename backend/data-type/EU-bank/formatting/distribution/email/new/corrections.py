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

def upgrade_p3_to_p1():
    """Upgrade 150 External P3-Medium records to P1-Critical"""
    
    print("=" * 80)
    print("UPGRADING P3-MEDIUM TO P1-CRITICAL FOR EXTERNAL EMAILS")
    print("=" * 80)
    
    # Filter criteria
    filter_query = {
        "category": "External",
        "priority": "P3-Medium",
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    }
    
    print("Step 1: Finding records matching criteria...")
    print(f"Filter: {filter_query}")
    
    # Count matching records
    matching_count = email_collection.count_documents(filter_query)
    print(f"Found {matching_count} records matching the criteria")
    
    if matching_count == 0:
        print("No records found matching the criteria. Exiting...")
        return
    
    if matching_count < 150:
        print(f"⚠️  Warning: Only {matching_count} records found, but need 150")
        print(f"Will upgrade all {matching_count} records to P1-Critical")
        target_count = matching_count
    else:
        target_count = 150
        print(f"Will upgrade {target_count} records to P1-Critical")
    
    # Get the records to update
    print(f"\nStep 2: Retrieving {target_count} records...")
    
    records_to_update = list(email_collection.find(
        filter_query,
        {"_id": 1, "dominant_topic": 1, "priority": 1}
    ).limit(target_count))
    
    print(f"Retrieved {len(records_to_update)} records for update")
    
    # Show sample of records being updated
    print(f"\nStep 3: Sample of records to be updated:")
    print("-" * 60)
    for i, record in enumerate(records_to_update[:5]):  # Show first 5
        topic = record.get("dominant_topic", "Unknown")
        print(f"{i+1}. ID: {record['_id']} | Topic: {topic}")
    
    if len(records_to_update) > 5:
        print(f"... and {len(records_to_update) - 5} more records")
    
    # Confirm before proceeding
    print(f"\nStep 4: Confirmation")
    print(f"About to upgrade {len(records_to_update)} records from P3-Medium to P1-Critical")
    print("This action will:")
    print("  - Change priority from 'P3-Medium' to 'P1-Critical'")
    print("  - Only affect External emails with follow_up_required='yes' and action_pending_status='yes'")
    
    proceed = input(f"\nProceed with the upgrade? (y/n): ").lower().strip() == 'y'
    
    if not proceed:
        print("Operation cancelled.")
        return
    
    # Perform the update
    print(f"\nStep 5: Updating records...")
    
    updated_count = 0
    failed_count = 0
    topic_stats = {}
    
    for record in records_to_update:
        try:
            # Update the record
            update_result = email_collection.update_one(
                {"_id": record["_id"]},
                {"$set": {"priority": "P1-Critical"}}
            )
            
            if update_result.modified_count > 0:
                updated_count += 1
                
                # Track statistics by topic
                topic = record.get("dominant_topic", "Unknown")
                if topic not in topic_stats:
                    topic_stats[topic] = 0
                topic_stats[topic] += 1
                
            else:
                failed_count += 1
                print(f"⚠️  Failed to update record: {record['_id']}")
                
        except Exception as e:
            failed_count += 1
            print(f"❌ Error updating record {record['_id']}: {e}")
    
    # Display results
    print(f"\n" + "=" * 80)
    print("UPGRADE RESULTS")
    print("=" * 80)
    
    print(f"Records processed: {len(records_to_update)}")
    print(f"Successfully updated: {updated_count}")
    print(f"Failed updates: {failed_count}")
    
    if updated_count > 0:
        print(f"\nTopic breakdown of upgraded records:")
        print("-" * 60)
        for topic, count in sorted(topic_stats.items()):
            print(f"  {topic}: {count} records")
    
    # Verify the changes
    print(f"\nStep 6: Verification")
    print("-" * 60)
    
    # Check remaining P3-Medium records with same criteria
    remaining_p3 = email_collection.count_documents(filter_query)
    print(f"Remaining P3-Medium records (same criteria): {remaining_p3}")
    
    # Check total P1-Critical count
    total_p1 = email_collection.count_documents({"priority": "P1-Critical"})
    print(f"Total P1-Critical records: {total_p1}")
    
    # Check total P3-Medium count
    total_p3 = email_collection.count_documents({"priority": "P3-Medium"})
    print(f"Total P3-Medium records: {total_p3}")
    
    # Check P1-Critical with our specific criteria
    p1_with_criteria = email_collection.count_documents({
        "category": "External",
        "priority": "P1-Critical",
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    })
    print(f"P1-Critical records (External, follow_up=yes, action_pending=yes): {p1_with_criteria}")
    
    print(f"\n✅ Upgrade completed successfully!")
    print(f"   {updated_count} records upgraded from P3-Medium to P1-Critical")
    
    return updated_count

def show_current_status():
    """Show current status of P3-Medium records"""
    
    print("=" * 80)
    print("CURRENT STATUS OF P3-MEDIUM RECORDS")
    print("=" * 80)
    
    # Total P3-Medium count
    total_p3 = email_collection.count_documents({"priority": "P3-Medium"})
    print(f"Total P3-Medium records: {total_p3}")
    
    # External P3-Medium with action required
    external_p3_action = email_collection.count_documents({
        "category": "External",
        "priority": "P3-Medium",
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    })
    print(f"External P3-Medium with action required: {external_p3_action}")
    
    # Internal P3-Medium with action required
    internal_p3_action = email_collection.count_documents({
        "category": "Internal",
        "priority": "P3-Medium",
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    })
    print(f"Internal P3-Medium with action required: {internal_p3_action}")
    
    # Show topic breakdown for External P3-Medium with action required
    print(f"\nTopic breakdown for External P3-Medium with action required:")
    print("-" * 60)
    
    pipeline = [
        {"$match": {
            "category": "External",
            "priority": "P3-Medium",
            "follow_up_required": "yes",
            "action_pending_status": "yes"
        }},
        {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    topic_results = list(email_collection.aggregate(pipeline))
    
    for i, result in enumerate(topic_results, 1):
        print(f"{i:2d}. {result['_id']}: {result['count']} records")
    
    print(f"\nTotal topics: {len(topic_results)}")

if __name__ == "__main__":
    try:
        # Show current status first
        show_current_status()
        
        print(f"\n" + "=" * 80)
        print("P3-MEDIUM TO P1-CRITICAL UPGRADE")
        print("=" * 80)
        
        # Run the upgrade
        updated_count = upgrade_p3_to_p1()
        
        if updated_count > 0:
            print(f"\n🎉 Successfully upgraded {updated_count} records!")
        else:
            print(f"\n⚠️  No records were upgraded.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print(f"\nDatabase connection closed.")
