# Import required libraries
from pymongo import MongoClient
import os
import random
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

def assign_stages_and_open():
    """Assign random stages and set resolution_status to 'open' for action-required records"""
    
    print("=" * 80)
    print("ASSIGNING STAGES AND OPENING ACTION-REQUIRED RECORDS")
    print("=" * 80)
    
    # Filter criteria
    filter_query = {
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    }
    
    print("Step 1: Finding records with action required...")
    print(f"Filter: {filter_query}")
    
    # Count matching records
    matching_count = email_collection.count_documents(filter_query)
    print(f"Found {matching_count} records with follow_up_required='yes' and action_pending_status='yes'")
    
    if matching_count == 0:
        print("No records found matching the criteria. Exiting...")
        return
    
    # Get all matching records
    print(f"\nStep 2: Retrieving all {matching_count} records...")
    
    records_to_update = list(email_collection.find(
        filter_query,
        {"_id": 1, "dominant_topic": 1, "category": 1, "stage": 1, "resolution_status": 1}
    ))
    
    print(f"Retrieved {len(records_to_update)} records for update")
    
    # Show sample of records being updated
    print(f"\nStep 3: Sample of records to be updated:")
    print("-" * 70)
    for i, record in enumerate(records_to_update[:5]):  # Show first 5
        topic = record.get("dominant_topic", "Unknown")
        category = record.get("category", "Unknown")
        current_stage = record.get("stage", "Not Set")
        current_resolution = record.get("resolution_status", "Not Set")
        print(f"{i+1}. ID: {record['_id']} | Topic: {topic} | Category: {category}")
        print(f"   Current Stage: {current_stage} | Current Resolution: {current_resolution}")
    
    if len(records_to_update) > 5:
        print(f"... and {len(records_to_update) - 5} more records")
    
    # Define stage options
    stage_options = ["Receive", "Authenticate", "Categorize", "Resolution", "Escalation", "Update"]
    
    # Calculate distribution percentages (you can modify these)
    stage_distribution = {
        "Receive": 0.20,      # 20% of records
        "Authenticate": 0.15,  # 15% of records
        "Categorize": 0.15,    # 15% of records
        "Resolution": 0.25,    # 25% of records
        "Escalation": 0.15,    # 15% of records
        "Update": 0.10         # 10% of records
    }
    
    print(f"\nStep 4: Stage distribution plan:")
    print("-" * 70)
    for stage, percentage in stage_distribution.items():
        count = int(matching_count * percentage)
        print(f"  {stage}: {count} records ({percentage*100:.0f}%)")
    
    # Confirm before proceeding
    print(f"\nStep 5: Confirmation")
    print(f"About to update {len(records_to_update)} records:")
    print("  - Assign random stages: Receive, Authenticate, Categorize, Resolution, Escalation, Update")
    print("  - Set resolution_status to 'open' for all records")
    print("  - Only affect records with follow_up_required='yes' and action_pending_status='yes'")
    
    proceed = input(f"\nProceed with the updates? (y/n): ").lower().strip() == 'y'
    
    if not proceed:
        print("Operation cancelled.")
        return
    
    # Perform the updates
    print(f"\nStep 6: Updating records...")
    
    updated_count = 0
    failed_count = 0
    stage_stats = {"Receive": 0, "Authenticate": 0, "Categorize": 0, "Resolution": 0, "Escalation": 0, "Update": 0}
    category_stats = {}
    
    # Shuffle the records for random distribution
    random.shuffle(records_to_update)
    
    # Calculate exact counts for each stage
    receive_count = int(len(records_to_update) * stage_distribution["Receive"])
    authenticate_count = int(len(records_to_update) * stage_distribution["Authenticate"])
    categorize_count = int(len(records_to_update) * stage_distribution["Categorize"])
    resolution_count = int(len(records_to_update) * stage_distribution["Resolution"])
    escalation_count = int(len(records_to_update) * stage_distribution["Escalation"])
    update_count = len(records_to_update) - receive_count - authenticate_count - categorize_count - resolution_count - escalation_count  # Remaining records
    
    print(f"Target distribution:")
    print(f"  Receive: {receive_count} records")
    print(f"  Authenticate: {authenticate_count} records")
    print(f"  Categorize: {categorize_count} records")
    print(f"  Resolution: {resolution_count} records")
    print(f"  Escalation: {escalation_count} records")
    print(f"  Update: {update_count} records")
    print(f"  Total: {receive_count + authenticate_count + categorize_count + resolution_count + escalation_count + update_count} records")
    
    for i, record in enumerate(records_to_update):
        try:
            # Determine stage based on position
            if i < receive_count:
                new_stage = "Receive"
            elif i < receive_count + authenticate_count:
                new_stage = "Authenticate"
            elif i < receive_count + authenticate_count + categorize_count:
                new_stage = "Categorize"
            elif i < receive_count + authenticate_count + categorize_count + resolution_count:
                new_stage = "Resolution"
            elif i < receive_count + authenticate_count + categorize_count + resolution_count + escalation_count:
                new_stage = "Escalation"
            else:
                new_stage = "Update"
            
            # Update the record
            update_result = email_collection.update_one(
                {"_id": record["_id"]},
                {
                    "$set": {
                        "stage": new_stage,
                        "resolution_status": "open"
                    }
                }
            )
            
            if update_result.modified_count > 0:
                updated_count += 1
                stage_stats[new_stage] += 1
                
                # Track statistics by category
                category = record.get("category", "Unknown")
                if category not in category_stats:
                    category_stats[category] = {"Receive": 0, "Authenticate": 0, "Categorize": 0, "Resolution": 0, "Escalation": 0, "Update": 0}
                category_stats[category][new_stage] += 1
                
            else:
                failed_count += 1
                print(f"⚠️  Failed to update record: {record['_id']}")
                
        except Exception as e:
            failed_count += 1
            print(f"❌ Error updating record {record['_id']}: {e}")
    
    # Display results
    print(f"\n" + "=" * 80)
    print("UPDATE RESULTS")
    print("=" * 80)
    
    print(f"Records processed: {len(records_to_update)}")
    print(f"Successfully updated: {updated_count}")
    print(f"Failed updates: {failed_count}")
    
    if updated_count > 0:
        print(f"\nStage distribution results:")
        print("-" * 70)
        for stage, count in stage_stats.items():
            percentage = (count / updated_count * 100) if updated_count > 0 else 0
            print(f"  {stage}: {count} records ({percentage:.1f}%)")
        
        print(f"\nCategory breakdown:")
        print("-" * 70)
        for category, stages in sorted(category_stats.items()):
            total = sum(stages.values())
            print(f"  {category}: {total} total")
            for stage, count in stages.items():
                if count > 0:
                    print(f"    {stage}: {count}")
    
    # Verify the changes
    print(f"\nStep 7: Verification")
    print("-" * 70)
    
    # Check records with our criteria and resolution_status = "open"
    open_records = email_collection.count_documents({
        "follow_up_required": "yes",
        "action_pending_status": "yes",
        "resolution_status": "open"
    })
    print(f"Records with action required + resolution_status='open': {open_records}")
    
    # Check stage distribution
    for stage in ["Receive", "Authenticate", "Categorize", "Resolution", "Escalation", "Update"]:
        stage_count = email_collection.count_documents({
            "follow_up_required": "yes",
            "action_pending_status": "yes",
            "stage": stage,
            "resolution_status": "open"
        })
        print(f"  {stage}: {stage_count} records")
    
    # Check total records with resolution_status = "open"
    total_open = email_collection.count_documents({"resolution_status": "open"})
    print(f"\nTotal records with resolution_status='open': {total_open}")
    
    print(f"\n✅ Update completed successfully!")
    print(f"   {updated_count} records updated with stages and resolution_status='open'")
    
    return updated_count

def show_current_status():
    """Show current status of action-required records"""
    
    print("=" * 80)
    print("CURRENT STATUS OF ACTION-REQUIRED RECORDS")
    print("=" * 80)
    
    # Total action-required records
    total_action_required = email_collection.count_documents({
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    })
    print(f"Total records with action required: {total_action_required}")
    
    # Check current stage distribution
    print(f"\nCurrent stage distribution for action-required records:")
    print("-" * 70)
    
    pipeline = [
        {"$match": {
            "follow_up_required": "yes",
            "action_pending_status": "yes"
        }},
        {"$group": {"_id": "$stage", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    stage_results = list(email_collection.aggregate(pipeline))
    
    for result in stage_results:
        stage = result['_id'] if result['_id'] else "Not Set"
        count = result['count']
        percentage = (count / total_action_required * 100) if total_action_required > 0 else 0
        print(f"  {stage}: {count} records ({percentage:.1f}%)")
    
    # Check current resolution_status distribution
    print(f"\nCurrent resolution_status distribution for action-required records:")
    print("-" * 70)
    
    pipeline = [
        {"$match": {
            "follow_up_required": "yes",
            "action_pending_status": "yes"
        }},
        {"$group": {"_id": "$resolution_status", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    resolution_results = list(email_collection.aggregate(pipeline))
    
    for result in resolution_results:
        resolution = result['_id'] if result['_id'] else "Not Set"
        count = result['count']
        percentage = (count / total_action_required * 100) if total_action_required > 0 else 0
        print(f"  {resolution}: {count} records ({percentage:.1f}%)")
    
    # Check by category
    print(f"\nAction-required records by category:")
    print("-" * 70)
    
    pipeline = [
        {"$match": {
            "follow_up_required": "yes",
            "action_pending_status": "yes"
        }},
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    category_results = list(email_collection.aggregate(pipeline))
    
    for result in category_results:
        category = result['_id'] if result['_id'] else "Unknown"
        count = result['count']
        percentage = (count / total_action_required * 100) if total_action_required > 0 else 0
        print(f"  {category}: {count} records ({percentage:.1f}%)")

if __name__ == "__main__":
    try:
        # Show current status first
        show_current_status()
        
        print(f"\n" + "=" * 80)
        print("STAGE ASSIGNMENT AND OPENING")
        print("=" * 80)
        
        # Run the update
        updated_count = assign_stages_and_open()
        
        if updated_count > 0:
            print(f"\n🎉 Successfully updated {updated_count} records!")
        else:
            print(f"\n⚠️  No records were updated.")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        client.close()
        print(f"\nDatabase connection closed.")
