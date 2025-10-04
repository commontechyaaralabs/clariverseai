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

def assign_stages_and_close():
    """Assign random stages and set resolution_status to 'closed' for no-action records"""
    
    print("=" * 80)
    print("ASSIGNING STAGES AND CLOSING NO-ACTION RECORDS")
    print("=" * 80)
    
    # Filter criteria
    filter_query = {
        "follow_up_required": "no",
        "action_pending_status": "no"
    }
    
    print("Step 1: Finding records with no action required...")
    print(f"Filter: {filter_query}")
    
    # Count matching records
    matching_count = email_collection.count_documents(filter_query)
    print(f"Found {matching_count} records with follow_up_required='no' and action_pending_status='no'")
    
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
    stage_options = ["Resolved", "Close", "Report"]
    
    # Calculate distribution percentages (you can modify these)
    stage_distribution = {
        "Resolved": 0.40,  # 40% of records
        "Close": 0.35,     # 35% of records
        "Report": 0.25     # 25% of records
    }
    
    print(f"\nStep 4: Stage distribution plan:")
    print("-" * 70)
    for stage, percentage in stage_distribution.items():
        count = int(matching_count * percentage)
        print(f"  {stage}: {count} records ({percentage*100:.0f}%)")
    
    # Confirm before proceeding
    print(f"\nStep 5: Confirmation")
    print(f"About to update {len(records_to_update)} records:")
    print("  - Assign random stages: Resolved, Close, or Report")
    print("  - Set resolution_status to 'closed' for all records")
    print("  - Only affect records with follow_up_required='no' and action_pending_status='no'")
    
    proceed = input(f"\nProceed with the updates? (y/n): ").lower().strip() == 'y'
    
    if not proceed:
        print("Operation cancelled.")
        return
    
    # Perform the updates
    print(f"\nStep 6: Updating records...")
    
    updated_count = 0
    failed_count = 0
    stage_stats = {"Resolved": 0, "Close": 0, "Report": 0}
    category_stats = {}
    
    # Shuffle the records for random distribution
    random.shuffle(records_to_update)
    
    # Calculate exact counts for each stage
    resolved_count = int(len(records_to_update) * stage_distribution["Resolved"])
    close_count = int(len(records_to_update) * stage_distribution["Close"])
    report_count = len(records_to_update) - resolved_count - close_count  # Remaining records
    
    print(f"Target distribution:")
    print(f"  Resolved: {resolved_count} records")
    print(f"  Close: {close_count} records")
    print(f"  Report: {report_count} records")
    print(f"  Total: {resolved_count + close_count + report_count} records")
    
    for i, record in enumerate(records_to_update):
        try:
            # Determine stage based on position
            if i < resolved_count:
                new_stage = "Resolved"
            elif i < resolved_count + close_count:
                new_stage = "Close"
            else:
                new_stage = "Report"
            
            # Update the record
            update_result = email_collection.update_one(
                {"_id": record["_id"]},
                {
                    "$set": {
                        "stage": new_stage,
                        "resolution_status": "closed"
                    }
                }
            )
            
            if update_result.modified_count > 0:
                updated_count += 1
                stage_stats[new_stage] += 1
                
                # Track statistics by category
                category = record.get("category", "Unknown")
                if category not in category_stats:
                    category_stats[category] = {"Resolved": 0, "Close": 0, "Report": 0}
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
    
    # Check records with our criteria and resolution_status = "closed"
    closed_records = email_collection.count_documents({
        "follow_up_required": "no",
        "action_pending_status": "no",
        "resolution_status": "closed"
    })
    print(f"Records with no action + resolution_status='closed': {closed_records}")
    
    # Check stage distribution
    for stage in ["Resolved", "Close", "Report"]:
        stage_count = email_collection.count_documents({
            "follow_up_required": "no",
            "action_pending_status": "no",
            "stage": stage,
            "resolution_status": "closed"
        })
        print(f"  {stage}: {stage_count} records")
    
    # Check total records with resolution_status = "closed"
    total_closed = email_collection.count_documents({"resolution_status": "closed"})
    print(f"\nTotal records with resolution_status='closed': {total_closed}")
    
    print(f"\n✅ Update completed successfully!")
    print(f"   {updated_count} records updated with stages and resolution_status='closed'")
    
    return updated_count

def show_current_status():
    """Show current status of no-action records"""
    
    print("=" * 80)
    print("CURRENT STATUS OF NO-ACTION RECORDS")
    print("=" * 80)
    
    # Total no-action records
    total_no_action = email_collection.count_documents({
        "follow_up_required": "no",
        "action_pending_status": "no"
    })
    print(f"Total records with no action required: {total_no_action}")
    
    # Check current stage distribution
    print(f"\nCurrent stage distribution for no-action records:")
    print("-" * 70)
    
    pipeline = [
        {"$match": {
            "follow_up_required": "no",
            "action_pending_status": "no"
        }},
        {"$group": {"_id": "$stage", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    stage_results = list(email_collection.aggregate(pipeline))
    
    for result in stage_results:
        stage = result['_id'] if result['_id'] else "Not Set"
        count = result['count']
        percentage = (count / total_no_action * 100) if total_no_action > 0 else 0
        print(f"  {stage}: {count} records ({percentage:.1f}%)")
    
    # Check current resolution_status distribution
    print(f"\nCurrent resolution_status distribution for no-action records:")
    print("-" * 70)
    
    pipeline = [
        {"$match": {
            "follow_up_required": "no",
            "action_pending_status": "no"
        }},
        {"$group": {"_id": "$resolution_status", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    resolution_results = list(email_collection.aggregate(pipeline))
    
    for result in resolution_results:
        resolution = result['_id'] if result['_id'] else "Not Set"
        count = result['count']
        percentage = (count / total_no_action * 100) if total_no_action > 0 else 0
        print(f"  {resolution}: {count} records ({percentage:.1f}%)")
    
    # Check by category
    print(f"\nNo-action records by category:")
    print("-" * 70)
    
    pipeline = [
        {"$match": {
            "follow_up_required": "no",
            "action_pending_status": "no"
        }},
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}}
    ]
    
    category_results = list(email_collection.aggregate(pipeline))
    
    for result in category_results:
        category = result['_id'] if result['_id'] else "Unknown"
        count = result['count']
        percentage = (count / total_no_action * 100) if total_no_action > 0 else 0
        print(f"  {category}: {count} records ({percentage:.1f}%)")

if __name__ == "__main__":
    try:
        # Show current status first
        show_current_status()
        
        print(f"\n" + "=" * 80)
        print("STAGE ASSIGNMENT AND CLOSURE")
        print("=" * 80)
        
        # Run the update
        updated_count = assign_stages_and_close()
        
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
