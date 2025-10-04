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

# Priority mapping with exact distribution percentages for URGENT + NO ACTION REQUIRED emails
# Internal Topics with specific priority rates
INTERNAL_TOPICS = {
    "Legal Escalation": {"p1": 0.00, "p2": 0.25, "p3": 0.50, "p4": 0.25, "p5": 0.00},  # 0% P1, 25% P2, 50% P3, 25% P4
    "Quality Assurance Breach": {"p1": 0.00, "p2": 0.00, "p3": 0.67, "p4": 0.33, "p5": 0.00},  # 0% P1, 0% P2, 67% P3, 33% P4
    "Risk Management Alert": {"p1": 0.00, "p2": 0.50, "p3": 0.50, "p4": 0.00, "p5": 0.00},  # 0% P1, 50% P2, 50% P3, 0% P4
    "Technology Emergency": {"p1": 0.00, "p2": 1.00, "p3": 0.00, "p4": 0.00, "p5": 0.00}  # 0% P1, 100% P2, 0% P3, 0% P4
}

# External Topics with specific priority rates
EXTERNAL_TOPICS = {
    "Clearing House Problem": {"p1": 0.00, "p2": 1.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 100% P2, 0% P3, 0% P4
    "Compliance Monitoring Alert": {"p1": 0.00, "p2": 1.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 100% P2, 0% P3, 0% P4
    "Covenant Breach Alert": {"p1": 0.00, "p2": 1.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 100% P2, 0% P3, 0% P4
    "Customer Service Escalation": {"p1": 0.00, "p2": 0.25, "p3": 0.50, "p4": 0.25, "p5": 0.00},  # 0% P1, 25% P2, 50% P3, 25% P4
    "Cybersecurity Incident Report": {"p1": 0.00, "p2": 0.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 0% P2, 0% P3, 0% P4 (no emails)
    "Data Breach Warning": {"p1": 0.00, "p2": 0.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 0% P2, 0% P3, 0% P4 (no emails)
    "Executive Escalation Email": {"p1": 0.00, "p2": 0.67, "p3": 0.33, "p4": 0.00, "p5": 0.00},  # 0% P1, 67% P2, 33% P3, 0% P4
    "Payment Service Problem": {"p1": 0.00, "p2": 0.50, "p3": 0.50, "p4": 0.00, "p5": 0.00},  # 0% P1, 50% P2, 50% P3, 0% P4
    "Processing Delay Complaint": {"p1": 0.00, "p2": 0.00, "p3": 1.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 0% P2, 100% P3, 0% P4
    "Sanctions Screening Alert": {"p1": 0.00, "p2": 1.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 100% P2, 0% P3, 0% P4
    "Security Incident Alert": {"p1": 0.00, "p2": 0.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 0% P1, 0% P2, 0% P3, 0% P4 (no emails)
    "System Outage Notification": {"p1": 0.00, "p2": 0.50, "p3": 0.50, "p4": 0.00, "p5": 0.00}  # 0% P1, 50% P2, 50% P3, 0% P4
}

# Combine all topics
ALL_TOPICS = {**INTERNAL_TOPICS, **EXTERNAL_TOPICS}

def add_priority_field():
    """Add priority field based on dominant_topic for urgent emails with no action required"""
    
    print("=" * 80)
    print("ADDING PRIORITY FIELD FOR URGENT + NO ACTION REQUIRED EMAILS")
    print("=" * 80)
    
    # Filter records where urgency=true, follow_up_required="no", action_pending_status="no"
    print("Step 1: Filtering urgent emails with no action required...")
    
    urgent_no_action_query = {
        "urgency": True,
        "follow_up_required": "no",
        "action_pending_status": "no"
    }
    
    urgent_no_action_count = email_collection.count_documents(urgent_no_action_query)
    print(f"Found {urgent_no_action_count} records with urgency=true, follow_up_required='no', action_pending_status='no'")
    
    if urgent_no_action_count == 0:
        print("No urgent no-action-required records found. Exiting...")
        return
    
    # Show current field status
    print(f"\nStep 2: Checking current priority field status...")
    priority_exists = email_collection.count_documents({"priority": {"$exists": True}})
    print(f"Records with priority field: {priority_exists}")
    
    # Get urgent no-action-required records with dominant_topic
    print(f"\nStep 3: Processing urgent no-action-required records...")
    
    urgent_no_action_records = list(email_collection.find(
        {
            "urgency": True,
            "follow_up_required": "no",
            "action_pending_status": "no",
            "dominant_topic": {"$exists": True}
        },
        {"_id": 1, "dominant_topic": 1, "category": 1, "priority": 1}
    ))
    
    print(f"Found {len(urgent_no_action_records)} urgent no-action-required records with dominant_topic")
    
    # Process each record with proper distribution
    updated_count = 0
    skipped_count = 0
    topic_stats = {}
    
    # Group records by topic for proper distribution
    topic_groups = {}
    for record in urgent_no_action_records:
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
            p1_rate = topic_config["p1"]
            p2_rate = topic_config["p2"]
            p3_rate = topic_config["p3"]
            p4_rate = topic_config["p4"]
            p5_rate = topic_config["p5"]
            
            # Calculate exact counts based on the distribution
            # For urgent no-action emails, use exact counts from the distribution specification
            if dominant_topic == "Legal Escalation":
                p1_count = 0
                p2_count = 1
                p3_count = 2
                p4_count = 1
            elif dominant_topic == "Quality Assurance Breach":
                p1_count = 0
                p2_count = 0
                p3_count = 2
                p4_count = 1
            elif dominant_topic == "Risk Management Alert":
                p1_count = 0
                p2_count = 1
                p3_count = 1
                p4_count = 0
            elif dominant_topic == "Technology Emergency":
                p1_count = 0
                p2_count = 1
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "Clearing House Problem":
                p1_count = 0
                p2_count = 1
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "Compliance Monitoring Alert":
                p1_count = 0
                p2_count = 1
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "Covenant Breach Alert":
                p1_count = 0
                p2_count = 1
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "Customer Service Escalation":
                p1_count = 0
                p2_count = 1
                p3_count = 2
                p4_count = 1
            elif dominant_topic == "Cybersecurity Incident Report":
                p1_count = 0
                p2_count = 0
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "Data Breach Warning":
                p1_count = 0
                p2_count = 0
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "Executive Escalation Email":
                p1_count = 0
                p2_count = 2
                p3_count = 1
                p4_count = 0
            elif dominant_topic == "Payment Service Problem":
                p1_count = 0
                p2_count = 1
                p3_count = 1
                p4_count = 0
            elif dominant_topic == "Processing Delay Complaint":
                p1_count = 0
                p2_count = 0
                p3_count = 2
                p4_count = 0
            elif dominant_topic == "Sanctions Screening Alert":
                p1_count = 0
                p2_count = 1
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "Security Incident Alert":
                p1_count = 0
                p2_count = 0
                p3_count = 0
                p4_count = 0
            elif dominant_topic == "System Outage Notification":
                p1_count = 0
                p2_count = 1
                p3_count = 1
                p4_count = 0
            else:
                # Fallback to percentage-based calculation for unknown topics
                p1_count = int(total_records * p1_rate)
                p2_count = int(total_records * p2_rate)
                p3_count = int(total_records * p3_rate)
                p4_count = total_records - p1_count - p2_count - p3_count
            
            p5_count = 0  # Always 0 for urgent emails - NO P5 assignments
            
            # Ensure total counts match actual records
            calculated_total = p1_count + p2_count + p3_count + p4_count + p5_count
            if calculated_total != total_records:
                print(f"⚠️  Warning: Count mismatch for {dominant_topic}. Expected {total_records}, calculated {calculated_total}")
                # Adjust p4_count to match total
                p4_count = total_records - p1_count - p2_count - p3_count - p5_count
            
            print(f"  {dominant_topic}: {total_records} records → P1:{p1_count}, P2:{p2_count}, P3:{p3_count}, P4:{p4_count}, P5:{p5_count}")
            
            # Process records for this topic
            for i, record in enumerate(records):
                # Determine priority value based on position in the list
                # For urgent no-action emails, only P1, P2, P3, P4 can be assigned (NO P5)
                if i < p1_count:
                    priority_value = "P1-Critical"
                elif i < p1_count + p2_count:
                    priority_value = "P2-High"
                elif i < p1_count + p2_count + p3_count:
                    priority_value = "P3-Medium"
                elif i < p1_count + p2_count + p3_count + p4_count:
                    priority_value = "P4-Low"
                else:
                    # This should never happen if counts are correct, but fallback to P4-Low
                    priority_value = "P4-Low"
                    print(f"⚠️  Warning: Unexpected record {i} for {dominant_topic} - assigning P4-Low")
                
                # Check if field already exists with correct value
                current_priority = record.get("priority")
                
                # Skip if field already exists with correct value
                if current_priority == priority_value:
                    skipped_count += 1
                    continue
                
                # Update the record
                update_result = email_collection.update_one(
                    {"_id": record["_id"]},
                    {"$set": {"priority": priority_value}}
                )
                
                if update_result.modified_count > 0:
                    updated_count += 1
                    
                    # Track statistics
                    if dominant_topic not in topic_stats:
                        topic_stats[dominant_topic] = {
                            "total": 0, "p1_count": 0, "p2_count": 0, "p3_count": 0, 
                            "p4_count": 0, "p5_count": 0, "updated": 0, "category": category
                        }
                    topic_stats[dominant_topic]["total"] += 1
                    topic_stats[dominant_topic]["updated"] += 1
                    
                    if priority_value == "P1-Critical":
                        topic_stats[dominant_topic]["p1_count"] += 1
                    elif priority_value == "P2-High":
                        topic_stats[dominant_topic]["p2_count"] += 1
                    elif priority_value == "P3-Medium":
                        topic_stats[dominant_topic]["p3_count"] += 1
                    elif priority_value == "P4-Low":
                        topic_stats[dominant_topic]["p4_count"] += 1
                    else:
                        topic_stats[dominant_topic]["p5_count"] += 1
        else:
            # Topic not in mapping - set default value to P3-Medium
            print(f"  {dominant_topic}: {total_records} records → P1:0, P2:0, P3:{total_records}, P4:0, P5:0 (unknown topic)")
            
            for record in records:
                update_result = email_collection.update_one(
                    {"_id": record["_id"]},
                    {"$set": {"priority": "P3-Medium"}}
                )
                
                if update_result.modified_count > 0:
                    updated_count += 1
                    print(f"⚠️  Unknown topic '{dominant_topic}' - set to 'P3-Medium'")
    
    # Display results
    print(f"\n" + "=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    
    print(f"Total urgent no-action-required records processed: {len(urgent_no_action_records)}")
    print(f"Records updated: {updated_count}")
    print(f"Records skipped (already correct): {skipped_count}")
    
    print(f"\nTopic Statistics:")
    print("-" * 70)
    
    # Sort topics by category and name
    internal_topics = [(topic, stats) for topic, stats in topic_stats.items() 
                      if stats["category"] == "Internal"]
    external_topics = [(topic, stats) for topic, stats in topic_stats.items() 
                      if stats["category"] == "External"]
    
    if internal_topics:
        print("Internal Topics:")
        for topic, stats in sorted(internal_topics):
            p1_pct = (stats['p1_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p2_pct = (stats['p2_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p3_pct = (stats['p3_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p4_pct = (stats['p4_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p5_pct = (stats['p5_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {topic}: {stats['total']} total → P1:{stats['p1_count']}({p1_pct:.0f}%), P2:{stats['p2_count']}({p2_pct:.0f}%), P3:{stats['p3_count']}({p3_pct:.0f}%), P4:{stats['p4_count']}({p4_pct:.0f}%), P5:{stats['p5_count']}({p5_pct:.0f}%)")
    
    if external_topics:
        print("External Topics:")
        for topic, stats in sorted(external_topics):
            p1_pct = (stats['p1_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p2_pct = (stats['p2_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p3_pct = (stats['p3_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p4_pct = (stats['p4_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            p5_pct = (stats['p5_count'] / stats['total'] * 100) if stats['total'] > 0 else 0
            print(f"  {topic}: {stats['total']} total → P1:{stats['p1_count']}({p1_pct:.0f}%), P2:{stats['p2_count']}({p2_pct:.0f}%), P3:{stats['p3_count']}({p3_pct:.0f}%), P4:{stats['p4_count']}({p4_pct:.0f}%), P5:{stats['p5_count']}({p5_pct:.0f}%)")
    
    # Calculate subtotals
    total_p1 = sum(stats['p1_count'] for stats in topic_stats.values())
    total_p2 = sum(stats['p2_count'] for stats in topic_stats.values())
    total_p3 = sum(stats['p3_count'] for stats in topic_stats.values())
    total_p4 = sum(stats['p4_count'] for stats in topic_stats.values())
    total_p5 = sum(stats['p5_count'] for stats in topic_stats.values())
    total_all = total_p1 + total_p2 + total_p3 + total_p4 + total_p5
    
    print(f"\nOverall Distribution:")
    print("-" * 70)
    if total_all > 0:
        print(f"P1-Critical: {total_p1} ({total_p1/total_all*100:.0f}%)")
        print(f"P2-High: {total_p2} ({total_p2/total_all*100:.0f}%)")
        print(f"P3-Medium: {total_p3} ({total_p3/total_all*100:.0f}%)")
        print(f"P4-Low: {total_p4} ({total_p4/total_all*100:.0f}%)")
        print(f"P5-Very Low: {total_p5} ({total_p5/total_all*100:.0f}%)")
    
    # Verify final counts
    print(f"\nFinal Verification:")
    final_p1 = email_collection.count_documents({"priority": "P1-Critical"})
    final_p2 = email_collection.count_documents({"priority": "P2-High"})
    final_p3 = email_collection.count_documents({"priority": "P3-Medium"})
    final_p4 = email_collection.count_documents({"priority": "P4-Low"})
    final_p5 = email_collection.count_documents({"priority": "P5-Very Low"})
    
    print(f"P1-Critical: {final_p1}")
    print(f"P2-High: {final_p2}")
    print(f"P3-Medium: {final_p3}")
    print(f"P4-Low: {final_p4}")
    print(f"P5-Very Low: {final_p5}")
    
    return updated_count

def show_priority_mapping():
    """Display the priority mapping that will be used"""
    
    print("=" * 80)
    print("PRIORITY MAPPING FOR URGENT + NO ACTION REQUIRED EMAILS")
    print("=" * 80)
    
    print("Internal Topics with Priority Distribution:")
    print("-" * 70)
    for i, (topic, config) in enumerate(sorted(INTERNAL_TOPICS.items()), 1):
        p1_pct = config["p1"] * 100
        p2_pct = config["p2"] * 100
        p3_pct = config["p3"] * 100
        p4_pct = config["p4"] * 100
        p5_pct = config["p5"] * 100
        print(f"{i:2d}. {topic}: P1:{p1_pct:.0f}%, P2:{p2_pct:.0f}%, P3:{p3_pct:.0f}%, P4:{p4_pct:.0f}%, P5:{p5_pct:.0f}%")
    
    print(f"\nExternal Topics with Priority Distribution:")
    print("-" * 70)
    for i, (topic, config) in enumerate(sorted(EXTERNAL_TOPICS.items()), 1):
        p1_pct = config["p1"] * 100
        p2_pct = config["p2"] * 100
        p3_pct = config["p3"] * 100
        p4_pct = config["p4"] * 100
        p5_pct = config["p5"] * 100
        print(f"{i:2d}. {topic}: P1:{p1_pct:.0f}%, P2:{p2_pct:.0f}%, P3:{p3_pct:.0f}%, P4:{p4_pct:.0f}%, P5:{p5_pct:.0f}%")
    
    print(f"\nTotal mapped topics: {len(ALL_TOPICS)}")
    print("Priority Levels for URGENT NO-ACTION emails:")
    print("  - P1-Critical: NOT USED for no-action emails")
    print("  - P2-High: High priority for awareness/documentation")
    print("  - P3-Medium: Medium priority for records")
    print("  - P4-Low: Low priority for administrative closure")
    print("  - P5-Very Low: NOT USED for urgent emails")
    print("Unknown topics will get 'P3-Medium' priority")

if __name__ == "__main__":
    try:
        # Show the mapping first
        show_priority_mapping()
        
        # Confirm before proceeding
        print(f"\nProceed with adding priority field to urgent no-action-required records? (y/n): ", end="")
        proceed = input().lower().strip() == 'y'
        
        if proceed:
            updated_count = add_priority_field()
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