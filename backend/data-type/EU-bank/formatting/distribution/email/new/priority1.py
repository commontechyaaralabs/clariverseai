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

# Priority mapping with exact distribution percentages for URGENT + ACTION REQUIRED emails
# Internal Topics with specific priority rates
INTERNAL_TOPICS = {
    "Legal Escalation": {"p1": 0.44, "p2": 0.39, "p3": 0.17, "p4": 0.00, "p5": 0.00},  # 44% P1, 39% P2, 17% P3
    "Quality Assurance Breach": {"p1": 0.27, "p2": 0.45, "p3": 0.28, "p4": 0.00, "p5": 0.00},  # 27% P1, 45% P2, 28% P3
    "Risk Management Alert": {"p1": 0.43, "p2": 0.43, "p3": 0.14, "p4": 0.00, "p5": 0.00},  # 43% P1, 43% P2, 14% P3
    "Technology Emergency": {"p1": 0.70, "p2": 0.30, "p3": 0.00, "p4": 0.00, "p5": 0.00}  # 70% P1, 30% P2, 0% P3
}

# External Topics with specific priority rates
EXTERNAL_TOPICS = {
    "Clearing House Problem": {"p1": 0.50, "p2": 0.50, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 50% P1, 50% P2, 0% P3
    "Compliance Monitoring Alert": {"p1": 0.43, "p2": 0.43, "p3": 0.14, "p4": 0.00, "p5": 0.00},  # 43% P1, 43% P2, 14% P3
    "Covenant Breach Alert": {"p1": 0.63, "p2": 0.37, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 63% P1, 37% P2, 0% P3
    "Customer Service Escalation": {"p1": 0.25, "p2": 0.50, "p3": 0.25, "p4": 0.00, "p5": 0.00},  # 25% P1, 50% P2, 25% P3
    "Cybersecurity Incident Report": {"p1": 1.00, "p2": 0.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 100% P1, 0% others
    "Data Breach Warning": {"p1": 1.00, "p2": 0.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 100% P1, 0% others
    "Executive Escalation Email": {"p1": 0.47, "p2": 0.41, "p3": 0.12, "p4": 0.00, "p5": 0.00},  # 47% P1, 41% P2, 12% P3
    "Payment Service Problem": {"p1": 0.33, "p2": 0.50, "p3": 0.17, "p4": 0.00, "p5": 0.00},  # 33% P1, 50% P2, 17% P3
    "Processing Delay Complaint": {"p1": 0.20, "p2": 0.60, "p3": 0.20, "p4": 0.00, "p5": 0.00},  # 20% P1, 60% P2, 20% P3
    "Sanctions Screening Alert": {"p1": 1.00, "p2": 0.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 100% P1, 0% others
    "Security Incident Alert": {"p1": 1.00, "p2": 0.00, "p3": 0.00, "p4": 0.00, "p5": 0.00},  # 100% P1, 0% others
    "System Outage Notification": {"p1": 0.67, "p2": 0.33, "p3": 0.00, "p4": 0.00, "p5": 0.00}  # 67% P1, 33% P2, 0% P3
}

# Combine all topics
ALL_TOPICS = {**INTERNAL_TOPICS, **EXTERNAL_TOPICS}

def add_priority_field():
    """Add priority field based on dominant_topic for urgent emails with action required"""
    
    print("=" * 80)
    print("ADDING PRIORITY FIELD FOR URGENT + ACTION REQUIRED EMAILS")
    print("=" * 80)
    
    # Filter records where urgency=true, follow_up_required="yes", action_pending_status="yes"
    print("Step 1: Filtering urgent emails with action required...")
    
    urgent_action_query = {
        "urgency": True,
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    }
    
    urgent_action_count = email_collection.count_documents(urgent_action_query)
    print(f"Found {urgent_action_count} records with urgency=true, follow_up_required='yes', action_pending_status='yes'")
    
    if urgent_action_count == 0:
        print("No urgent action-required records found. Exiting...")
        return
    
    # Show current field status
    print(f"\nStep 2: Checking current priority field status...")
    priority_exists = email_collection.count_documents({"priority": {"$exists": True}})
    print(f"Records with priority field: {priority_exists}")
    
    # Get urgent action-required records with dominant_topic
    print(f"\nStep 3: Processing urgent action-required records...")
    
    urgent_action_records = list(email_collection.find(
        {
            "urgency": True,
            "follow_up_required": "yes",
            "action_pending_status": "yes",
            "dominant_topic": {"$exists": True}
        },
        {"_id": 1, "dominant_topic": 1, "category": 1, "priority": 1}
    ))
    
    print(f"Found {len(urgent_action_records)} urgent action-required records with dominant_topic")
    
    # Process each record with proper distribution
    updated_count = 0
    skipped_count = 0
    topic_stats = {}
    
    # Group records by topic for proper distribution
    topic_groups = {}
    for record in urgent_action_records:
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
            # For urgent emails, only P1, P2, P3 are allowed (P4=0, P5=0)
            # Use exact counts from the distribution specification
            if dominant_topic == "Legal Escalation":
                p1_count = 8
                p2_count = 7
                p3_count = 3
            elif dominant_topic == "Quality Assurance Breach":
                p1_count = 3
                p2_count = 5
                p3_count = 3
            elif dominant_topic == "Risk Management Alert":
                p1_count = 6
                p2_count = 6
                p3_count = 2
            elif dominant_topic == "Technology Emergency":
                p1_count = 7
                p2_count = 3
                p3_count = 0
            elif dominant_topic == "Clearing House Problem":
                p1_count = 2
                p2_count = 2
                p3_count = 0
            elif dominant_topic == "Compliance Monitoring Alert":
                p1_count = 3
                p2_count = 3
                p3_count = 1
            elif dominant_topic == "Covenant Breach Alert":
                p1_count = 5
                p2_count = 3
                p3_count = 0
            elif dominant_topic == "Customer Service Escalation":
                p1_count = 3
                p2_count = 6
                p3_count = 3
            elif dominant_topic == "Cybersecurity Incident Report":
                p1_count = 9
                p2_count = 0
                p3_count = 0
            elif dominant_topic == "Data Breach Warning":
                p1_count = 12
                p2_count = 0
                p3_count = 0
            elif dominant_topic == "Executive Escalation Email":
                p1_count = 8
                p2_count = 7
                p3_count = 2
            elif dominant_topic == "Payment Service Problem":
                p1_count = 2
                p2_count = 3
                p3_count = 1
            elif dominant_topic == "Processing Delay Complaint":
                p1_count = 1
                p2_count = 3
                p3_count = 1
            elif dominant_topic == "Sanctions Screening Alert":
                p1_count = 9
                p2_count = 0
                p3_count = 0
            elif dominant_topic == "Security Incident Alert":
                p1_count = 6
                p2_count = 0
                p3_count = 0
            elif dominant_topic == "System Outage Notification":
                p1_count = 4
                p2_count = 2
                p3_count = 0
            else:
                # Fallback to percentage-based calculation for unknown topics
                p1_count = int(total_records * p1_rate)
                p2_count = int(total_records * p2_rate)
                p3_count = total_records - p1_count - p2_count
            
            p4_count = 0  # Always 0 for urgent emails
            p5_count = 0  # Always 0 for urgent emails
            
            print(f"  {dominant_topic}: {total_records} records → P1:{p1_count}, P2:{p2_count}, P3:{p3_count}, P4:{p4_count}, P5:{p5_count}")
            
            # Process records for this topic
            for i, record in enumerate(records):
                # Determine priority value based on position in the list
                # For urgent emails, only P1, P2, P3 are assigned
                if i < p1_count:
                    priority_value = "P1-Critical"
                elif i < p1_count + p2_count:
                    priority_value = "P2-High"
                else:
                    priority_value = "P3-Medium"
                
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
    
    print(f"Total urgent action-required records processed: {len(urgent_action_records)}")
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
    print("PRIORITY MAPPING FOR URGENT + ACTION REQUIRED EMAILS")
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
    print("Priority Levels for URGENT emails:")
    print("  - P1-Critical: Immediate attention required")
    print("  - P2-High: High priority, resolve within hours")
    print("  - P3-Medium: Medium priority, resolve within days")
    print("  - P4-Low: NOT USED for urgent emails")
    print("  - P5-Very Low: NOT USED for urgent emails")
    print("Unknown topics will get 'P3-Medium' priority")

if __name__ == "__main__":
    try:
        # Show the mapping first
        show_priority_mapping()
        
        # Confirm before proceeding
        print(f"\nProceed with adding priority field to urgent action-required records? (y/n): ", end="")
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