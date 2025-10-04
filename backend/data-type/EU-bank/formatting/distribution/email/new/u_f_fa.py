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

# Topic mapping with exact distribution percentages for NON-URGENT emails
# Internal Topics with specific follow-up rates
INTERNAL_TOPICS = {
    "User Access Approval": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "IT Marketing Unresolved": {"yes_rate": 0.67, "no_rate": 0.33},  # 67% yes, 33% no
    "IT Access Problem": {"yes_rate": 0.77, "no_rate": 0.23},  # 77% yes, 23% no
    "Salary Leave Issues": {"yes_rate": 0.70, "no_rate": 0.30},  # 70% yes, 30% no
    "Vendor Management": {"yes_rate": 0.68, "no_rate": 0.32},  # 68% yes, 32% no
    "Process Exception": {"yes_rate": 0.77, "no_rate": 0.23},  # 77% yes, 23% no
    "Performance Management": {"yes_rate": 0.64, "no_rate": 0.36},  # 64% yes, 36% no
    "Executive Decision Required": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Risk Management Alert": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Quality Assurance Breach": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Training Need Alert": {"yes_rate": 0.62, "no_rate": 0.38},  # 62% yes, 38% no
    "Approval Required": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Internal Audit": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Compliance Issues": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Staff Performance Issue": {"yes_rate": 0.70, "no_rate": 0.30},  # 70% yes, 30% no
    "Technology Emergency": {"yes_rate": 0.75, "no_rate": 0.25}  # 75% yes, 25% no
}

# External Topics with specific follow-up rates
EXTERNAL_TOPICS = {
    "Covenant Compliance Question": {"yes_rate": 0.65, "no_rate": 0.35},  # 65% yes, 35% no
    "Consent Withdrawal Notification": {"yes_rate": 0.90, "no_rate": 0.10},  # 90% yes, 10% no
    "ECB Policy Update": {"yes_rate": 0.40, "no_rate": 0.60},  # 40% yes, 60% no
    "GDPR Access Request": {"yes_rate": 0.90, "no_rate": 0.10},  # 90% yes, 10% no
    "MiFID Investment Inquiry": {"yes_rate": 0.63, "no_rate": 0.37},  # 63% yes, 37% no
    "CRS Reporting Question": {"yes_rate": 0.68, "no_rate": 0.32},  # 68% yes, 32% no
    "Account Access Error": {"yes_rate": 0.79, "no_rate": 0.21},  # 79% yes, 21% no
    "SEPA Payment Failure": {"yes_rate": 0.84, "no_rate": 0.16},  # 84% yes, 16% no
    "Credit Line Utilization": {"yes_rate": 0.58, "no_rate": 0.42},  # 58% yes, 42% no
    "Letter Credit Amendment": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "KYC Documentation Problem": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Account Opening Issue": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "Relationship Manager Request": {"yes_rate": 0.65, "no_rate": 0.35},  # 65% yes, 35% no
    "Cross-Border Transfer Problem": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "API Connectivity Issue": {"yes_rate": 0.76, "no_rate": 0.24},  # 76% yes, 24% no
    "Mobile Authentication Error": {"yes_rate": 0.81, "no_rate": 0.19},  # 81% yes, 19% no
    "Digital Platform Malfunction": {"yes_rate": 0.81, "no_rate": 0.19},  # 81% yes, 19% no
    "Transaction Processing Error": {"yes_rate": 0.81, "no_rate": 0.19},  # 81% yes, 19% no
    "Fintech Integration Error": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Regulatory Documentation Need": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Strong Customer Authentication": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Foreign Exchange Problem": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Fund Transfer Problem": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "International Transfer Question": {"yes_rate": 0.60, "no_rate": 0.40},  # 60% yes, 40% no
    "Reporting Platform Issue": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Multi-Currency Statement Need": {"yes_rate": 0.67, "no_rate": 0.33},  # 67% yes, 33% no
    "Online Banking Problem": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Clearing House Problem": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Export Documentation Error": {"yes_rate": 0.79, "no_rate": 0.21},  # 79% yes, 21% no
    "Derivatives Question Alert": {"yes_rate": 0.64, "no_rate": 0.36},  # 64% yes, 36% no
    "RegTech Alert": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Client Categorization Update": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "SEPA Payment Status": {"yes_rate": 0.57, "no_rate": 0.43},  # 57% yes, 43% no
    "Third Party Provider": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Export Credit Issue": {"yes_rate": 0.79, "no_rate": 0.21},  # 79% yes, 21% no
    "Card Payment Dispute": {"yes_rate": 0.79, "no_rate": 0.21},  # 79% yes, 21% no
    "FATCA Compliance Issue": {"yes_rate": 0.86, "no_rate": 0.14},  # 86% yes, 14% no
    "Custody Service Problem": {"yes_rate": 0.79, "no_rate": 0.21},  # 79% yes, 21% no
    "Fee Clarification Need": {"yes_rate": 0.62, "no_rate": 0.38},  # 62% yes, 38% no
    "Process Improvement Request": {"yes_rate": 0.54, "no_rate": 0.46},  # 54% yes, 46% no
    "ACH Processing Error": {"yes_rate": 0.85, "no_rate": 0.15},  # 85% yes, 15% no
    "Merchant Service Problem": {"yes_rate": 0.77, "no_rate": 0.23},  # 77% yes, 23% no
    "Data Retention Question": {"yes_rate": 0.69, "no_rate": 0.31},  # 69% yes, 31% no
    "Import Finance Question": {"yes_rate": 0.69, "no_rate": 0.31},  # 69% yes, 31% no
    "Card Network Issue": {"yes_rate": 0.77, "no_rate": 0.23},  # 77% yes, 23% no
    "Monitoring System Alert": {"yes_rate": 0.69, "no_rate": 0.31},  # 69% yes, 31% no
    "Check Clearing Issue": {"yes_rate": 0.77, "no_rate": 0.23},  # 77% yes, 23% no
    "Legal Notice Response": {"yes_rate": 0.85, "no_rate": 0.15},  # 85% yes, 15% no
    "AML Documentation Request": {"yes_rate": 0.85, "no_rate": 0.15},  # 85% yes, 15% no
    "Fee Dispute Email": {"yes_rate": 0.77, "no_rate": 0.23},  # 77% yes, 23% no
    "Best Execution Report": {"yes_rate": 0.54, "no_rate": 0.46},  # 54% yes, 46% no
    "Digital Banking Error": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "SEPA Instant Failure": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Consent Management Dispute": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Instant Payment Rejected": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Private Banking Service": {"yes_rate": 0.67, "no_rate": 0.33},  # 67% yes, 33% no
    "Open Banking API": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Phone Banking Issue": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Integration Problem Report": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Personal Data Inquiry": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Commercial Paper Issue": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Direct Debit Returned": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Mobile App Issue": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Quality Assurance Issue": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "POS Terminal Issue": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "TPP Access Problem": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Corporate Account Issue": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Complaint Resolution Request": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Facility Renewal Inquiry": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Corporate Loan Inquiry": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Audit Trail Need": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Service Quality Complaint": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "EU Taxonomy Reporting": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Liquidity Facility Problem": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Processing Information Need": {"yes_rate": 0.64, "no_rate": 0.36},  # 64% yes, 36% no
    "Authentication Problem Report": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Basel III Requirement": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "TARGET2 Settlement Issue": {"yes_rate": 0.82, "no_rate": 0.18},  # 82% yes, 18% no
    "Deposit Service Issue": {"yes_rate": 0.73, "no_rate": 0.27},  # 73% yes, 27% no
    "Wealth Management Issue": {"yes_rate": 0.70, "no_rate": 0.30},  # 70% yes, 30% no
    "Risk Assessment Update": {"yes_rate": 0.70, "no_rate": 0.30},  # 70% yes, 30% no
    "Investment Advisory Question": {"yes_rate": 0.60, "no_rate": 0.40},  # 60% yes, 40% no
    "Tax Withholding Problem": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Cash Management Issue": {"yes_rate": 0.70, "no_rate": 0.30},  # 70% yes, 30% no
    "Core Banking System": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Wire Transfer Delay": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Compliance Certificate Request": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "SEPA Processing Error": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Data Sharing Problem": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Right Erasure Demand": {"yes_rate": 0.90, "no_rate": 0.10},  # 90% yes, 10% no
    "Service Level Breach": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Trade Finance Delay": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "EU Passporting Service": {"yes_rate": 0.67, "no_rate": 0.33},  # 67% yes, 33% no
    "Overdraft Facility Request": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "Reporting Discrepancy Alert": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "Documentary Collection Issue": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "Audit Firm Communication": {"yes_rate": 0.67, "no_rate": 0.33},  # 67% yes, 33% no
    "Digital Wallet Issue": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "ATM Network Problem": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "Processing Delay Complaint": {"yes_rate": 0.78, "no_rate": 0.22},  # 78% yes, 22% no
    "Legal Advisor Update": {"yes_rate": 0.56, "no_rate": 0.44},  # 56% yes, 44% no
    "Credit Facility Question": {"yes_rate": 0.67, "no_rate": 0.33},  # 67% yes, 33% no
    "Enhanced Due Diligence": {"yes_rate": 0.88, "no_rate": 0.12},  # 88% yes, 12% no
    "Cash Flow Issue": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Investment Service Question": {"yes_rate": 0.63, "no_rate": 0.37},  # 63% yes, 37% no
    "Balance Inquiry Problem": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Treasury Service Problem": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Treasury System Error": {"yes_rate": 0.88, "no_rate": 0.12},  # 88% yes, 12% no
    "Beneficial Ownership Declaration": {"yes_rate": 0.88, "no_rate": 0.12},  # 88% yes, 12% no
    "Transaction Dispute Report": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "FX Execution Problem": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Euro Clearing Problem": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "PSD2 Compliance Question": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Consulting Project Report": {"yes_rate": 0.50, "no_rate": 0.50},  # 50% yes, 50% no
    "Bond Trading Problem": {"yes_rate": 0.75, "no_rate": 0.25},  # 75% yes, 25% no
    "Certificate Deposit Question": {"yes_rate": 0.63, "no_rate": 0.37},  # 63% yes, 37% no
    "Asset Management Issue": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Covenant Breach Alert": {"yes_rate": 0.86, "no_rate": 0.14},  # 86% yes, 14% no
    "Interest Rate Query": {"yes_rate": 0.57, "no_rate": 0.43},  # 57% yes, 43% no
    "Cross-Border Regulatory Question": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Letter Credit Processing": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Media Inquiry Email": {"yes_rate": 0.86, "no_rate": 0.14},  # 86% yes, 14% no
    "Payment Service Problem": {"yes_rate": 0.71, "no_rate": 0.29},  # 71% yes, 29% no
    "Trade Documentation Error": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Guarantee Execution Problem": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Branch Service Complaint": {"yes_rate": 0.83, "no_rate": 0.17},  # 83% yes, 17% no
    "Compliance Monitoring Alert": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Satisfaction Survey Result": {"yes_rate": 0.40, "no_rate": 0.60},  # 40% yes, 60% no
    "Two-Factor Authentication Error": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Beneficiary Bank Unavailable": {"yes_rate": 0.80, "no_rate": 0.20},  # 80% yes, 20% no
    "Customer Feedback Report": {"yes_rate": 0.50, "no_rate": 0.50},  # 50% yes, 50% no
    "System Outage Notification": {"yes_rate": 0.67, "no_rate": 0.33},  # 67% yes, 33% no
    "Data Breach Warning": {"yes_rate": 1.00, "no_rate": 0.00},  # 100% yes, 0% no
    "Sanctions Screening Alert": {"yes_rate": 1.00, "no_rate": 0.00},  # 100% yes, 0% no
    "Security Incident Alert": {"yes_rate": 1.00, "no_rate": 0.00}  # 100% yes, 0% no
}

# Combine all topics
ALL_TOPICS = {**INTERNAL_TOPICS, **EXTERNAL_TOPICS}

def add_follow_up_fields():
    """Add follow_up_required and action_pending_status fields based on dominant_topic for non-urgent emails"""
    
    print("=" * 80)
    print("ADDING FOLLOW_UP_REQUIRED AND ACTION_PENDING_STATUS FIELDS FOR NON-URGENT EMAILS")
    print("=" * 80)
    
    # First, filter records where urgency is false
    print("Step 1: Filtering records where urgency = false...")
    
    non_urgent_query = {"urgency": False}
    non_urgent_count = email_collection.count_documents(non_urgent_query)
    print(f"Found {non_urgent_count} records with urgency = false")
    
    if non_urgent_count == 0:
        print("No non-urgent records found. Exiting...")
        return
    
    # Show current field status
    print(f"\nStep 2: Checking current field status...")
    follow_up_exists = email_collection.count_documents({"follow_up_required": {"$exists": True}})
    action_pending_exists = email_collection.count_documents({"action_pending_status": {"$exists": True}})
    
    print(f"Records with follow_up_required field: {follow_up_exists}")
    print(f"Records with action_pending_status field: {action_pending_exists}")
    
    # Get non-urgent records with dominant_topic
    print(f"\nStep 3: Processing non-urgent records...")
    
    non_urgent_records = list(email_collection.find(
        {"urgency": False, "dominant_topic": {"$exists": True}},
        {"_id": 1, "dominant_topic": 1, "category": 1, "follow_up_required": 1, "action_pending_status": 1}
    ))
    
    print(f"Found {len(non_urgent_records)} non-urgent records with dominant_topic")
    
    # Process each record with proper distribution
    updated_count = 0
    skipped_count = 0
    topic_stats = {}
    
    # Group records by topic for proper distribution
    topic_groups = {}
    for record in non_urgent_records:
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
    
    print(f"Total non-urgent records processed: {len(non_urgent_records)}")
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
    print("TOPIC MAPPING FOR NON-URGENT EMAILS - FOLLOW_UP_REQUIRED AND ACTION_PENDING_STATUS")
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
        print(f"\nProceed with adding fields to non-urgent records? (y/n): ", end="")
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