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

# Priority mapping with exact distribution for NON-URGENT + NO ACTION REQUIRED emails
# Internal Topics with specific priority counts
INTERNAL_TOPICS = {
    "Performance Management": {"p1": 0, "p2": 0, "p3": 0, "p4": 8, "p5": 0},  # Total 8
    "Vendor Management": {"p1": 0, "p2": 0, "p3": 0, "p4": 8, "p5": 0},  # Total 8
    "Internal Audit": {"p1": 0, "p2": 0, "p3": 0, "p4": 3, "p5": 0},  # Total 3
    "Staff Performance Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 3, "p5": 0},  # Total 3
    "User Access Approval": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 9},  # Total 9
    "IT Marketing Unresolved": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 9},  # Total 9
    "Salary Leave Issues": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 7},  # Total 7
    "Process Exception": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 6},  # Total 6
    "IT Access Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 6},  # Total 6
    "Training Need Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Risk Management Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Quality Assurance Breach": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Executive Decision Required": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Compliance Issues": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Approval Required": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Technology Emergency": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2}  # Total 2
}

# External Topics with specific priority counts
EXTERNAL_TOPICS = {
    "ECB Policy Update": {"p1": 0, "p2": 0, "p3": 0, "p4": 12, "p5": 0},  # Total 12
    "MiFID Investment Inquiry": {"p1": 0, "p2": 0, "p3": 0, "p4": 8, "p5": 0},  # Total 8
    "Credit Line Utilization": {"p1": 0, "p2": 0, "p3": 0, "p4": 8, "p5": 0},  # Total 8
    "CRS Reporting Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 7, "p5": 0},  # Total 7
    "Covenant Compliance Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 7, "p5": 0},  # Total 7
    "Best Execution Report": {"p1": 0, "p2": 0, "p3": 0, "p4": 6, "p5": 0},  # Total 6
    "Regulatory Documentation Need": {"p1": 0, "p2": 0, "p3": 0, "p4": 3, "p5": 0},  # Total 3
    "Risk Assessment Update": {"p1": 0, "p2": 0, "p3": 0, "p4": 3, "p5": 0},  # Total 3
    "Basel III Requirement": {"p1": 0, "p2": 0, "p3": 0, "p4": 2, "p5": 0},  # Total 2
    "EU Taxonomy Reporting": {"p1": 0, "p2": 0, "p3": 0, "p4": 2, "p5": 0},  # Total 2
    "Audit Trail Need": {"p1": 0, "p2": 0, "p3": 0, "p4": 2, "p5": 0},  # Total 2
    "SEPA Payment Status": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 7},  # Total 7
    "Derivatives Question Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 6},  # Total 6
    "Relationship Manager Request": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 6},  # Total 6
    "Process Improvement Request": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 6},  # Total 6
    "International Transfer Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 6},  # Total 6
    "Import Finance Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "RegTech Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Client Categorization Update": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Data Retention Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Fee Clarification Need": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Clearing House Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Fintech Integration Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Multi-Currency Statement Need": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Third Party Provider": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Monitoring System Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "API Connectivity Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Reporting Platform Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Foreign Exchange Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 5},  # Total 5
    "Private Banking Service": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Letter Credit Amendment": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "KYC Documentation Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Mobile Authentication Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Investment Advisory Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Interest Rate Query": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Processing Information Need": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Account Opening Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Consulting Project Report": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Transaction Processing Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "SEPA Payment Failure": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Legal Advisor Update": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Account Access Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Digital Platform Malfunction": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Cross-Border Transfer Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 4},  # Total 4
    "Service Quality Complaint": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Open Banking API": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Phone Banking Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Facility Renewal Inquiry": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Integration Problem Report": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Cash Management Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Personal Data Inquiry": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Strong Customer Authentication": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Audit Firm Communication": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Investment Service Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Consent Management Dispute": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Instant Payment Rejected": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Corporate Account Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Export Documentation Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "POS Terminal Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Satisfaction Survey Result": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "EU Passporting Service": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Digital Banking Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Wealth Management Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Merchant Service Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Asset Management Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "SEPA Instant Failure": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Quality Assurance Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Custody Service Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Certificate Deposit Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Payment Service Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Deposit Service Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Export Credit Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Letter Credit Processing": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Card Payment Dispute": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Fee Dispute Email": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Mobile App Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Online Banking Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Check Clearing Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Direct Debit Returned": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Credit Facility Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Commercial Paper Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Corporate Loan Inquiry": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Card Network Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Cross-Border Regulatory Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Fund Transfer Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 3},  # Total 3
    "Treasury Service Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Consent Withdrawal Notification": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Customer Feedback Report": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Transaction Dispute Report": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "TPP Access Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Balance Inquiry Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Complaint Resolution Request": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Digital Wallet Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Tax Withholding Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "ATM Network Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Cash Flow Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Documentary Collection Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Trade Documentation Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Overdraft Facility Request": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "ACH Processing Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Reporting Discrepancy Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Compliance Certificate Request": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "FATCA Compliance Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Authentication Problem Report": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "SEPA Processing Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Data Sharing Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Branch Service Complaint": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "TARGET2 Settlement Issue": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Service Level Breach": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Trade Finance Delay": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "AML Documentation Request": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Bond Trading Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "GDPR Access Request": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Guarantee Execution Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Legal Notice Response": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "PSD2 Compliance Question": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Wire Transfer Delay": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "FX Execution Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Liquidity Facility Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Core Banking System": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Processing Delay Complaint": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Euro Clearing Problem": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 2},  # Total 2
    "Treasury System Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Beneficial Ownership Declaration": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Two-Factor Authentication Error": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Compliance Monitoring Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "System Outage Notification": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Covenant Breach Alert": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Enhanced Due Diligence": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Media Inquiry Email": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Right Erasure Demand": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1},  # Total 1
    "Beneficiary Bank Unavailable": {"p1": 0, "p2": 0, "p3": 0, "p4": 0, "p5": 1}  # Total 1
}

# Combine all topics
ALL_TOPICS = {**INTERNAL_TOPICS, **EXTERNAL_TOPICS}

def add_priority_field():
    """Add priority field based on dominant_topic for non-urgent emails with no action required"""
    
    print("=" * 80)
    print("ADDING PRIORITY FIELD FOR NON-URGENT + NO ACTION REQUIRED EMAILS")
    print("=" * 80)
    
    # Filter records where urgency=false, follow_up_required="no", action_pending_status="no"
    print("Step 1: Filtering non-urgent emails with no action required...")
    
    non_urgent_no_action_query = {
        "urgency": False,
        "follow_up_required": "no",
        "action_pending_status": "no"
    }
    
    non_urgent_no_action_count = email_collection.count_documents(non_urgent_no_action_query)
    print(f"Found {non_urgent_no_action_count} records with urgency=false, follow_up_required='no', action_pending_status='no'")
    
    if non_urgent_no_action_count == 0:
        print("No non-urgent no-action-required records found. Exiting...")
        return
    
    # Show current field status
    print(f"\nStep 2: Checking current priority field status...")
    priority_exists = email_collection.count_documents({"priority": {"$exists": True}})
    print(f"Records with priority field: {priority_exists}")
    
    # Get non-urgent no-action-required records with dominant_topic
    print(f"\nStep 3: Processing non-urgent no-action-required records...")
    
    non_urgent_no_action_records = list(email_collection.find(
        {
            "urgency": False,
            "follow_up_required": "no",
            "action_pending_status": "no",
            "dominant_topic": {"$exists": True}
        },
        {"_id": 1, "dominant_topic": 1, "category": 1, "priority": 1}
    ))
    
    print(f"Found {len(non_urgent_no_action_records)} non-urgent no-action-required records with dominant_topic")
    
    # Process each record with proper distribution
    updated_count = 0
    skipped_count = 0
    topic_stats = {}
    
    # Group records by topic for proper distribution
    topic_groups = {}
    for record in non_urgent_no_action_records:
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
            p1_count = topic_config["p1"]
            p2_count = topic_config["p2"]
            p3_count = topic_config["p3"]
            p4_count = topic_config["p4"]
            p5_count = topic_config["p5"]
            
            # Ensure total counts match actual records
            calculated_total = p1_count + p2_count + p3_count + p4_count + p5_count
            if calculated_total != total_records:
                print(f"⚠️  Warning: Count mismatch for {dominant_topic}. Expected {total_records}, calculated {calculated_total}")
                
                # Adjust counts based on the difference
                difference = total_records - calculated_total
                
                if difference > 0:  # More records than expected - add excess to P5
                    print(f"  Adding {difference} excess records to P5")
                    p5_count += difference
                        
                else:  # Fewer records than expected - reduce from P5 first, then P4
                    difference = abs(difference)
                    print(f"  Reducing {difference} records from P5, P4 in that order")
                    
                    if difference > 0 and p5_count > 0:
                        reduce_p5 = min(difference, p5_count)
                        p5_count -= reduce_p5
                        difference -= reduce_p5
                        print(f"    Reduced P5 by {reduce_p5}, remaining difference: {difference}")
                    
                    if difference > 0 and p4_count > 0:
                        reduce_p4 = min(difference, p4_count)
                        p4_count -= reduce_p4
                        difference -= reduce_p4
                        print(f"    Reduced P4 by {reduce_p4}, remaining difference: {difference}")
                
                # Verify final count
                final_total = p1_count + p2_count + p3_count + p4_count + p5_count
                print(f"  Final count verification: {final_total} (should match {total_records})")
            
            print(f"  {dominant_topic}: {total_records} records → P1:{p1_count}, P2:{p2_count}, P3:{p3_count}, P4:{p4_count}, P5:{p5_count}")
            
            # Process records for this topic
            for i, record in enumerate(records):
                # Determine priority value based on position in the list
                # For non-urgent no-action emails, only P4 and P5 can be assigned
                if i < p1_count:
                    priority_value = "P1-Critical"
                elif i < p1_count + p2_count:
                    priority_value = "P2-High"
                elif i < p1_count + p2_count + p3_count:
                    priority_value = "P3-Medium"
                elif i < p1_count + p2_count + p3_count + p4_count:
                    priority_value = "P4-Low"
                else:
                    priority_value = "P5-Very Low"
                
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
            # Topic not in mapping - set default value to P5-Very Low
            print(f"  {dominant_topic}: {total_records} records → P1:0, P2:0, P3:0, P4:0, P5:{total_records} (unknown topic)")
            
            for record in records:
                update_result = email_collection.update_one(
                    {"_id": record["_id"]},
                    {"$set": {"priority": "P5-Very Low"}}
                )
                
                if update_result.modified_count > 0:
                    updated_count += 1
                    print(f"⚠️  Unknown topic '{dominant_topic}' - set to 'P5-Very Low'")
    
    # Display results
    print(f"\n" + "=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    
    print(f"Total non-urgent no-action-required records processed: {len(non_urgent_no_action_records)}")
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
    print("PRIORITY MAPPING FOR NON-URGENT + NO ACTION REQUIRED EMAILS")
    print("=" * 80)
    
    print("Internal Topics with Priority Distribution:")
    print("-" * 70)
    for i, (topic, config) in enumerate(sorted(INTERNAL_TOPICS.items()), 1):
        total = sum(config.values())
        print(f"{i:2d}. {topic}: P1:{config['p1']}, P2:{config['p2']}, P3:{config['p3']}, P4:{config['p4']}, P5:{config['p5']} (Total: {total})")
    
    print(f"\nExternal Topics with Priority Distribution:")
    print("-" * 70)
    for i, (topic, config) in enumerate(sorted(EXTERNAL_TOPICS.items()), 1):
        total = sum(config.values())
        print(f"{i:2d}. {topic}: P1:{config['p1']}, P2:{config['p2']}, P3:{config['p3']}, P4:{config['p4']}, P5:{config['p5']} (Total: {total})")
    
    print(f"\nTotal mapped topics: {len(ALL_TOPICS)}")
    print("Priority Levels for NON-URGENT NO-ACTION emails:")
    print("  - P1-Critical: NOT USED for no-action emails")
    print("  - P2-High: NOT USED for no-action emails")
    print("  - P3-Medium: NOT USED for no-action emails")
    print("  - P4-Low: Low priority for administrative closure (used for some topics)")
    print("  - P5-Very Low: Very low priority for records only (used for most topics)")
    print("Unknown topics will get 'P5-Very Low' priority")

if __name__ == "__main__":
    try:
        # Show the mapping first
        show_priority_mapping()
        
        # Confirm before proceeding
        print(f"\nProceed with adding priority field to non-urgent no-action-required records? (y/n): ", end="")
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