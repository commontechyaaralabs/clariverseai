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

# Priority mapping with exact distribution for NON-URGENT + ACTION REQUIRED emails
# Internal Topics with specific priority counts
INTERNAL_TOPICS = {
    "User Access Approval": {"p1": 0, "p2": 3, "p3": 12, "p4": 6, "p5": 1},  # Total 22
    "IT Marketing Unresolved": {"p1": 0, "p2": 2, "p3": 6, "p4": 8, "p5": 2},  # Total 18
    "IT Access Problem": {"p1": 0, "p2": 4, "p3": 11, "p4": 5, "p5": 0},  # Total 20
    "Salary Leave Issues": {"p1": 0, "p2": 2, "p3": 8, "p4": 5, "p5": 1},  # Total 16
    "Vendor Management": {"p1": 0, "p2": 3, "p3": 8, "p4": 4, "p5": 0},  # Total 15
    "Process Exception": {"p1": 0, "p2": 4, "p3": 10, "p4": 3, "p5": 0},  # Total 17
    "Performance Management": {"p1": 0, "p2": 2, "p3": 6, "p4": 5, "p5": 1},  # Total 14
    "Executive Decision Required": {"p1": 1, "p2": 6, "p3": 6, "p4": 1, "p5": 0},  # Total 14
    "Risk Management Alert": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "Quality Assurance Breach": {"p1": 0, "p2": 2, "p3": 6, "p4": 2, "p5": 0},  # Total 10
    "Training Need Alert": {"p1": 0, "p2": 1, "p3": 3, "p4": 3, "p5": 1},  # Total 8
    "Approval Required": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "Internal Audit": {"p1": 0, "p2": 3, "p3": 4, "p4": 1, "p5": 0},  # Total 8
    "Compliance Issues": {"p1": 0, "p2": 3, "p3": 4, "p4": 1, "p5": 0},  # Total 8
    "Staff Performance Issue": {"p1": 0, "p2": 1, "p3": 4, "p4": 2, "p5": 0},  # Total 7
    "Technology Emergency": {"p1": 0, "p2": 2, "p3": 3, "p4": 1, "p5": 0}  # Total 6
}

# External Topics with specific priority counts (subset of most common topics)
EXTERNAL_TOPICS = {
    "Covenant Compliance Question": {"p1": 0, "p2": 4, "p3": 7, "p4": 2, "p5": 0},  # Total 13
    "Consent Withdrawal Notification": {"p1": 2, "p2": 10, "p3": 5, "p4": 1, "p5": 0},  # Total 18
    "ECB Policy Update": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "GDPR Access Request": {"p1": 1, "p2": 11, "p3": 5, "p4": 1, "p5": 0},  # Total 18
    "MiFID Investment Inquiry": {"p1": 0, "p2": 3, "p3": 7, "p4": 2, "p5": 0},  # Total 12
    "CRS Reporting Question": {"p1": 0, "p2": 4, "p3": 7, "p4": 2, "p5": 0},  # Total 13
    "Account Access Error": {"p1": 0, "p2": 5, "p3": 8, "p4": 2, "p5": 0},  # Total 15
    "SEPA Payment Failure": {"p1": 0, "p2": 6, "p3": 8, "p4": 2, "p5": 0},  # Total 16
    "Credit Line Utilization": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "Letter Credit Amendment": {"p1": 0, "p2": 5, "p3": 7, "p4": 2, "p5": 0},  # Total 14
    "KYC Documentation Problem": {"p1": 0, "p2": 7, "p3": 6, "p4": 2, "p5": 0},  # Total 15
    "Account Opening Issue": {"p1": 0, "p2": 5, "p3": 7, "p4": 2, "p5": 0},  # Total 14
    "Relationship Manager Request": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "Cross-Border Transfer Problem": {"p1": 0, "p2": 5, "p3": 7, "p4": 2, "p5": 0},  # Total 14
    "API Connectivity Issue": {"p1": 0, "p2": 4, "p3": 7, "p4": 2, "p5": 0},  # Total 13
    "Mobile Authentication Error": {"p1": 0, "p2": 4, "p3": 7, "p4": 2, "p5": 0},  # Total 13
    "Digital Platform Malfunction": {"p1": 0, "p2": 4, "p3": 7, "p4": 2, "p5": 0},  # Total 13
    "Transaction Processing Error": {"p1": 0, "p2": 5, "p3": 6, "p4": 2, "p5": 0},  # Total 13
    "Fintech Integration Error": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "Regulatory Documentation Need": {"p1": 0, "p2": 5, "p3": 5, "p4": 2, "p5": 0},  # Total 12
    "Strong Customer Authentication": {"p1": 0, "p2": 5, "p3": 5, "p4": 2, "p5": 0},  # Total 12
    "Foreign Exchange Problem": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "Fund Transfer Problem": {"p1": 0, "p2": 4, "p3": 6, "p4": 2, "p5": 0},  # Total 12
    "International Transfer Question": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "Reporting Platform Issue": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "Multi-Currency Statement Need": {"p1": 0, "p2": 2, "p3": 6, "p4": 2, "p5": 0},  # Total 10
    "Online Banking Problem": {"p1": 0, "p2": 4, "p3": 6, "p4": 2, "p5": 0},  # Total 12
    "Clearing House Problem": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "Export Documentation Error": {"p1": 0, "p2": 4, "p3": 5, "p4": 2, "p5": 0},  # Total 11
    "Derivatives Question Alert": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "RegTech Alert": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "Client Categorization Update": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "SEPA Payment Status": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "Third Party Provider": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "Export Credit Issue": {"p1": 0, "p2": 4, "p3": 5, "p4": 2, "p5": 0},  # Total 11
    "Card Payment Dispute": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "FATCA Compliance Issue": {"p1": 0, "p2": 6, "p3": 4, "p4": 2, "p5": 0},  # Total 12
    "Custody Service Problem": {"p1": 0, "p2": 3, "p3": 6, "p4": 2, "p5": 0},  # Total 11
    "Fee Clarification Need": {"p1": 0, "p2": 1, "p3": 4, "p4": 3, "p5": 0},  # Total 8
    "Process Improvement Request": {"p1": 0, "p2": 1, "p3": 3, "p4": 2, "p5": 1},  # Total 7
    "ACH Processing Error": {"p1": 0, "p2": 4, "p3": 5, "p4": 2, "p5": 0},  # Total 11
    "Merchant Service Problem": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "Data Retention Question": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "Import Finance Question": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "Card Network Issue": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "Monitoring System Alert": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "Check Clearing Issue": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "Legal Notice Response": {"p1": 0, "p2": 5, "p3": 4, "p4": 2, "p5": 0},  # Total 11
    "AML Documentation Request": {"p1": 0, "p2": 5, "p3": 4, "p4": 2, "p5": 0},  # Total 11
    "Fee Dispute Email": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "Best Execution Report": {"p1": 0, "p2": 1, "p3": 4, "p4": 2, "p5": 0},  # Total 7
    "Digital Banking Error": {"p1": 0, "p2": 3, "p3": 5, "p4": 2, "p5": 0},  # Total 10
    "SEPA Instant Failure": {"p1": 0, "p2": 4, "p3": 4, "p4": 2, "p5": 0},  # Total 10
    "Consent Management Dispute": {"p1": 0, "p2": 5, "p3": 4, "p4": 1, "p5": 0},  # Total 10
    "Instant Payment Rejected": {"p1": 0, "p2": 4, "p3": 4, "p4": 2, "p5": 0},  # Total 10
    "Private Banking Service": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "Open Banking API": {"p1": 0, "p2": 3, "p3": 4, "p4": 2, "p5": 0},  # Total 9
    "Phone Banking Issue": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "Integration Problem Report": {"p1": 0, "p2": 3, "p3": 4, "p4": 2, "p5": 0},  # Total 9
    "Personal Data Inquiry": {"p1": 0, "p2": 5, "p3": 4, "p4": 1, "p5": 0},  # Total 10
    "Commercial Paper Issue": {"p1": 0, "p2": 3, "p3": 4, "p4": 2, "p5": 0},  # Total 9
    "Direct Debit Returned": {"p1": 0, "p2": 4, "p3": 4, "p4": 2, "p5": 0},  # Total 10
    "Mobile App Issue": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "Quality Assurance Issue": {"p1": 0, "p2": 2, "p3": 5, "p4": 2, "p5": 0},  # Total 9
    "POS Terminal Issue": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "TPP Access Problem": {"p1": 0, "p2": 4, "p3": 4, "p4": 1, "p5": 0},  # Total 9
    "Corporate Account Issue": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "Complaint Resolution Request": {"p1": 0, "p2": 3, "p3": 4, "p4": 2, "p5": 0},  # Total 9
    "Facility Renewal Inquiry": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "Corporate Loan Inquiry": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "Audit Trail Need": {"p1": 0, "p2": 4, "p3": 3, "p4": 2, "p5": 0},  # Total 9
    "Service Quality Complaint": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "EU Taxonomy Reporting": {"p1": 0, "p2": 4, "p3": 3, "p4": 2, "p5": 0},  # Total 9
    "Liquidity Facility Problem": {"p1": 0, "p2": 4, "p3": 3, "p4": 2, "p5": 0},  # Total 9
    "Processing Information Need": {"p1": 0, "p2": 1, "p3": 4, "p4": 2, "p5": 0},  # Total 7
    "Authentication Problem Report": {"p1": 0, "p2": 3, "p3": 4, "p4": 2, "p5": 0},  # Total 9
    "Basel III Requirement": {"p1": 0, "p2": 4, "p3": 3, "p4": 2, "p5": 0},  # Total 9
    "TARGET2 Settlement Issue": {"p1": 0, "p2": 4, "p3": 3, "p4": 2, "p5": 0},  # Total 9
    "Deposit Service Issue": {"p1": 0, "p2": 2, "p3": 4, "p4": 2, "p5": 0},  # Total 8
    "Wealth Management Issue": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "Risk Assessment Update": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "Investment Advisory Question": {"p1": 0, "p2": 1, "p3": 3, "p4": 2, "p5": 0},  # Total 6
    "Tax Withholding Problem": {"p1": 0, "p2": 3, "p3": 3, "p4": 2, "p5": 0},  # Total 8
    "Cash Management Issue": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "Core Banking System": {"p1": 0, "p2": 3, "p3": 3, "p4": 2, "p5": 0},  # Total 8
    "Wire Transfer Delay": {"p1": 0, "p2": 3, "p3": 3, "p4": 2, "p5": 0},  # Total 8
    "Compliance Certificate Request": {"p1": 0, "p2": 3, "p3": 3, "p4": 2, "p5": 0},  # Total 8
    "SEPA Processing Error": {"p1": 0, "p2": 3, "p3": 3, "p4": 2, "p5": 0},  # Total 8
    "Data Sharing Problem": {"p1": 0, "p2": 4, "p3": 3, "p4": 1, "p5": 0},  # Total 8
    "Right Erasure Demand": {"p1": 1, "p2": 5, "p3": 2, "p4": 1, "p5": 0},  # Total 9
    "Service Level Breach": {"p1": 0, "p2": 3, "p3": 3, "p4": 2, "p5": 0},  # Total 8
    "Trade Finance Delay": {"p1": 0, "p2": 3, "p3": 3, "p4": 2, "p5": 0},  # Total 8
    "EU Passporting Service": {"p1": 0, "p2": 2, "p3": 3, "p4": 1, "p5": 0},  # Total 6
    "Overdraft Facility Request": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "Reporting Discrepancy Alert": {"p1": 0, "p2": 3, "p3": 3, "p4": 1, "p5": 0},  # Total 7
    "Documentary Collection Issue": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "Audit Firm Communication": {"p1": 0, "p2": 2, "p3": 3, "p4": 1, "p5": 0},  # Total 6
    "Digital Wallet Issue": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "ATM Network Problem": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "Processing Delay Complaint": {"p1": 0, "p2": 2, "p3": 3, "p4": 2, "p5": 0},  # Total 7
    "Legal Advisor Update": {"p1": 0, "p2": 1, "p3": 3, "p4": 1, "p5": 0},  # Total 5
    "Credit Facility Question": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "Enhanced Due Diligence": {"p1": 0, "p2": 3, "p3": 3, "p4": 1, "p5": 0},  # Total 7
    "Cash Flow Issue": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "Investment Service Question": {"p1": 0, "p2": 1, "p3": 2, "p4": 2, "p5": 0},  # Total 5
    "Balance Inquiry Problem": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "Treasury Service Problem": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "Treasury System Error": {"p1": 0, "p2": 3, "p3": 3, "p4": 1, "p5": 0},  # Total 7
    "Beneficial Ownership Declaration": {"p1": 0, "p2": 3, "p3": 3, "p4": 1, "p5": 0},  # Total 7
    "Transaction Dispute Report": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "FX Execution Problem": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "Euro Clearing Problem": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "PSD2 Compliance Question": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "Consulting Project Report": {"p1": 0, "p2": 1, "p3": 2, "p4": 1, "p5": 0},  # Total 4
    "Bond Trading Problem": {"p1": 0, "p2": 2, "p3": 2, "p4": 2, "p5": 0},  # Total 6
    "Certificate Deposit Question": {"p1": 0, "p2": 1, "p3": 2, "p4": 2, "p5": 0},  # Total 5
    "Asset Management Issue": {"p1": 0, "p2": 2, "p3": 2, "p4": 1, "p5": 0},  # Total 5
    "Covenant Breach Alert": {"p1": 1, "p2": 3, "p3": 2, "p4": 0, "p5": 0},  # Total 6
    "Interest Rate Query": {"p1": 0, "p2": 1, "p3": 1, "p4": 2, "p5": 0},  # Total 4
    "Cross-Border Regulatory Question": {"p1": 0, "p2": 2, "p3": 2, "p4": 1, "p5": 0},  # Total 5
    "Letter Credit Processing": {"p1": 0, "p2": 2, "p3": 2, "p4": 1, "p5": 0},  # Total 5
    "Media Inquiry Email": {"p1": 0, "p2": 3, "p3": 2, "p4": 1, "p5": 0},  # Total 6
    "Payment Service Problem": {"p1": 0, "p2": 2, "p3": 2, "p4": 1, "p5": 0},  # Total 5
    "Trade Documentation Error": {"p1": 0, "p2": 2, "p3": 2, "p4": 1, "p5": 0},  # Total 5
    "Guarantee Execution Problem": {"p1": 0, "p2": 2, "p3": 2, "p4": 1, "p5": 0},  # Total 5
    "Branch Service Complaint": {"p1": 0, "p2": 1, "p3": 3, "p4": 1, "p5": 0},  # Total 5
    "Compliance Monitoring Alert": {"p1": 0, "p2": 2, "p3": 1, "p4": 1, "p5": 0},  # Total 4
    "Satisfaction Survey Result": {"p1": 0, "p2": 0, "p3": 1, "p4": 1, "p5": 0},  # Total 2
    "Two-Factor Authentication Error": {"p1": 0, "p2": 1, "p3": 2, "p4": 1, "p5": 0},  # Total 4
    "Beneficiary Bank Unavailable": {"p1": 0, "p2": 1, "p3": 2, "p4": 1, "p5": 0},  # Total 4
    "Customer Feedback Report": {"p1": 0, "p2": 0, "p3": 1, "p4": 1, "p5": 0},  # Total 2
    "System Outage Notification": {"p1": 0, "p2": 1, "p3": 1, "p4": 0, "p5": 0},  # Total 2
    "Data Breach Warning": {"p1": 3, "p2": 0, "p3": 0, "p4": 0, "p5": 0},  # Total 3
    "Sanctions Screening Alert": {"p1": 2, "p2": 0, "p3": 0, "p4": 0, "p5": 0},  # Total 2
    "Security Incident Alert": {"p1": 2, "p2": 0, "p3": 0, "p4": 0, "p5": 0}  # Total 2
}

# Combine all topics
ALL_TOPICS = {**INTERNAL_TOPICS, **EXTERNAL_TOPICS}

def add_priority_field():
    """Add priority field based on dominant_topic for non-urgent emails with action required"""
    
    print("=" * 80)
    print("ADDING PRIORITY FIELD FOR NON-URGENT + ACTION REQUIRED EMAILS")
    print("=" * 80)
    
    # Filter records where urgency=false, follow_up_required="yes", action_pending_status="yes"
    print("Step 1: Filtering non-urgent emails with action required...")
    
    non_urgent_action_query = {
        "urgency": False,
        "follow_up_required": "yes",
        "action_pending_status": "yes"
    }
    
    non_urgent_action_count = email_collection.count_documents(non_urgent_action_query)
    print(f"Found {non_urgent_action_count} records with urgency=false, follow_up_required='yes', action_pending_status='yes'")
    
    if non_urgent_action_count == 0:
        print("No non-urgent action-required records found. Exiting...")
        return
    
    # Show current field status
    print(f"\nStep 2: Checking current priority field status...")
    priority_exists = email_collection.count_documents({"priority": {"$exists": True}})
    print(f"Records with priority field: {priority_exists}")
    
    # Get non-urgent action-required records with dominant_topic
    print(f"\nStep 3: Processing non-urgent action-required records...")
    
    non_urgent_action_records = list(email_collection.find(
        {
            "urgency": False,
            "follow_up_required": "yes",
            "action_pending_status": "yes",
            "dominant_topic": {"$exists": True}
        },
        {"_id": 1, "dominant_topic": 1, "category": 1, "priority": 1}
    ))
    
    print(f"Found {len(non_urgent_action_records)} non-urgent action-required records with dominant_topic")
    
    # Process each record with proper distribution
    updated_count = 0
    skipped_count = 0
    topic_stats = {}
    
    # Group records by topic for proper distribution
    topic_groups = {}
    for record in non_urgent_action_records:
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
                        
                else:  # Fewer records than expected - reduce from P5 first, then P2, then P3
                    difference = abs(difference)
                    print(f"  Reducing {difference} records from P5, P2, P3 in that order")
                    
                    if difference > 0 and p5_count > 0:
                        reduce_p5 = min(difference, p5_count)
                        p5_count -= reduce_p5
                        difference -= reduce_p5
                        print(f"    Reduced P5 by {reduce_p5}, remaining difference: {difference}")
                    
                    if difference > 0 and p2_count > 0:
                        reduce_p2 = min(difference, p2_count)
                        p2_count -= reduce_p2
                        difference -= reduce_p2
                        print(f"    Reduced P2 by {reduce_p2}, remaining difference: {difference}")
                    
                    if difference > 0 and p3_count > 0:
                        reduce_p3 = min(difference, p3_count)
                        p3_count -= reduce_p3
                        difference -= reduce_p3
                        print(f"    Reduced P3 by {reduce_p3}, remaining difference: {difference}")
                
                # Verify final count
                final_total = p1_count + p2_count + p3_count + p4_count + p5_count
                print(f"  Final count verification: {final_total} (should match {total_records})")
            
            print(f"  {dominant_topic}: {total_records} records → P1:{p1_count}, P2:{p2_count}, P3:{p3_count}, P4:{p4_count}, P5:{p5_count}")
            
            # Process records for this topic
            for i, record in enumerate(records):
                # Determine priority value based on position in the list
                # For non-urgent emails, P1, P2, P3, P4, P5 can all be assigned
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
    
    print(f"Total non-urgent action-required records processed: {len(non_urgent_action_records)}")
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
    print("PRIORITY MAPPING FOR NON-URGENT + ACTION REQUIRED EMAILS")
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
    print("Priority Levels for NON-URGENT emails:")
    print("  - P1-Critical: Strategic/regulatory matters requiring immediate attention")
    print("  - P2-High: Business-critical issues needing prompt resolution")
    print("  - P3-Medium: Standard operational matters with normal SLAs")
    print("  - P4-Low: Administrative tasks that can be deferred")
    print("  - P5-Very Low: Non-critical items with flexible timelines")
    print("Unknown topics will get 'P3-Medium' priority")

if __name__ == "__main__":
    try:
        # Show the mapping first
        show_priority_mapping()
        
        # Confirm before proceeding
        print(f"\nProceed with adding priority field to non-urgent action-required records? (y/n): ", end="")
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
