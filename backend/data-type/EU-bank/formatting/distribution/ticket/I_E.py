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

# Get collection
tickets_collection = db['tickets_new']

# Define INTERNAL ISSUES topics (Backend/Infrastructure - ~17 topics, 10%)
INTERNAL_TOPICS = [
    "Core System Error", "Network Connection Lost", "Database Corruption Detected", "Interface Integration Failed", 
    "Server Hardware Failed", "Application Server Down", "Bandwidth Issue Reported", "Service Interface Down", 
    "Equipment Failure Reported", "Storage Capacity Issue", "API Connection Failed", "Biometric Setup Failed", 
    "AML Alert Triggered", "Regulatory Report Delay", "Alert System Failed", "Process Quality Issue", 
    "Payment Processor Error", "Vendor License Issue", "Fraud Detection Alert", "Data Recovery Required", 
    "Backup System Failed", "Protocol Communication Error", "Remote Access Problem", "Vulnerability Assessment Required", 
    "Batch Processing Failed", "Due Diligence Required", "Credit Covenant Breach", "Transaction Reporting Error",
    "Error Detection Problem", "Software Update Failed", "Sanctions Screening Alert"
]

# Define EXTERNAL ISSUES topics (Customer-facing problems - ~150 topics, 90%)
EXTERNAL_TOPICS = [
    "Password Reset Failed", "Account Lockout Issue", "Mobile Login Error", "Web Portal Timeout", 
    "Password Expiry Alert", "Multi Factor Failed", "Login Session Expired", "Authentication Error Reported", 
    "Strong Authentication Error", "Session Management Error", "Access Token Expired", "Biometric Recognition Failed", 
    "Profile Modification Request", "Contact Details Update", "Account Closure Request", "Signatory Update Required", 
    "Account Mandate Change", "Profile Data Incorrect", "Account Termination Request", "Corporate Setup Delayed", 
    "User Permission Error", "Authorization Matrix Error", "Account Synchronization Failed", "Closure Documentation Missing", 
    "Balance Update Error", "Balance Reconciliation Error", "Multi Currency Issue", "Direct Debit Rejection", 
    "Wire Transfer Delay", "Card Authorization Failed", "Standing Order Error", "Bulk Payment Problem", 
    "Transaction Limit Exceeded", "Fee Calculation Error", "International Transfer Failed", "SWIFT Message Error", 
    "Card PIN Reset", "Payment File Rejected", "Transaction Dispute Raised", "Payment Queue Error", 
    "Service Fee Question", "Cross Border Failed", "Correspondent Bank Delay", "Payment Instruction Error", 
    "Currency Conversion Problem", "Exchange Rate Error", "FX Rate Dispute", "Transaction History Missing", 
    "Pricing Dispute Raised", "Dual Control Failed", "Settlement Issue Reported", "Statement Generation Failed", 
    "Statement Download Error", "Notification Setup Error", "Email Notification Failed", "Push Notification Failed", 
    "Custom Report Error", "Dashboard Access Problem", "Analytics Query Failed", "Real Time Data", 
    "Business Intelligence Error", "Report Formatting Error", "Liquidity Report Error", "Platform Response Slow", 
    "Mobile Feature Broken", "Browser Compatibility Issue", "Open Banking Error", "Consent Management Failed", 
    "Token Generation Error", "System Performance Issue", "Connection Timeout Error", "App Update Problem", 
    "Data Sharing Problem", "Third Party Access", "Provider Authentication Failed", "Letter Credit Issue", 
    "Document Discrepancy Found", "Bank Guarantee Delay", "Export Finance Problem", "Import Payment Delay", 
    "Trade Document Error", "Electronic Document Error", "Credit Amendment Request", "Collection Processing Delay", 
    "Guarantee Claim Raised", "Finance Application Delayed", "Customs Clearance Issue", "Document Amendment Required", 
    "Claim Settlement Delayed", "Application Processing Slow", "Processing Status Delayed", "Investment Query Raised", 
    "Credit Facility Issue", "Hedging Problem Reported", "Cash Flow Problem", "Deal Confirmation Delayed", 
    "Portfolio Management Issue", "Loan Interest Error", "Quote Request Delayed", "Investment Maturity Issue", 
    "Derivative Confirmation Delayed", "Balance Concentration Failed", "KYC Update Required", "Data Access Request", 
    "Personal Data Deletion", "Document Verification Failed", "Suitability Assessment Issue", "Client Categorization Issue", 
    "Risk Rating Update", "Product Governance Issue", "Data Portability Request", "Transaction Fraud Detected", 
    "Security Breach Reported", "Card Skimming Incident", "Identity Theft Alert", "Access Control Breach", 
    "Online Fraud Reported", "Service Response Complaint", "Executive Complaint Raised", "Account Manager Issue", 
    "SLA Breach Detected", "Response Time Complaint", "Board Member Concern", "Relationship Coverage Gap", 
    "Performance Metric Breach", "Service Provider Complaint", "Customer Communication Failed", "Document Authentication Failed", 
    "Workflow Approval Stuck", "Form Processing Delay", "Status Tracking Error", "Signature Verification Failed", 
    "Courier Service Delayed", "Submission Deadline Missed", "Contract Compliance Issue"
]

def categorize_dominant_topic(dominant_topic):
    """
    Categorize the dominant_topic as Internal or External
    """
    if dominant_topic in INTERNAL_TOPICS:
        return "Internal"
    elif dominant_topic in EXTERNAL_TOPICS:
        return "External"
    else:
        return "Unknown"  # For topics not in either list

def update_tickets_with_category():
    """
    Update all documents in 'tickets_new' collection to add category field
    based on dominant_topic
    """
    
    # Get all documents from tickets_new collection
    ticket_documents = list(tickets_collection.find({}))
    
    print(f"Found {len(ticket_documents)} documents in tickets_new collection")
    
    updated_count = 0
    internal_count = 0
    external_count = 0
    unknown_count = 0
    unknown_topics_set = set()
    
    for doc in ticket_documents:
        doc_id = doc['_id']
        dominant_topic = doc.get('dominant_topic', '')
        
        # Categorize the dominant topic
        category = categorize_dominant_topic(dominant_topic)
        
        # Count categories for statistics
        if category == "Internal":
            internal_count += 1
        elif category == "External":
            external_count += 1
        else:
            unknown_count += 1
            if dominant_topic:
                unknown_topics_set.add(dominant_topic)
        
        # Update the document with category field
        try:
            result = tickets_collection.update_one(
                {'_id': doc_id},
                {'$set': {'category': category}}
            )
            if result.modified_count > 0:
                updated_count += 1
        except Exception as e:
            print(f"Error updating document {doc_id}: {str(e)}")
    
    print(f"\nSuccessfully updated {updated_count} documents in tickets_new collection")
    print(f"\nCategory distribution:")
    print(f"  - Internal: {internal_count} ({(internal_count/len(ticket_documents)*100):.2f}%)")
    print(f"  - External: {external_count} ({(external_count/len(ticket_documents)*100):.2f}%)")
    print(f"  - Unknown: {unknown_count} ({(unknown_count/len(ticket_documents)*100):.2f}%)")
    
    # Verify the changes by checking sample documents
    print("\n=== Sample documents after update ===")
    
    # Check one Internal category
    internal_sample = tickets_collection.find_one({'category': 'Internal'})
    if internal_sample:
        print(f"Internal sample - dominant_topic: '{internal_sample.get('dominant_topic')}', category: '{internal_sample.get('category')}'")
    
    # Check one External category
    external_sample = tickets_collection.find_one({'category': 'External'})
    if external_sample:
        print(f"External sample - dominant_topic: '{external_sample.get('dominant_topic')}', category: '{external_sample.get('category')}'")
    
    # Check one Unknown category (if any)
    unknown_sample = tickets_collection.find_one({'category': 'Unknown'})
    if unknown_sample:
        print(f"Unknown sample - dominant_topic: '{unknown_sample.get('dominant_topic')}', category: '{unknown_sample.get('category')}'")
    
    # Show unique dominant topics that are unknown
    if unknown_count > 0:
        print(f"\n=== Unknown dominant topics found ({len(unknown_topics_set)} unique) ===")
        for topic in sorted(unknown_topics_set):
            print(f"  - '{topic}'")

if __name__ == "__main__":
    try:
        print("=" * 60)
        print("TICKET CATEGORY CLASSIFICATION SCRIPT")
        print("=" * 60)
        update_tickets_with_category()
        print("\n" + "=" * 60)
        print("Operation completed successfully!")
        print("=" * 60)
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
    finally:
        # Close the connection
        client.close()