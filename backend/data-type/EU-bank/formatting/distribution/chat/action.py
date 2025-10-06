# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collections
chat_new_collection = db['chat_new']

def update_action_pending_from():
    """
    Filter records with action_pending_status: "yes" and update action_pending_from field
    based on dominant_topic classification
    """
    
    print("="*80)
    print("ACTION PENDING FROM UPDATE")
    print("="*80)
    
    # Define dominant topics for customer and company
    customer_topics = [
        "GDPR Access Request",
        "Documentation Request Alert",
        "Transaction Status Inquiry",
        "Account Access Problem",
        "User Access Problem",
        "Policy Clarification Request",
        "Account Management Strategy"
    ]
    
    company_topics = [
        "Financial Reporting Procedure",
        "Software Issue Resolution",
        "Risk Assessment Update",
        "Attendance Management Alert",
        "Team Meeting Reminder",
        "Employee Relations Coordination",
        "Monthly Compliance Audit",
        "Pipeline Management Discussion",
        "Performance Metric Tracking",
        "Database Access Problem",
        "Compliance Monitoring Update",
        "Network Performance Issue",
        "Break Schedule Coordination",
        "Customer Insight Report",
        "Customer Research Finding",
        "Audit Feedback Review",
        "Security Protocol Change",
        "Product Update Communication",
        "Audit Finding Resolution",
        "Knowledge Management Update",
        "Cybersecurity Alert Warning",
        "Regulatory Examination Preparation",
        "Compliance Gap Remediation",
        "Audit Coordination Schedule",
        "Risk Assessment Documentation",
        "Case Management Discussion",
        "Campaign Coordination Discussion",
        "Technology Provider Communication",
        "Compliance Report Generation",
        "Skill Development Update",
        "Audit Planning Coordination",
        "Regulatory Response Management",
        "Daily Operational Procedure",
        "Shift Coverage Request",
        "Hardware Troubleshooting Help",
        "Budget Management Discussion",
        "Workflow Optimization Alert",
        "Control Testing Validation",
        "Quality Standard Reminder",
        "System Status Alert",
        "Regulatory Update Alert",
        "Vendor Management Discussion",
        "Workforce Planning Update",
        "Team Coordination Update",
        "Technology Rollout Update",
        "Cross-Department Project Update",
        "Leave Request Approval",
        "Business Continuity Activation",
        "Performance Review Planning",
        "Content Management Update",
        "Authentication Service Down",
        "Compliance Training Reminder",
        "Overtime Approval Need",
        "Staff Management Discussion",
        "Process Coordination Discussion",
        "Application Server Warning",
        "System Integration Progress",
        "Training Schedule Update"
    ]
    
    print(f"Customer topics: {len(customer_topics)}")
    print(f"Company topics: {len(company_topics)}")
    print()
    
    # Filter records with action_pending_status: "yes"
    query = {"action_pending_status": "yes"}
    action_pending_docs = list(chat_new_collection.find(query))
    total_action_pending = len(action_pending_docs)
    
    print(f"Total records with action_pending_status: 'yes': {total_action_pending}")
    print()
    
    if total_action_pending == 0:
        print("No records found with action_pending_status: 'yes'")
        return
    
    # Process each document
    customer_count = 0
    company_count = 0
    unknown_count = 0
    
    for doc in action_pending_docs:
        doc_id = doc['_id']
        dominant_topic = doc.get('dominant_topic', '')
        
        # Determine action_pending_from based on dominant_topic
        if dominant_topic in customer_topics:
            action_pending_from = "customer"
            customer_count += 1
        elif dominant_topic in company_topics:
            action_pending_from = "company"
            company_count += 1
        else:
            # For topics not in either list, set to "unknown"
            action_pending_from = "unknown"
            unknown_count += 1
        
        # Update the document
        chat_new_collection.update_one(
            {'_id': doc_id},
            {'$set': {'action_pending_from': action_pending_from}}
        )
    
    print("Update completed!")
    print(f"Records updated with action_pending_from: 'customer': {customer_count}")
    print(f"Records updated with action_pending_from: 'company': {company_count}")
    print(f"Records updated with action_pending_from: 'unknown': {unknown_count}")
    print(f"Total records processed: {customer_count + company_count + unknown_count}")

def verify_action_pending_from():
    """
    Verify the action_pending_from field updates
    """
    print("\n" + "="*80)
    print("VERIFICATION - ACTION PENDING FROM RESULTS")
    print("="*80)
    
    # Get all records with action_pending_status: "yes"
    query = {"action_pending_status": "yes"}
    action_pending_docs = list(chat_new_collection.find(query))
    
    if not action_pending_docs:
        print("No records found with action_pending_status: 'yes'")
        return
    
    # Count by action_pending_from
    customer_docs = [doc for doc in action_pending_docs if doc.get('action_pending_from') == 'customer']
    company_docs = [doc for doc in action_pending_docs if doc.get('action_pending_from') == 'company']
    unknown_docs = [doc for doc in action_pending_docs if doc.get('action_pending_from') == 'unknown']
    
    print(f"Total records with action_pending_status: 'yes': {len(action_pending_docs)}")
    print(f"Records with action_pending_from: 'customer': {len(customer_docs)}")
    print(f"Records with action_pending_from: 'company': {len(company_docs)}")
    print(f"Records with action_pending_from: 'unknown': {len(unknown_docs)}")
    print()
    
    # Show sample records for each category
    if customer_docs:
        print("SAMPLE CUSTOMER RECORDS:")
        print("-" * 40)
        for i, doc in enumerate(customer_docs[:5], 1):
            dominant_topic = doc.get('dominant_topic', 'N/A')
            print(f"{i}. {dominant_topic}")
        if len(customer_docs) > 5:
            print(f"... and {len(customer_docs) - 5} more")
        print()
    
    if company_docs:
        print("SAMPLE COMPANY RECORDS:")
        print("-" * 40)
        for i, doc in enumerate(company_docs[:5], 1):
            dominant_topic = doc.get('dominant_topic', 'N/A')
            print(f"{i}. {dominant_topic}")
        if len(company_docs) > 5:
            print(f"... and {len(company_docs) - 5} more")
        print()
    
    if unknown_docs:
        print("UNKNOWN RECORDS (topics not in predefined lists):")
        print("-" * 40)
        unknown_topics = [doc.get('dominant_topic', 'N/A') for doc in unknown_docs]
        unique_unknown_topics = list(set(unknown_topics))
        for i, topic in enumerate(unique_unknown_topics, 1):
            print(f"{i}. {topic}")
        print()

def analyze_dominant_topics_by_action_pending_from():
    """
    Analyze dominant_topics by action_pending_from category
    """
    print("\n" + "="*80)
    print("DOMINANT TOPIC BY ACTION PENDING FROM")
    print("="*80)
    
    # Get all records with action_pending_status: "yes"
    query = {"action_pending_status": "yes"}
    action_pending_docs = list(chat_new_collection.find(query))
    
    if not action_pending_docs:
        print("No records found with action_pending_status: 'yes'")
        return
    
    # Separate by action_pending_from
    customer_docs = [doc for doc in action_pending_docs if doc.get('action_pending_from') == 'customer']
    company_docs = [doc for doc in action_pending_docs if doc.get('action_pending_from') == 'company']
    
    print(f"Customer records: {len(customer_docs)}")
    print(f"Company records: {len(company_docs)}")
    print()
    
    # Analyze Customer category
    if customer_docs:
        print("CUSTOMER CATEGORY:")
        print("-" * 40)
        customer_topics = [doc.get('dominant_topic') for doc in customer_docs if doc.get('dominant_topic')]
        from collections import Counter
        customer_counts = Counter(customer_topics)
        sorted_customer = sorted(customer_counts.items(), key=lambda x: x[1], reverse=True)
        
        for i, (topic, count) in enumerate(sorted_customer, 1):
            percentage = (count / len(customer_topics)) * 100
            print(f"{i:2d}. {topic}: {count} ({percentage:.1f}%)")
        print()
    
    # Analyze Company category
    if company_docs:
        print("COMPANY CATEGORY:")
        print("-" * 40)
        company_topics = [doc.get('dominant_topic') for doc in company_docs if doc.get('dominant_topic')]
        company_counts = Counter(company_topics)
        sorted_company = sorted(company_counts.items(), key=lambda x: x[1], reverse=True)
        
        for i, (topic, count) in enumerate(sorted_company, 1):
            percentage = (count / len(company_topics)) * 100
            print(f"{i:2d}. {topic}: {count} ({percentage:.1f}%)")
        print()

if __name__ == "__main__":
    try:
        update_action_pending_from()
        verify_action_pending_from()
        analyze_dominant_topics_by_action_pending_from()
        print("\n" + "="*80)
        print("ACTION PENDING FROM UPDATE COMPLETED SUCCESSFULLY!")
        print("="*80)
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Close the connection
        client.close()
