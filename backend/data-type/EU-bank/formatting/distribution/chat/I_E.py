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

# Define internal and external topics
INTERNAL_TOPICS = [
    "Shift Coverage Request",
    "Training Schedule Update",
    "Performance Review Planning",
    "Break Schedule Coordination",
    "Overtime Approval Need",
    "Team Meeting Reminder",
    "Staff Management Discussion",
    "Workforce Planning Update",
    "Employee Relations Coordination",
    "Attendance Management Alert",
    "Leave Request Approval",
    "Policy Clarification Request",
    "Compliance Training Reminder",
    "Skill Development Update",
    "System Status Alert",
    "Network Performance Issue",
    "Application Server Warning",
    "Database Access Problem",
    "Authentication Service Down",
    "Security Protocol Change",
    "Hardware Troubleshooting Help",
    "Software Issue Resolution",
    "User Access Problem",
    "Technology Rollout Update",
    "System Integration Progress",
    "Cybersecurity Alert Warning",
    "Process Coordination Discussion",
    "Workflow Optimization Alert",
    "Quality Standard Reminder",
    "Case Management Discussion",
    "Knowledge Management Update",
    "Team Coordination Update",
    "Daily Operational Procedure",
    "Compliance Monitoring Update",
    "Business Continuity Activation",
    "Performance Metric Tracking",
    "Cross-Department Project Update",
    "Campaign Coordination Discussion",
    "Content Management Update",
    "Budget Management Discussion",
    "Financial Reporting Procedure",
    "Account Access Problem",
    "Documentation Request Alert",
    "Compliance Report Generation",
    "Audit Planning Coordination",
    "Risk Assessment Documentation",
    "Control Testing Validation",
    "Compliance Gap Remediation",
    "Audit Finding Resolution",
    "Audit Coordination Schedule",
    "Risk Assessment Update",
    "Product Update Communication",
    "Pipeline Management Discussion",
    "Monthly Compliance Audit",
    "Audit Feedback Review"
]

EXTERNAL_TOPICS = [
    "Customer Insight Report",
    "Account Management Strategy",
    "Customer Research Finding",
    "Regulatory Update Alert",
    "Vendor Management Discussion",
    "Transaction Status Inquiry",
    "GDPR Access Request",
    "Technology Provider Communication",
    "Regulatory Examination Preparation",
    "Regulatory Response Management"
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

def update_chat_new_with_category():
    """
    Update all documents in 'chat_new' collection to add category field
    based on dominant_topic
    """
    
    # Get all documents from chat_new collection
    chat_documents = list(chat_new_collection.find({}))
    
    print(f"Found {len(chat_documents)} documents in chat_new collection")
    
    updated_count = 0
    internal_count = 0
    external_count = 0
    unknown_count = 0
    
    for doc in chat_documents:
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
        
        # Update the document with category field
        try:
            result = chat_new_collection.update_one(
                {'_id': doc_id},
                {'$set': {'category': category}}
            )
            if result.modified_count > 0:
                updated_count += 1
        except Exception as e:
            print(f"Error updating document {doc_id}: {str(e)}")
    
    print(f"Successfully updated {updated_count} documents in chat_new collection")
    print(f"Category distribution:")
    print(f"  - Internal: {internal_count}")
    print(f"  - External: {external_count}")
    print(f"  - Unknown: {unknown_count}")
    
    # Verify the changes by checking sample documents
    print("\nSample documents after update:")
    
    # Check one Internal category
    internal_sample = chat_new_collection.find_one({'category': 'Internal'})
    if internal_sample:
        print(f"Internal sample - dominant_topic: '{internal_sample.get('dominant_topic')}', category: '{internal_sample.get('category')}'")
    
    # Check one External category
    external_sample = chat_new_collection.find_one({'category': 'External'})
    if external_sample:
        print(f"External sample - dominant_topic: '{external_sample.get('dominant_topic')}', category: '{external_sample.get('category')}'")
    
    # Check one Unknown category (if any)
    unknown_sample = chat_new_collection.find_one({'category': 'Unknown'})
    if unknown_sample:
        print(f"Unknown sample - dominant_topic: '{unknown_sample.get('dominant_topic')}', category: '{unknown_sample.get('category')}'")
    
    # Show unique dominant topics that are unknown
    if unknown_count > 0:
        unknown_topics = chat_new_collection.distinct('dominant_topic', {'category': 'Unknown'})
        print(f"\nUnknown dominant topics found: {unknown_topics}")

if __name__ == "__main__":
    try:
        update_chat_new_with_category()
        print("Operation completed successfully!")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
    finally:
        # Close the connection
        client.close()