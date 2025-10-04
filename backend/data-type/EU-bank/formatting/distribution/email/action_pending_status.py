# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import random

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def update_action_pending_status():
    """
    Update action_pending_status field based on tier specifications
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # Define specific topics and their action pending requirements by tier
        action_pending_topics = {
            # Tier 1: Critical Action Required - 100% Action Pending
            "Data Breach Warning": {"required": 15, "tier": 1, "percentage": 100},
            "Cybersecurity Incident Report": {"required": 9, "tier": 1, "percentage": 100},
            "Security Incident Alert": {"required": 8, "tier": 1, "percentage": 100},
            "Technology Emergency": {"required": 19, "tier": 1, "percentage": 100},
            "Risk Management Alert": {"required": 31, "tier": 1, "percentage": 100},
            "Sanctions Screening Alert": {"required": 12, "tier": 1, "percentage": 100},
            "Covenant Breach Alert": {"required": 16, "tier": 1, "percentage": 100},
            "Executive Escalation Email": {"required": 20, "tier": 1, "percentage": 100},
            "Legal Escalation": {"required": 22, "tier": 1, "percentage": 100},
            "Compliance Monitoring Alert": {"required": 13, "tier": 1, "percentage": 100},
            "Quality Assurance Breach": {"required": 28, "tier": 1, "percentage": 100},
            "Customer Service Escalation": {"required": 16, "tier": 1, "percentage": 100},
            
            # Tier 2: Approval & Decision Required - 90% Action Pending
            "Executive Decision Required": {"required": 15, "tier": 2, "percentage": 90},
            "Approval Required": {"required": 11, "tier": 2, "percentage": 90},
            "User Access Approval": {"required": 27, "tier": 2, "percentage": 90},
            "Process Exception": {"required": 20, "tier": 2, "percentage": 90},
            
            # Tier 3: Regulatory Action Required - 70% Action Pending
            "Legal Notice Response": {"required": 9, "tier": 3, "percentage": 70},
            "GDPR Access Request": {"required": 14, "tier": 3, "percentage": 70},
            "Right Erasure Demand": {"required": 7, "tier": 3, "percentage": 70},
            "Consent Withdrawal Notification": {"required": 14, "tier": 3, "percentage": 70},
            "AML Documentation Request": {"required": 9, "tier": 3, "percentage": 70},
            "KYC Documentation Problem": {"required": 13, "tier": 3, "percentage": 70},
            "FATCA Compliance Issue": {"required": 10, "tier": 3, "percentage": 70},
            "Enhanced Due Diligence": {"required": 6, "tier": 3, "percentage": 70},
            "Covenant Compliance Question": {"required": 14, "tier": 3, "percentage": 70},
            
            # Tier 4: High Priority Operations - 50% Action Pending
            "Letter Credit Amendment": {"required": 9, "tier": 4, "percentage": 50},
            "Account Opening Issue": {"required": 9, "tier": 4, "percentage": 50},
            "Processing Delay Complaint": {"required": 8, "tier": 4, "percentage": 50},
            "Payment Service Problem": {"required": 8, "tier": 4, "percentage": 50},
            "Clearing House Problem": {"required": 10, "tier": 4, "percentage": 50},
            "Cross-Border Transfer Problem": {"required": 9, "tier": 4, "percentage": 50},
            "Regulatory Documentation Need": {"required": 8, "tier": 4, "percentage": 50},
            "CRS Reporting Question": {"required": 10, "tier": 4, "percentage": 50},
            "Card Payment Dispute": {"required": 7, "tier": 4, "percentage": 50},
            
            # Tier 5: Medium Priority Operations - 40% Action Pending
            "MiFID Investment Inquiry": {"required": 8, "tier": 5, "percentage": 40},
            "Transaction Dispute Report": {"required": 3, "tier": 5, "percentage": 40},
            "Export Documentation Error": {"required": 6, "tier": 5, "percentage": 40},
            "Account Access Error": {"required": 8, "tier": 5, "percentage": 40},
            "IT Access Problem": {"required": 10, "tier": 5, "percentage": 40},
            "Digital Platform Malfunction": {"required": 6, "tier": 5, "percentage": 40},
            "Transaction Processing Error": {"required": 6, "tier": 5, "percentage": 40},
            "SEPA Payment Failure": {"required": 8, "tier": 5, "percentage": 40},
            "Credit Line Utilization": {"required": 8, "tier": 5, "percentage": 40},
            "Performance Management": {"required": 9, "tier": 5, "percentage": 40},
            "Vendor Management": {"required": 9, "tier": 5, "percentage": 40},
            
            # Tier 6: Lower Priority - 20% Action Pending
            "Compliance Issues": {"required": 2, "tier": 6, "percentage": 20},
            "Letter Credit Processing": {"required": 1, "tier": 6, "percentage": 20},
            "Fee Dispute Email": {"required": 3, "tier": 6, "percentage": 20},
            "Compliance Certificate Request": {"required": 2, "tier": 6, "percentage": 20},
            "Service Level Breach": {"required": 2, "tier": 6, "percentage": 20},
            "Relationship Manager Request": {"required": 3, "tier": 6, "percentage": 20},
            "Mobile Authentication Error": {"required": 3, "tier": 6, "percentage": 20},
            "Salary Leave Issues": {"required": 5, "tier": 6, "percentage": 20},
            "IT Marketing Unresolved": {"required": 5, "tier": 6, "percentage": 20},
            "API Connectivity Issue": {"required": 3, "tier": 6, "percentage": 20},
            "Online Banking Problem": {"required": 3, "tier": 6, "percentage": 20},
            "Fund Transfer Problem": {"required": 3, "tier": 6, "percentage": 20},
            "ACH Processing Error": {"required": 3, "tier": 6, "percentage": 20},
            "Check Clearing Issue": {"required": 3, "tier": 6, "percentage": 20},
            "Card Network Issue": {"required": 3, "tier": 6, "percentage": 20},
            "Foreign Exchange Problem": {"required": 3, "tier": 6, "percentage": 20},
            "International Transfer Question": {"required": 3, "tier": 6, "percentage": 20},
            "Multi-Currency Statement Need": {"required": 3, "tier": 6, "percentage": 20},
            "Strong Customer Authentication": {"required": 3, "tier": 6, "percentage": 20},
            "Reporting Platform Issue": {"required": 3, "tier": 6, "percentage": 20},
            "Fintech Integration Error": {"required": 3, "tier": 6, "percentage": 20},
            "ECB Policy Update": {"required": 4, "tier": 6, "percentage": 20},
            "SEPA Payment Status": {"required": 3, "tier": 6, "percentage": 20},
            "SEPA Processing Error": {"required": 2, "tier": 6, "percentage": 20},
            "Custody Service Problem": {"required": 3, "tier": 6, "percentage": 20},
            "Third Party Provider": {"required": 3, "tier": 6, "percentage": 20},
            "Derivatives Question Alert": {"required": 3, "tier": 6, "percentage": 20},
            "RegTech Alert": {"required": 3, "tier": 6, "percentage": 20},
            "Export Credit Issue": {"required": 3, "tier": 6, "percentage": 20}
        }
        
        # First, set all records to action_pending_status: "false"
        print("Setting all records to action_pending_status: 'false'...")
        all_false_result = collection.update_many(
            {},
            {'$set': {'action_pending_status': 'false'}}
        )
        print(f"Updated {all_false_result.modified_count} records to action_pending_status: 'false'")
        
        total_yes_from_topics = 0
        updated_topics = []
        
        # Update specific topics with action_pending_status: "true"
        print(f"\nUpdating specific topics with action_pending_status: 'true'...")
        for topic, config in action_pending_topics.items():
            required_count = config["required"]
            tier = config["tier"]
            percentage = config["percentage"]
            
            # Get all documents with this dominant_topic
            docs_with_topic = list(collection.find(
                {'dominant_topic': topic},
                {'_id': 1}
            ))
            
            available_count = len(docs_with_topic)
            print(f"\nTopic: '{topic}' (Tier {tier}, {percentage}%)")
            print(f"  Available records: {available_count}")
            print(f"  Required 'true' records: {required_count}")
            
            if available_count >= required_count:
                # Randomly select the required number of documents
                random.shuffle(docs_with_topic)
                selected_docs = docs_with_topic[:required_count]
                selected_ids = [doc['_id'] for doc in selected_docs]
                
                # Update selected documents to action_pending_status: "true"
                result = collection.update_many(
                    {'_id': {'$in': selected_ids}},
                    {'$set': {'action_pending_status': 'true'}}
                )
                
                print(f"  Updated {result.modified_count} records to action_pending_status: 'true'")
                total_yes_from_topics += result.modified_count
                updated_topics.append(topic)
            else:
                print(f"  ⚠ Warning: Only {available_count} records available, but {required_count} required")
                if available_count > 0:
                    # Update all available records
                    doc_ids = [doc['_id'] for doc in docs_with_topic]
                    result = collection.update_many(
                        {'_id': {'$in': doc_ids}},
                        {'$set': {'action_pending_status': 'true'}}
                    )
                    print(f"  Updated all {result.modified_count} available records to action_pending_status: 'true'")
                    total_yes_from_topics += result.modified_count
                    updated_topics.append(topic)
        
        print(f"\nTotal 'true' records from specific topics: {total_yes_from_topics}")
        
        # Verification
        print(f"\n=== VERIFICATION ===")
        total_true = collection.count_documents({'action_pending_status': 'true'})
        total_false = collection.count_documents({'action_pending_status': 'false'})
        total_docs = collection.count_documents({})
        
        print(f"Total documents: {total_docs}")
        print(f"Documents with action_pending_status: 'true': {total_true}")
        print(f"Documents with action_pending_status: 'false': {total_false}")
        
        # Show breakdown by tier
        print(f"\nAction pending status by tier:")
        for tier in range(1, 7):
            tier_topics = [topic for topic, config in action_pending_topics.items() if config["tier"] == tier]
            tier_true = 0
            for topic in tier_topics:
                true_count = collection.count_documents({
                    'dominant_topic': topic,
                    'action_pending_status': 'true'
                })
                tier_true += true_count
            print(f"  Tier {tier}: {tier_true} 'true' records")
        
        # Show sample true documents
        print(f"\nSample 'true' documents:")
        sample_docs = collection.find(
            {'action_pending_status': 'true'},
            {'dominant_topic': 1, 'action_pending_status': 1}
        ).limit(10)
        
        for doc in sample_docs:
            print(f"  {doc['dominant_topic']}: action_pending_status = {doc['action_pending_status']}")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'true_documents': total_true,
            'false_documents': total_false,
            'true_from_topics': total_yes_from_topics,
            'updated_topics': updated_topics
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting action pending status field update process...\n")
    print("Topics to be marked with action_pending_status: 'true':")
    
    # Group topics by tier for display
    tier_groups = {
        1: "Critical Action Required - 100% Action Pending",
        2: "Approval & Decision Required - 90% Action Pending", 
        3: "Regulatory Action Required - 70% Action Pending",
        4: "High Priority Operations - 50% Action Pending",
        5: "Medium Priority Operations - 40% Action Pending",
        6: "Lower Priority - 20% Action Pending"
    }
    
    action_pending_topics = {
        "Data Breach Warning": {"required": 15, "tier": 1, "percentage": 100},
        "Cybersecurity Incident Report": {"required": 9, "tier": 1, "percentage": 100},
        "Security Incident Alert": {"required": 8, "tier": 1, "percentage": 100},
        "Technology Emergency": {"required": 19, "tier": 1, "percentage": 100},
        "Risk Management Alert": {"required": 31, "tier": 1, "percentage": 100},
        "Sanctions Screening Alert": {"required": 12, "tier": 1, "percentage": 100},
        "Covenant Breach Alert": {"required": 16, "tier": 1, "percentage": 100},
        "Executive Escalation Email": {"required": 20, "tier": 1, "percentage": 100},
        "Legal Escalation": {"required": 22, "tier": 1, "percentage": 100},
        "Compliance Monitoring Alert": {"required": 13, "tier": 1, "percentage": 100},
        "Quality Assurance Breach": {"required": 28, "tier": 1, "percentage": 100},
        "Customer Service Escalation": {"required": 16, "tier": 1, "percentage": 100},
        "Executive Decision Required": {"required": 15, "tier": 2, "percentage": 90},
        "Approval Required": {"required": 11, "tier": 2, "percentage": 90},
        "User Access Approval": {"required": 27, "tier": 2, "percentage": 90},
        "Process Exception": {"required": 20, "tier": 2, "percentage": 90},
        "Legal Notice Response": {"required": 9, "tier": 3, "percentage": 70},
        "GDPR Access Request": {"required": 14, "tier": 3, "percentage": 70},
        "Right Erasure Demand": {"required": 7, "tier": 3, "percentage": 70},
        "Consent Withdrawal Notification": {"required": 14, "tier": 3, "percentage": 70},
        "AML Documentation Request": {"required": 9, "tier": 3, "percentage": 70},
        "KYC Documentation Problem": {"required": 13, "tier": 3, "percentage": 70},
        "FATCA Compliance Issue": {"required": 10, "tier": 3, "percentage": 70},
        "Enhanced Due Diligence": {"required": 6, "tier": 3, "percentage": 70},
        "Covenant Compliance Question": {"required": 14, "tier": 3, "percentage": 70},
        "Letter Credit Amendment": {"required": 9, "tier": 4, "percentage": 50},
        "Account Opening Issue": {"required": 9, "tier": 4, "percentage": 50},
        "Processing Delay Complaint": {"required": 8, "tier": 4, "percentage": 50},
        "Payment Service Problem": {"required": 8, "tier": 4, "percentage": 50},
        "Clearing House Problem": {"required": 10, "tier": 4, "percentage": 50},
        "Cross-Border Transfer Problem": {"required": 9, "tier": 4, "percentage": 50},
        "Regulatory Documentation Need": {"required": 8, "tier": 4, "percentage": 50},
        "CRS Reporting Question": {"required": 10, "tier": 4, "percentage": 50},
        "Card Payment Dispute": {"required": 7, "tier": 4, "percentage": 50},
        "MiFID Investment Inquiry": {"required": 8, "tier": 5, "percentage": 40},
        "Transaction Dispute Report": {"required": 3, "tier": 5, "percentage": 40},
        "Export Documentation Error": {"required": 6, "tier": 5, "percentage": 40},
        "Account Access Error": {"required": 8, "tier": 5, "percentage": 40},
        "IT Access Problem": {"required": 10, "tier": 5, "percentage": 40},
        "Digital Platform Malfunction": {"required": 6, "tier": 5, "percentage": 40},
        "Transaction Processing Error": {"required": 6, "tier": 5, "percentage": 40},
        "SEPA Payment Failure": {"required": 8, "tier": 5, "percentage": 40},
        "Credit Line Utilization": {"required": 8, "tier": 5, "percentage": 40},
        "Performance Management": {"required": 9, "tier": 5, "percentage": 40},
        "Vendor Management": {"required": 9, "tier": 5, "percentage": 40},
        "Compliance Issues": {"required": 2, "tier": 6, "percentage": 20},
        "Letter Credit Processing": {"required": 1, "tier": 6, "percentage": 20},
        "Fee Dispute Email": {"required": 3, "tier": 6, "percentage": 20},
        "Compliance Certificate Request": {"required": 2, "tier": 6, "percentage": 20},
        "Service Level Breach": {"required": 2, "tier": 6, "percentage": 20},
        "Relationship Manager Request": {"required": 3, "tier": 6, "percentage": 20},
        "Mobile Authentication Error": {"required": 3, "tier": 6, "percentage": 20},
        "Salary Leave Issues": {"required": 5, "tier": 6, "percentage": 20},
        "IT Marketing Unresolved": {"required": 5, "tier": 6, "percentage": 20},
        "API Connectivity Issue": {"required": 3, "tier": 6, "percentage": 20},
        "Online Banking Problem": {"required": 3, "tier": 6, "percentage": 20},
        "Fund Transfer Problem": {"required": 3, "tier": 6, "percentage": 20},
        "ACH Processing Error": {"required": 3, "tier": 6, "percentage": 20},
        "Check Clearing Issue": {"required": 3, "tier": 6, "percentage": 20},
        "Card Network Issue": {"required": 3, "tier": 6, "percentage": 20},
        "Foreign Exchange Problem": {"required": 3, "tier": 6, "percentage": 20},
        "International Transfer Question": {"required": 3, "tier": 6, "percentage": 20},
        "Multi-Currency Statement Need": {"required": 3, "tier": 6, "percentage": 20},
        "Strong Customer Authentication": {"required": 3, "tier": 6, "percentage": 20},
        "Reporting Platform Issue": {"required": 3, "tier": 6, "percentage": 20},
        "Fintech Integration Error": {"required": 3, "tier": 6, "percentage": 20},
        "ECB Policy Update": {"required": 4, "tier": 6, "percentage": 20},
        "SEPA Payment Status": {"required": 3, "tier": 6, "percentage": 20},
        "SEPA Processing Error": {"required": 2, "tier": 6, "percentage": 20},
        "Custody Service Problem": {"required": 3, "tier": 6, "percentage": 20},
        "Third Party Provider": {"required": 3, "tier": 6, "percentage": 20},
        "Derivatives Question Alert": {"required": 3, "tier": 6, "percentage": 20},
        "RegTech Alert": {"required": 3, "tier": 6, "percentage": 20},
        "Export Credit Issue": {"required": 3, "tier": 6, "percentage": 20}
    }
    
    for tier, description in tier_groups.items():
        tier_topics = [topic for topic, config in action_pending_topics.items() if config["tier"] == tier]
        print(f"\nTier {tier}: {description}")
        for topic in tier_topics:
            config = action_pending_topics[topic]
            print(f"  {topic}: {config['required']} records ({config['percentage']}%)")
    
    print(f"\nAll other records will be set to action_pending_status: 'false'")
    print()
    
    result = update_action_pending_status()
    
    if result:
        print(f"\nProcess completed successfully!")
        print(f"Total 'true' documents: {result['true_documents']}")
        print(f"Total 'false' documents: {result['false_documents']}")
    else:
        print("\nProcess failed!")
