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

def update_action_pending_com_cus():
    """
    Update action_pending_status and action_pending_from fields based on tier specifications
    for both company and customer action pending requirements
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # Define company action pending topics by tier
        company_action_pending_topics = {
            # Tier 1: Critical Action Required - Company (193 records)
            "Data Breach Warning": {"required": 15, "tier": 1, "source": "company"},
            "Cybersecurity Incident Report": {"required": 9, "tier": 1, "source": "company"},
            "Security Incident Alert": {"required": 8, "tier": 1, "source": "company"},
            "Technology Emergency": {"required": 19, "tier": 1, "source": "company"},
            "Risk Management Alert": {"required": 31, "tier": 1, "source": "company"},
            "Sanctions Screening Alert": {"required": 12, "tier": 1, "source": "company"},
            "Covenant Breach Alert": {"required": 16, "tier": 1, "source": "company"},
            "Executive Escalation Email": {"required": 20, "tier": 1, "source": "company"},
            "Legal Escalation": {"required": 22, "tier": 1, "source": "company"},
            "Compliance Monitoring Alert": {"required": 13, "tier": 1, "source": "company"},
            "Quality Assurance Breach": {"required": 28, "tier": 1, "source": "company"},
            
            # Tier 2: Approval & Decision Required - Company (73 records)
            "Executive Decision Required": {"required": 15, "tier": 2, "source": "company"},
            "Approval Required": {"required": 11, "tier": 2, "source": "company"},
            "User Access Approval": {"required": 27, "tier": 2, "source": "company"},
            "Process Exception": {"required": 20, "tier": 2, "source": "company"},
            
            # Tier 3: Company Operations & Systems (46 records)
            "Clearing House Problem": {"required": 10, "tier": 3, "source": "company"},
            "Cross-Border Transfer Problem": {"required": 9, "tier": 3, "source": "company"},
            "Performance Management": {"required": 9, "tier": 3, "source": "company"},
            "Vendor Management": {"required": 9, "tier": 3, "source": "company"},
            "Service Level Breach": {"required": 2, "tier": 3, "source": "company"},
            "IT Marketing Unresolved": {"required": 5, "tier": 3, "source": "company"},
            "Salary Leave Issues": {"required": 5, "tier": 3, "source": "company"},
            "Compliance Issues": {"required": 2, "tier": 3, "source": "company"},
            "ECB Policy Update": {"required": 4, "tier": 3, "source": "company"},
            "Reporting Platform Issue": {"required": 3, "tier": 3, "source": "company"},
            "RegTech Alert": {"required": 3, "tier": 3, "source": "company"}
        }
        
        # Define customer action pending topics by tier
        customer_action_pending_topics = {
            # Tier 1: Customer Service & Escalation (16 records)
            "Customer Service Escalation": {"required": 16, "tier": 1, "source": "customer"},
            
            # Tier 2: Regulatory Documentation from Customer (91 records)
            "Legal Notice Response": {"required": 9, "tier": 2, "source": "customer"},
            "GDPR Access Request": {"required": 14, "tier": 2, "source": "customer"},
            "Right Erasure Demand": {"required": 7, "tier": 2, "source": "customer"},
            "Consent Withdrawal Notification": {"required": 14, "tier": 2, "source": "customer"},
            "AML Documentation Request": {"required": 9, "tier": 2, "source": "customer"},
            "KYC Documentation Problem": {"required": 13, "tier": 2, "source": "customer"},
            "FATCA Compliance Issue": {"required": 10, "tier": 2, "source": "customer"},
            "Enhanced Due Diligence": {"required": 6, "tier": 2, "source": "customer"},
            "Covenant Compliance Question": {"required": 9, "tier": 2, "source": "customer"},
            
            # Tier 3: Customer Account & Transaction Issues (79 records)
            "Letter Credit Amendment": {"required": 9, "tier": 3, "source": "customer"},
            "Account Opening Issue": {"required": 9, "tier": 3, "source": "customer"},
            "Processing Delay Complaint": {"required": 8, "tier": 3, "source": "customer"},
            "Payment Service Problem": {"required": 8, "tier": 3, "source": "customer"},
            "Regulatory Documentation Need": {"required": 8, "tier": 3, "source": "customer"},
            "CRS Reporting Question": {"required": 10, "tier": 3, "source": "customer"},
            "Card Payment Dispute": {"required": 7, "tier": 3, "source": "customer"},
            "MiFID Investment Inquiry": {"required": 8, "tier": 3, "source": "customer"},
            "Credit Line Utilization": {"required": 8, "tier": 3, "source": "customer"},
            "Letter Credit Processing": {"required": 1, "tier": 3, "source": "customer"},
            "Relationship Manager Request": {"required": 3, "tier": 3, "source": "customer"},
            
            # Tier 4: Customer Technical & Access Issues (35 records)
            "Transaction Dispute Report": {"required": 3, "tier": 4, "source": "customer"},
            "Export Documentation Error": {"required": 6, "tier": 4, "source": "customer"},
            "Account Access Error": {"required": 8, "tier": 4, "source": "customer"},
            "IT Access Problem": {"required": 10, "tier": 4, "source": "customer"},
            "Digital Platform Malfunction": {"required": 6, "tier": 4, "source": "customer"},
            "Mobile Authentication Error": {"required": 3, "tier": 4, "source": "customer"},
            
            # Tier 5: Customer Payment & Banking Operations (68 records)
            "Transaction Processing Error": {"required": 6, "tier": 5, "source": "customer"},
            "SEPA Payment Failure": {"required": 8, "tier": 5, "source": "customer"},
            "Fee Dispute Email": {"required": 3, "tier": 5, "source": "customer"},
            "Compliance Certificate Request": {"required": 2, "tier": 5, "source": "customer"},
            "API Connectivity Issue": {"required": 3, "tier": 5, "source": "customer"},
            "Online Banking Problem": {"required": 3, "tier": 5, "source": "customer"},
            "Fund Transfer Problem": {"required": 3, "tier": 5, "source": "customer"},
            "ACH Processing Error": {"required": 3, "tier": 5, "source": "customer"},
            "Check Clearing Issue": {"required": 3, "tier": 5, "source": "customer"},
            "Card Network Issue": {"required": 3, "tier": 5, "source": "customer"},
            "Foreign Exchange Problem": {"required": 3, "tier": 5, "source": "customer"},
            "International Transfer Question": {"required": 3, "tier": 5, "source": "customer"},
            "Multi-Currency Statement Need": {"required": 3, "tier": 5, "source": "customer"},
            "Strong Customer Authentication": {"required": 3, "tier": 5, "source": "customer"},
            "Fintech Integration Error": {"required": 3, "tier": 5, "source": "customer"},
            "SEPA Payment Status": {"required": 3, "tier": 5, "source": "customer"},
            "SEPA Processing Error": {"required": 2, "tier": 5, "source": "customer"},
            "Custody Service Problem": {"required": 3, "tier": 5, "source": "customer"},
            "Third Party Provider": {"required": 3, "tier": 5, "source": "customer"},
            "Derivatives Question Alert": {"required": 3, "tier": 5, "source": "customer"},
            "Export Credit Issue": {"required": 3, "tier": 5, "source": "customer"}
        }
        
        # Combine all action pending topics
        all_action_pending_topics = {**company_action_pending_topics, **customer_action_pending_topics}
        
        # First, set all records to action_pending_status: "false" and action_pending_from: null
        print("Setting all records to action_pending_status: 'false' and action_pending_from: null...")
        all_false_result = collection.update_many(
            {},
            {'$set': {'action_pending_status': 'false', 'action_pending_from': None}}
        )
        print(f"Updated {all_false_result.modified_count} records to action_pending_status: 'false'")
        
        total_company_yes = 0
        total_customer_yes = 0
        updated_company_topics = []
        updated_customer_topics = []
        
        # Update company action pending topics
        print(f"\n=== UPDATING COMPANY ACTION PENDING TOPICS ===")
        for topic, config in company_action_pending_topics.items():
            required_count = config["required"]
            tier = config["tier"]
            source = config["source"]
            
            # Get all documents with this dominant_topic
            docs_with_topic = list(collection.find(
                {'dominant_topic': topic},
                {'_id': 1}
            ))
            
            available_count = len(docs_with_topic)
            print(f"\nCompany Topic: '{topic}' (Tier {tier})")
            print(f"  Available records: {available_count}")
            print(f"  Required 'true' records: {required_count}")
            
            if available_count >= required_count:
                # Randomly select the required number of documents
                random.shuffle(docs_with_topic)
                selected_docs = docs_with_topic[:required_count]
                selected_ids = [doc['_id'] for doc in selected_docs]
                
                # Update selected documents
                result = collection.update_many(
                    {'_id': {'$in': selected_ids}},
                    {'$set': {'action_pending_status': 'true', 'action_pending_from': source}}
                )
                
                print(f"  Updated {result.modified_count} records to action_pending_status: 'true', action_pending_from: '{source}'")
                total_company_yes += result.modified_count
                updated_company_topics.append(topic)
            else:
                print(f"  ⚠ Warning: Only {available_count} records available, but {required_count} required")
                if available_count > 0:
                    # Update all available records
                    doc_ids = [doc['_id'] for doc in docs_with_topic]
                    result = collection.update_many(
                        {'_id': {'$in': doc_ids}},
                        {'$set': {'action_pending_status': 'true', 'action_pending_from': source}}
                    )
                    print(f"  Updated all {result.modified_count} available records to action_pending_status: 'true', action_pending_from: '{source}'")
                    total_company_yes += result.modified_count
                    updated_company_topics.append(topic)
        
        # Update customer action pending topics
        print(f"\n=== UPDATING CUSTOMER ACTION PENDING TOPICS ===")
        for topic, config in customer_action_pending_topics.items():
            required_count = config["required"]
            tier = config["tier"]
            source = config["source"]
            
            # Get all documents with this dominant_topic
            docs_with_topic = list(collection.find(
                {'dominant_topic': topic},
                {'_id': 1}
            ))
            
            available_count = len(docs_with_topic)
            print(f"\nCustomer Topic: '{topic}' (Tier {tier})")
            print(f"  Available records: {available_count}")
            print(f"  Required 'true' records: {required_count}")
            
            if available_count >= required_count:
                # Randomly select the required number of documents
                random.shuffle(docs_with_topic)
                selected_docs = docs_with_topic[:required_count]
                selected_ids = [doc['_id'] for doc in selected_docs]
                
                # Update selected documents
                result = collection.update_many(
                    {'_id': {'$in': selected_ids}},
                    {'$set': {'action_pending_status': 'true', 'action_pending_from': source}}
                )
                
                print(f"  Updated {result.modified_count} records to action_pending_status: 'true', action_pending_from: '{source}'")
                total_customer_yes += result.modified_count
                updated_customer_topics.append(topic)
            else:
                print(f"  ⚠ Warning: Only {available_count} records available, but {required_count} required")
                if available_count > 0:
                    # Update all available records
                    doc_ids = [doc['_id'] for doc in docs_with_topic]
                    result = collection.update_many(
                        {'_id': {'$in': doc_ids}},
                        {'$set': {'action_pending_status': 'true', 'action_pending_from': source}}
                    )
                    print(f"  Updated all {result.modified_count} available records to action_pending_status: 'true', action_pending_from: '{source}'")
                    total_customer_yes += result.modified_count
                    updated_customer_topics.append(topic)
        
        print(f"\nTotal 'true' records from company topics: {total_company_yes}")
        print(f"Total 'true' records from customer topics: {total_customer_yes}")
        print(f"Total 'true' records: {total_company_yes + total_customer_yes}")
        
        # Verification
        print(f"\n=== VERIFICATION ===")
        total_true = collection.count_documents({'action_pending_status': 'true'})
        total_false = collection.count_documents({'action_pending_status': 'false'})
        total_docs = collection.count_documents({})
        
        company_true = collection.count_documents({'action_pending_status': 'true', 'action_pending_from': 'company'})
        customer_true = collection.count_documents({'action_pending_status': 'true', 'action_pending_from': 'customer'})
        
        print(f"Total documents: {total_docs}")
        print(f"Documents with action_pending_status: 'true': {total_true}")
        print(f"Documents with action_pending_status: 'false': {total_false}")
        print(f"Company action pending: {company_true}")
        print(f"Customer action pending: {customer_true}")
        
        # Show breakdown by tier for company
        print(f"\nCompany action pending status by tier:")
        for tier in range(1, 4):
            tier_topics = [topic for topic, config in company_action_pending_topics.items() if config["tier"] == tier]
            tier_true = 0
            for topic in tier_topics:
                true_count = collection.count_documents({
                    'dominant_topic': topic,
                    'action_pending_status': 'true',
                    'action_pending_from': 'company'
                })
                tier_true += true_count
            print(f"  Tier {tier}: {tier_true} 'true' records")
        
        # Show breakdown by tier for customer
        print(f"\nCustomer action pending status by tier:")
        for tier in range(1, 6):
            tier_topics = [topic for topic, config in customer_action_pending_topics.items() if config["tier"] == tier]
            tier_true = 0
            for topic in tier_topics:
                true_count = collection.count_documents({
                    'dominant_topic': topic,
                    'action_pending_status': 'true',
                    'action_pending_from': 'customer'
                })
                tier_true += true_count
            print(f"  Tier {tier}: {tier_true} 'true' records")
        
        # Show sample true documents
        print(f"\nSample 'true' documents:")
        sample_docs = collection.find(
            {'action_pending_status': 'true'},
            {'dominant_topic': 1, 'action_pending_status': 1, 'action_pending_from': 1}
        ).limit(10)
        
        for doc in sample_docs:
            print(f"  {doc['dominant_topic']}: action_pending_status = {doc['action_pending_status']}, action_pending_from = {doc['action_pending_from']}")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'true_documents': total_true,
            'false_documents': total_false,
            'company_true': company_true,
            'customer_true': customer_true,
            'company_topics_updated': updated_company_topics,
            'customer_topics_updated': updated_customer_topics
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting action pending status and source field update process...\n")
    
    # Display company topics by tier
    print("COMPANY ACTION PENDING TOPICS:")
    company_tiers = {
        1: "Critical Action Required - Company (193 records)",
        2: "Approval & Decision Required - Company (73 records)", 
        3: "Company Operations & Systems (46 records)"
    }
    
    company_action_pending_topics = {
        "Data Breach Warning": {"required": 15, "tier": 1, "source": "company"},
        "Cybersecurity Incident Report": {"required": 9, "tier": 1, "source": "company"},
        "Security Incident Alert": {"required": 8, "tier": 1, "source": "company"},
        "Technology Emergency": {"required": 19, "tier": 1, "source": "company"},
        "Risk Management Alert": {"required": 31, "tier": 1, "source": "company"},
        "Sanctions Screening Alert": {"required": 12, "tier": 1, "source": "company"},
        "Covenant Breach Alert": {"required": 16, "tier": 1, "source": "company"},
        "Executive Escalation Email": {"required": 20, "tier": 1, "source": "company"},
        "Legal Escalation": {"required": 22, "tier": 1, "source": "company"},
        "Compliance Monitoring Alert": {"required": 13, "tier": 1, "source": "company"},
        "Quality Assurance Breach": {"required": 28, "tier": 1, "source": "company"},
        "Executive Decision Required": {"required": 15, "tier": 2, "source": "company"},
        "Approval Required": {"required": 11, "tier": 2, "source": "company"},
        "User Access Approval": {"required": 27, "tier": 2, "source": "company"},
        "Process Exception": {"required": 20, "tier": 2, "source": "company"},
        "Clearing House Problem": {"required": 10, "tier": 3, "source": "company"},
        "Cross-Border Transfer Problem": {"required": 9, "tier": 3, "source": "company"},
        "Performance Management": {"required": 9, "tier": 3, "source": "company"},
        "Vendor Management": {"required": 9, "tier": 3, "source": "company"},
        "Service Level Breach": {"required": 2, "tier": 3, "source": "company"},
        "IT Marketing Unresolved": {"required": 5, "tier": 3, "source": "company"},
        "Salary Leave Issues": {"required": 5, "tier": 3, "source": "company"},
        "Compliance Issues": {"required": 2, "tier": 3, "source": "company"},
        "ECB Policy Update": {"required": 4, "tier": 3, "source": "company"},
        "Reporting Platform Issue": {"required": 3, "tier": 3, "source": "company"},
        "RegTech Alert": {"required": 3, "tier": 3, "source": "company"}
    }
    
    for tier, description in company_tiers.items():
        tier_topics = [topic for topic, config in company_action_pending_topics.items() if config["tier"] == tier]
        print(f"\nTier {tier}: {description}")
        for topic in tier_topics:
            config = company_action_pending_topics[topic]
            print(f"  {topic}: {config['required']} records")
    
    # Display customer topics by tier
    print(f"\n\nCUSTOMER ACTION PENDING TOPICS:")
    customer_tiers = {
        1: "Customer Service & Escalation (16 records)",
        2: "Regulatory Documentation from Customer (91 records)",
        3: "Customer Account & Transaction Issues (79 records)",
        4: "Customer Technical & Access Issues (35 records)",
        5: "Customer Payment & Banking Operations (68 records)"
    }
    
    customer_action_pending_topics = {
        "Customer Service Escalation": {"required": 16, "tier": 1, "source": "customer"},
        "Legal Notice Response": {"required": 9, "tier": 2, "source": "customer"},
        "GDPR Access Request": {"required": 14, "tier": 2, "source": "customer"},
        "Right Erasure Demand": {"required": 7, "tier": 2, "source": "customer"},
        "Consent Withdrawal Notification": {"required": 14, "tier": 2, "source": "customer"},
        "AML Documentation Request": {"required": 9, "tier": 2, "source": "customer"},
        "KYC Documentation Problem": {"required": 13, "tier": 2, "source": "customer"},
        "FATCA Compliance Issue": {"required": 10, "tier": 2, "source": "customer"},
        "Enhanced Due Diligence": {"required": 6, "tier": 2, "source": "customer"},
        "Covenant Compliance Question": {"required": 9, "tier": 2, "source": "customer"},
        "Letter Credit Amendment": {"required": 9, "tier": 3, "source": "customer"},
        "Account Opening Issue": {"required": 9, "tier": 3, "source": "customer"},
        "Processing Delay Complaint": {"required": 8, "tier": 3, "source": "customer"},
        "Payment Service Problem": {"required": 8, "tier": 3, "source": "customer"},
        "Regulatory Documentation Need": {"required": 8, "tier": 3, "source": "customer"},
        "CRS Reporting Question": {"required": 10, "tier": 3, "source": "customer"},
        "Card Payment Dispute": {"required": 7, "tier": 3, "source": "customer"},
        "MiFID Investment Inquiry": {"required": 8, "tier": 3, "source": "customer"},
        "Credit Line Utilization": {"required": 8, "tier": 3, "source": "customer"},
        "Letter Credit Processing": {"required": 1, "tier": 3, "source": "customer"},
        "Relationship Manager Request": {"required": 3, "tier": 3, "source": "customer"},
        "Transaction Dispute Report": {"required": 3, "tier": 4, "source": "customer"},
        "Export Documentation Error": {"required": 6, "tier": 4, "source": "customer"},
        "Account Access Error": {"required": 8, "tier": 4, "source": "customer"},
        "IT Access Problem": {"required": 10, "tier": 4, "source": "customer"},
        "Digital Platform Malfunction": {"required": 6, "tier": 4, "source": "customer"},
        "Mobile Authentication Error": {"required": 3, "tier": 4, "source": "customer"},
        "Transaction Processing Error": {"required": 6, "tier": 5, "source": "customer"},
        "SEPA Payment Failure": {"required": 8, "tier": 5, "source": "customer"},
        "Fee Dispute Email": {"required": 3, "tier": 5, "source": "customer"},
        "Compliance Certificate Request": {"required": 2, "tier": 5, "source": "customer"},
        "API Connectivity Issue": {"required": 3, "tier": 5, "source": "customer"},
        "Online Banking Problem": {"required": 3, "tier": 5, "source": "customer"},
        "Fund Transfer Problem": {"required": 3, "tier": 5, "source": "customer"},
        "ACH Processing Error": {"required": 3, "tier": 5, "source": "customer"},
        "Check Clearing Issue": {"required": 3, "tier": 5, "source": "customer"},
        "Card Network Issue": {"required": 3, "tier": 5, "source": "customer"},
        "Foreign Exchange Problem": {"required": 3, "tier": 5, "source": "customer"},
        "International Transfer Question": {"required": 3, "tier": 5, "source": "customer"},
        "Multi-Currency Statement Need": {"required": 3, "tier": 5, "source": "customer"},
        "Strong Customer Authentication": {"required": 3, "tier": 5, "source": "customer"},
        "Fintech Integration Error": {"required": 3, "tier": 5, "source": "customer"},
        "SEPA Payment Status": {"required": 3, "tier": 5, "source": "customer"},
        "SEPA Processing Error": {"required": 2, "tier": 5, "source": "customer"},
        "Custody Service Problem": {"required": 3, "tier": 5, "source": "customer"},
        "Third Party Provider": {"required": 3, "tier": 5, "source": "customer"},
        "Derivatives Question Alert": {"required": 3, "tier": 5, "source": "customer"},
        "Export Credit Issue": {"required": 3, "tier": 5, "source": "customer"}
    }
    
    for tier, description in customer_tiers.items():
        tier_topics = [topic for topic, config in customer_action_pending_topics.items() if config["tier"] == tier]
        print(f"\nTier {tier}: {description}")
        for topic in tier_topics:
            config = customer_action_pending_topics[topic]
            print(f"  {topic}: {config['required']} records")
    
    print(f"\nAll other records will be set to action_pending_status: 'false' and action_pending_from: null")
    print()
    
    result = update_action_pending_com_cus()
    
    if result:
        print(f"\nProcess completed successfully!")
        print(f"Total 'true' documents: {result['true_documents']}")
        print(f"Total 'false' documents: {result['false_documents']}")
        print(f"Company action pending: {result['company_true']}")
        print(f"Customer action pending: {result['customer_true']}")
    else:
        print("\nProcess failed!")
