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

def update_followup_field():
    """
    Update follow_up_required field based on specific dominant_topic matches and percentages
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # Define specific topics and their follow-up requirements
        followup_topics = {
            # 100% Follow-up Required
            "Data Breach Warning": {"required": 15, "total": 15, "percentage": 100},
            "Cybersecurity Incident Report": {"required": 9, "total": 9, "percentage": 100},
            "Security Incident Alert": {"required": 8, "total": 8, "percentage": 100},
            "Technology Emergency": {"required": 19, "total": 19, "percentage": 100},
            "Risk Management Alert": {"required": 31, "total": 31, "percentage": 100},
            "Sanctions Screening Alert": {"required": 12, "total": 12, "percentage": 100},
            "Covenant Breach Alert": {"required": 16, "total": 16, "percentage": 100},
            "Executive Escalation Email": {"required": 20, "total": 20, "percentage": 100},
            "Legal Escalation": {"required": 22, "total": 22, "percentage": 100},
            "Compliance Monitoring Alert": {"required": 13, "total": 13, "percentage": 100},
            "Quality Assurance Breach": {"required": 28, "total": 28, "percentage": 100},
            "Customer Service Escalation": {"required": 16, "total": 16, "percentage": 100},
            "Legal Notice Response": {"required": 13, "total": 13, "percentage": 100},
            "Executive Decision Required": {"required": 17, "total": 17, "percentage": 100},
            "Approval Required": {"required": 12, "total": 12, "percentage": 100},
            "User Access Approval": {"required": 30, "total": 30, "percentage": 100},
            "GDPR Access Request": {"required": 20, "total": 20, "percentage": 100},
            "Right Erasure Demand": {"required": 10, "total": 10, "percentage": 100},
            "Consent Withdrawal Notification": {"required": 20, "total": 20, "percentage": 100},
            "AML Documentation Request": {"required": 13, "total": 13, "percentage": 100},
            "KYC Documentation Problem": {"required": 18, "total": 18, "percentage": 100},
            "FATCA Compliance Issue": {"required": 14, "total": 14, "percentage": 100},
            "Enhanced Due Diligence": {"required": 8, "total": 8, "percentage": 100},
            "Covenant Compliance Question": {"required": 20, "total": 20, "percentage": 100},
            "Process Exception": {"required": 22, "total": 22, "percentage": 100},
            "Performance Management": {"required": 22, "total": 22, "percentage": 100},
            "Vendor Management": {"required": 22, "total": 22, "percentage": 100},
            "Compliance Issues": {"required": 11, "total": 11, "percentage": 100},
            
            # 70% Follow-up Required
            "Letter Credit Amendment": {"required": 13, "total": 18, "percentage": 70},
            "Account Opening Issue": {"required": 13, "total": 18, "percentage": 70},
            "Processing Delay Complaint": {"required": 11, "total": 16, "percentage": 70},
            "Payment Service Problem": {"required": 11, "total": 15, "percentage": 70},
            "Clearing House Problem": {"required": 13, "total": 19, "percentage": 70},
            "Cross-Border Transfer Problem": {"required": 12, "total": 17, "percentage": 70},
            "Regulatory Documentation Need": {"required": 11, "total": 15, "percentage": 70},
            "CRS Reporting Question": {"required": 13, "total": 19, "percentage": 70},
            "MiFID Investment Inquiry": {"required": 13, "total": 19, "percentage": 70},
            "Card Payment Dispute": {"required": 10, "total": 14, "percentage": 70},
            
            # 50% Follow-up Required
            "Transaction Dispute Report": {"required": 4, "total": 8, "percentage": 50},
            "Export Documentation Error": {"required": 7, "total": 14, "percentage": 50},
            "Account Access Error": {"required": 10, "total": 19, "percentage": 50},
            "IT Access Problem": {"required": 13, "total": 26, "percentage": 50},
            "Digital Platform Malfunction": {"required": 8, "total": 16, "percentage": 50},
            "Transaction Processing Error": {"required": 8, "total": 16, "percentage": 50},
            "SEPA Payment Failure": {"required": 10, "total": 19, "percentage": 50},
            "Credit Line Utilization": {"required": 10, "total": 19, "percentage": 50},
            
            # 30% Follow-up Required
            "Letter Credit Processing": {"required": 2, "total": 7, "percentage": 30},
            "Fee Dispute Email": {"required": 4, "total": 13, "percentage": 30},
            "Compliance Certificate Request": {"required": 3, "total": 10, "percentage": 30},
            "Service Level Breach": {"required": 3, "total": 10, "percentage": 30},
            "Relationship Manager Request": {"required": 5, "total": 17, "percentage": 30},
            "Mobile Authentication Error": {"required": 5, "total": 16, "percentage": 30},
            "Salary Leave Issues": {"required": 7, "total": 23, "percentage": 30}
        }
        
        # First, set all records to follow_up_required: "no"
        print("Setting all records to follow_up_required: 'no'...")
        all_no_result = collection.update_many(
            {},
            {'$set': {'follow_up_required': 'no'}}
        )
        print(f"Updated {all_no_result.modified_count} records to follow_up_required: 'no'")
        
        total_yes_from_topics = 0
        updated_topics = []
        
        # Update specific topics with follow_up_required: "yes"
        print(f"\nUpdating specific topics with follow_up_required: 'yes'...")
        for topic, config in followup_topics.items():
            required_count = config["required"]
            total_count = config["total"]
            percentage = config["percentage"]
            
            # Get all documents with this dominant_topic
            docs_with_topic = list(collection.find(
                {'dominant_topic': topic},
                {'_id': 1}
            ))
            
            available_count = len(docs_with_topic)
            print(f"\nTopic: '{topic}' ({percentage}%)")
            print(f"  Available records: {available_count}")
            print(f"  Required yes records: {required_count}")
            print(f"  Expected total records: {total_count}")
            
            if available_count >= required_count:
                # Randomly select the required number of documents
                random.shuffle(docs_with_topic)
                selected_docs = docs_with_topic[:required_count]
                selected_ids = [doc['_id'] for doc in selected_docs]
                
                # Update selected documents to follow_up_required: "yes"
                result = collection.update_many(
                    {'_id': {'$in': selected_ids}},
                    {'$set': {'follow_up_required': 'yes'}}
                )
                
                print(f"  Updated {result.modified_count} records to follow_up_required: 'yes'")
                total_yes_from_topics += result.modified_count
                updated_topics.append(topic)
            else:
                print(f"  ⚠ Warning: Only {available_count} records available, but {required_count} required")
                if available_count > 0:
                    # Update all available records
                    doc_ids = [doc['_id'] for doc in docs_with_topic]
                    result = collection.update_many(
                        {'_id': {'$in': doc_ids}},
                        {'$set': {'follow_up_required': 'yes'}}
                    )
                    print(f"  Updated all {result.modified_count} available records to follow_up_required: 'yes'")
                    total_yes_from_topics += result.modified_count
                    updated_topics.append(topic)
        
        print(f"\nTotal 'yes' records from specific topics: {total_yes_from_topics}")
        
        # Verification
        print(f"\n=== VERIFICATION ===")
        total_yes = collection.count_documents({'follow_up_required': 'yes'})
        total_no = collection.count_documents({'follow_up_required': 'no'})
        total_docs = collection.count_documents({})
        
        print(f"Total documents: {total_docs}")
        print(f"Documents with follow_up_required: 'yes': {total_yes}")
        print(f"Documents with follow_up_required: 'no': {total_no}")
        
        # Show breakdown by percentage category
        print(f"\nFollow-up required by percentage category:")
        
        # 100% category
        hundred_percent_topics = [topic for topic, config in followup_topics.items() if config["percentage"] == 100]
        hundred_percent_yes = 0
        for topic in hundred_percent_topics:
            yes_count = collection.count_documents({
                'dominant_topic': topic,
                'follow_up_required': 'yes'
            })
            hundred_percent_yes += yes_count
        print(f"  100% category: {hundred_percent_yes} 'yes' records")
        
        # 70% category
        seventy_percent_topics = [topic for topic, config in followup_topics.items() if config["percentage"] == 70]
        seventy_percent_yes = 0
        for topic in seventy_percent_topics:
            yes_count = collection.count_documents({
                'dominant_topic': topic,
                'follow_up_required': 'yes'
            })
            seventy_percent_yes += yes_count
        print(f"  70% category: {seventy_percent_yes} 'yes' records")
        
        # 50% category
        fifty_percent_topics = [topic for topic, config in followup_topics.items() if config["percentage"] == 50]
        fifty_percent_yes = 0
        for topic in fifty_percent_topics:
            yes_count = collection.count_documents({
                'dominant_topic': topic,
                'follow_up_required': 'yes'
            })
            fifty_percent_yes += yes_count
        print(f"  50% category: {fifty_percent_yes} 'yes' records")
        
        # 30% category
        thirty_percent_topics = [topic for topic, config in followup_topics.items() if config["percentage"] == 30]
        thirty_percent_yes = 0
        for topic in thirty_percent_topics:
            yes_count = collection.count_documents({
                'dominant_topic': topic,
                'follow_up_required': 'yes'
            })
            thirty_percent_yes += yes_count
        print(f"  30% category: {thirty_percent_yes} 'yes' records")
        
        # Show sample yes documents
        print(f"\nSample 'yes' documents:")
        sample_docs = collection.find(
            {'follow_up_required': 'yes'},
            {'dominant_topic': 1, 'follow_up_required': 1}
        ).limit(10)
        
        for doc in sample_docs:
            print(f"  {doc['dominant_topic']}: follow_up_required = {doc['follow_up_required']}")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'yes_documents': total_yes,
            'no_documents': total_no,
            'yes_from_topics': total_yes_from_topics,
            'updated_topics': updated_topics
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting follow-up required field update process...\n")
    print("Topics to be marked with follow_up_required: 'yes':")
    print("\n100% Follow-up Required:")
    hundred_percent = [
        "Data Breach Warning", "Cybersecurity Incident Report", "Security Incident Alert",
        "Technology Emergency", "Risk Management Alert", "Sanctions Screening Alert",
        "Covenant Breach Alert", "Executive Escalation Email", "Legal Escalation",
        "Compliance Monitoring Alert", "Quality Assurance Breach", "Customer Service Escalation",
        "Legal Notice Response", "Executive Decision Required", "Approval Required",
        "User Access Approval", "GDPR Access Request", "Right Erasure Demand",
        "Consent Withdrawal Notification", "AML Documentation Request", "KYC Documentation Problem",
        "FATCA Compliance Issue", "Enhanced Due Diligence", "Covenant Compliance Question",
        "Process Exception", "Performance Management", "Vendor Management", "Compliance Issues"
    ]
    for topic in hundred_percent:
        print(f"  {topic}")
    
    print("\n70% Follow-up Required:")
    seventy_percent = [
        "Letter Credit Amendment", "Account Opening Issue", "Processing Delay Complaint",
        "Payment Service Problem", "Clearing House Problem", "Cross-Border Transfer Problem",
        "Regulatory Documentation Need", "CRS Reporting Question", "MiFID Investment Inquiry",
        "Card Payment Dispute"
    ]
    for topic in seventy_percent:
        print(f"  {topic}")
    
    print("\n50% Follow-up Required:")
    fifty_percent = [
        "Transaction Dispute Report", "Export Documentation Error", "Account Access Error",
        "IT Access Problem", "Digital Platform Malfunction", "Transaction Processing Error",
        "SEPA Payment Failure", "Credit Line Utilization"
    ]
    for topic in fifty_percent:
        print(f"  {topic}")
    
    print("\n30% Follow-up Required:")
    thirty_percent = [
        "Letter Credit Processing", "Fee Dispute Email", "Compliance Certificate Request",
        "Service Level Breach", "Relationship Manager Request", "Mobile Authentication Error",
        "Salary Leave Issues"
    ]
    for topic in thirty_percent:
        print(f"  {topic}")
    
    print(f"\nAll other records will be set to follow_up_required: 'no'")
    print()
    
    result = update_followup_field()
    
    if result:
        print(f"\nProcess completed successfully!")
        print(f"Total 'yes' documents: {result['yes_documents']}")
        print(f"Total 'no' documents: {result['no_documents']}")
    else:
        print("\nProcess failed!")