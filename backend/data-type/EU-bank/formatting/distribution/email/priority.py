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

def update_priority_levels():
    """
    Update priority field based on updated priority distribution specifications
    P1-Critical, P2-High, P3-Medium, P4-Low, P5-Very Low
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # First, remove all existing priority fields
        print("Removing all existing priority fields...")
        remove_priority_result = collection.update_many(
            {},
            {'$unset': {'priority': ""}}
        )
        print(f"Removed priority field from {remove_priority_result.modified_count} records")
        
        # Define updated priority topics by level
        priority_topics = {
            # P1 - CRITICAL (301 records - 15%)
            "P1": {
                "Security & Critical Incidents": {
                    "Data Breach Warning": {"total": 15, "p1_count": 15, "percentage": 100},
                    "Cybersecurity Incident Report": {"total": 9, "p1_count": 9, "percentage": 100},
                    "Security Incident Alert": {"total": 8, "p1_count": 8, "percentage": 100},
                    "Technology Emergency": {"total": 19, "p1_count": 8, "percentage": 42}
                },
                "Critical Business Escalations": {
                    "Executive Escalation Email": {"total": 20, "p1_count": 20, "percentage": 100},
                    "Legal Escalation": {"total": 22, "p1_count": 11, "percentage": 50},
                    "Sanctions Screening Alert": {"total": 12, "p1_count": 12, "percentage": 100},
                    "Covenant Breach Alert": {"total": 16, "p1_count": 16, "percentage": 100},
                    "Quality Assurance Breach": {"total": 28, "p1_count": 6, "percentage": 21},
                    "Compliance Monitoring Alert": {"total": 13, "p1_count": 13, "percentage": 100},
                    "Risk Management Alert": {"total": 31, "p1_count": 9, "percentage": 29},
                    "Customer Service Escalation": {"total": 16, "p1_count": 6, "percentage": 38},
                    "Executive Decision Required": {"total": 15, "p1_count": 6, "percentage": 35}
                },
                "Critical Operational Failures": {
                    "System Outage Notification": {"total": 11, "p1_count": 11, "percentage": 100},
                    "Clearing House Problem": {"total": 19, "p1_count": 5, "percentage": 26},
                    "Payment Service Problem": {"total": 15, "p1_count": 8, "percentage": 53},
                    "Processing Delay Complaint": {"total": 16, "p1_count": 7, "percentage": 44},
                    "SEPA Instant Failure": {"total": 12, "p1_count": 8, "percentage": 67},
                    "Wire Transfer Delay": {"total": 10, "p1_count": 8, "percentage": 80},
                    "Legal Notice Response": {"total": 13, "p1_count": 13, "percentage": 100},
                    "GDPR Access Request": {"total": 20, "p1_count": 20, "percentage": 100},
                    "Right Erasure Demand": {"total": 10, "p1_count": 10, "percentage": 100},
                    "Consent Withdrawal Notification": {"total": 20, "p1_count": 20, "percentage": 100},
                    "AML Documentation Request": {"total": 13, "p1_count": 13, "percentage": 100},
                    "KYC Documentation Problem": {"total": 18, "p1_count": 18, "percentage": 100},
                    "FATCA Compliance Issue": {"total": 14, "p1_count": 14, "percentage": 100},
                    "Enhanced Due Diligence": {"total": 8, "p1_count": 8, "percentage": 100}
                }
            },
            
            # P2 - HIGH (401 records - 20%)
            "P2": {
                "Elevated Urgent Issues": {
                    "Technology Emergency": {"total": 19, "p2_count": 11, "percentage": 58},
                    "Risk Management Alert": {"total": 31, "p2_count": 22, "percentage": 71},
                    "Quality Assurance Breach": {"total": 28, "p2_count": 22, "percentage": 79},
                    "Legal Escalation": {"total": 22, "p2_count": 11, "percentage": 50},
                    "Customer Service Escalation": {"total": 16, "p2_count": 10, "percentage": 63},
                    "Executive Decision Required": {"total": 15, "p2_count": 11, "percentage": 65}
                },
                "Critical Approvals & Escalations": {
                    "Approval Required": {"total": 12, "p2_count": 12, "percentage": 100},
                    "User Access Approval": {"total": 30, "p2_count": 24, "percentage": 80},
                    "Process Exception": {"total": 20, "p2_count": 20, "percentage": 100}
                },
                "High-Priority Operations": {
                    "Letter Credit Amendment": {"total": 18, "p2_count": 18, "percentage": 100},
                    "Letter Credit Processing": {"total": 7, "p2_count": 7, "percentage": 100},
                    "Account Opening Issue": {"total": 18, "p2_count": 14, "percentage": 78},
                    "Processing Delay Complaint": {"total": 16, "p2_count": 9, "percentage": 56},
                    "Payment Service Problem": {"total": 15, "p2_count": 7, "percentage": 47},
                    "Clearing House Problem": {"total": 19, "p2_count": 14, "percentage": 74},
                    "Cross-Border Transfer Problem": {"total": 17, "p2_count": 17, "percentage": 100},
                    "Card Payment Dispute": {"total": 14, "p2_count": 14, "percentage": 100},
                    "Regulatory Documentation Need": {"total": 15, "p2_count": 15, "percentage": 100},
                    "Export Documentation Error": {"total": 14, "p2_count": 14, "percentage": 100}
                },
                "Critical Management & Compliance": {
                    "Performance Management": {"total": 22, "p2_count": 18, "percentage": 82},
                    "Vendor Management": {"total": 22, "p2_count": 18, "percentage": 82},
                    "Salary Leave Issues": {"total": 23, "p2_count": 18, "percentage": 78},
                    "Compliance Issues": {"total": 11, "p2_count": 11, "percentage": 100},
                    "IT Access Problem": {"total": 26, "p2_count": 21, "percentage": 81}
                },
                "Payment Operations": {
                    "Digital Platform Malfunction": {"total": 16, "p2_count": 10, "percentage": 63},
                    "Basel III Requirement": {"total": 11, "p2_count": 11, "percentage": 100},
                    "Covenant Compliance Question": {"total": 20, "p2_count": 16, "percentage": 80},
                    "CRS Reporting Question": {"total": 19, "p2_count": 13, "percentage": 68},
                    "SEPA Payment Failure": {"total": 19, "p2_count": 13, "percentage": 68},
                    "Wire Transfer Delay": {"total": 10, "p2_count": 2, "percentage": 20},
                    "Transaction Processing Error": {"total": 16, "p2_count": 11, "percentage": 69},
                    "Direct Debit Returned": {"total": 12, "p2_count": 8, "percentage": 67},
                    "Instant Payment Rejected": {"total": 12, "p2_count": 8, "percentage": 67},
                    "ACH Processing Error": {"total": 13, "p2_count": 9, "percentage": 69},
                    "SEPA Instant Failure": {"total": 12, "p2_count": 4, "percentage": 33}
                }
            },
            
            # P3 - MEDIUM (501 records - 25%)
            "P3": {
                "Standard Transactions & Payments": {
                    "Transaction Processing Error": {"total": 16, "p3_count": 5, "percentage": 31},
                    "Transaction Dispute Report": {"total": 8, "p3_count": 8, "percentage": 100},
                    "SEPA Payment Failure": {"total": 19, "p3_count": 6, "percentage": 32},
                    "SEPA Payment Status": {"total": 14, "p3_count": 14, "percentage": 100},
                    "SEPA Processing Error": {"total": 10, "p3_count": 10, "percentage": 100},
                    "ACH Processing Error": {"total": 13, "p3_count": 4, "percentage": 31},
                    "Check Clearing Issue": {"total": 13, "p3_count": 13, "percentage": 100},
                    "Fund Transfer Problem": {"total": 15, "p3_count": 15, "percentage": 100},
                    "Direct Debit Returned": {"total": 12, "p3_count": 4, "percentage": 33},
                    "Instant Payment Rejected": {"total": 12, "p3_count": 4, "percentage": 33},
                    "Foreign Exchange Problem": {"total": 15, "p3_count": 15, "percentage": 100},
                    "FX Execution Problem": {"total": 8, "p3_count": 8, "percentage": 100},
                    "Best Execution Report": {"total": 13, "p3_count": 13, "percentage": 100},
                    "Fee Dispute Email": {"total": 13, "p3_count": 13, "percentage": 100},
                    "Fee Clarification Need": {"total": 13, "p3_count": 13, "percentage": 100}
                },
                "Account & Access Issues": {
                    "Account Access Error": {"total": 19, "p3_count": 19, "percentage": 100},
                    "Account Opening Issue": {"total": 18, "p3_count": 4, "percentage": 22},
                    "Mobile Authentication Error": {"total": 16, "p3_count": 16, "percentage": 100},
                    "Two-Factor Authentication Error": {"total": 5, "p3_count": 5, "percentage": 100},
                    "Authentication Problem Report": {"total": 11, "p3_count": 11, "percentage": 100},
                    "Strong Customer Authentication": {"total": 15, "p3_count": 15, "percentage": 100},
                    "IT Access Problem": {"total": 26, "p3_count": 5, "percentage": 19},
                    "Digital Platform Malfunction": {"total": 16, "p3_count": 6, "percentage": 38},
                    "User Access Approval": {"total": 30, "p3_count": 6, "percentage": 20},
                    "IT Marketing Unresolved": {"total": 27, "p3_count": 27, "percentage": 100}
                },
                "Banking Services & Products": {
                    "Credit Line Utilization": {"total": 19, "p3_count": 19, "percentage": 100},
                    "Credit Facility Question": {"total": 9, "p3_count": 9, "percentage": 100},
                    "Overdraft Facility Request": {"total": 9, "p3_count": 9, "percentage": 100},
                    "Corporate Loan Inquiry": {"total": 11, "p3_count": 11, "percentage": 100},
                    "Liquidity Facility Problem": {"total": 11, "p3_count": 11, "percentage": 100},
                    "Facility Renewal Inquiry": {"total": 11, "p3_count": 11, "percentage": 100},
                    "MiFID Investment Inquiry": {"total": 19, "p3_count": 19, "percentage": 100},
                    "Investment Service Question": {"total": 8, "p3_count": 8, "percentage": 100},
                    "Investment Advisory Question": {"total": 10, "p3_count": 10, "percentage": 100},
                    "Wealth Management Issue": {"total": 10, "p3_count": 10, "percentage": 100},
                    "Private Banking Service": {"total": 12, "p3_count": 12, "percentage": 100},
                    "Asset Management Issue": {"total": 7, "p3_count": 7, "percentage": 100},
                    "Deposit Service Issue": {"total": 11, "p3_count": 11, "percentage": 100},
                    "Certificate Deposit Question": {"total": 8, "p3_count": 8, "percentage": 100},
                    "Cash Management Issue": {"total": 10, "p3_count": 10, "percentage": 100}
                },
                "Trade Finance & International": {
                    "International Transfer Question": {"total": 15, "p3_count": 15, "percentage": 100},
                    "Cross-Border Regulatory Question": {"total": 7, "p3_count": 7, "percentage": 100},
                    "Trade Finance Delay": {"total": 10, "p3_count": 10, "percentage": 100},
                    "Trade Documentation Error": {"total": 6, "p3_count": 6, "percentage": 100},
                    "Import Finance Question": {"total": 13, "p3_count": 13, "percentage": 100},
                    "Export Credit Issue": {"total": 14, "p3_count": 14, "percentage": 100},
                    "Documentary Collection Issue": {"total": 9, "p3_count": 9, "percentage": 100},
                    "Multi-Currency Statement Need": {"total": 15, "p3_count": 15, "percentage": 100},
                    "CRS Reporting Question": {"total": 19, "p3_count": 6, "percentage": 32},
                    "Covenant Compliance Question": {"total": 20, "p3_count": 4, "percentage": 20}
                },
                "Card & Merchant Services": {
                    "Card Network Issue": {"total": 13, "p3_count": 13, "percentage": 100},
                    "POS Terminal Issue": {"total": 11, "p3_count": 11, "percentage": 100},
                    "Merchant Service Problem": {"total": 13, "p3_count": 13, "percentage": 100},
                    "API Connectivity Issue": {"total": 17, "p3_count": 17, "percentage": 100},
                    "Online Banking Problem": {"total": 15, "p3_count": 15, "percentage": 100},
                    "Mobile App Issue": {"total": 12, "p3_count": 12, "percentage": 100},
                    "Digital Banking Error": {"total": 12, "p3_count": 12, "percentage": 100},
                    "Fintech Integration Error": {"total": 15, "p3_count": 15, "percentage": 100},
                    "Open Banking API": {"total": 12, "p3_count": 12, "percentage": 100},
                    "Integration Problem Report": {"total": 12, "p3_count": 12, "percentage": 100}
                }
            },
            
            # P4 - LOW (501 records - 25%)
            "P4": {
                "Service & Complaints": {
                    "Service Level Breach": {"total": 10, "p4_count": 10, "percentage": 100},
                    "Service Quality Complaint": {"total": 11, "p4_count": 11, "percentage": 100},
                    "Complaint Resolution Request": {"total": 11, "p4_count": 11, "percentage": 100},
                    "Branch Service Complaint": {"total": 6, "p4_count": 6, "percentage": 100},
                    "Phone Banking Issue": {"total": 12, "p4_count": 12, "percentage": 100}
                },
                "General Inquiries": {
                    "Relationship Manager Request": {"total": 17, "p4_count": 17, "percentage": 100},
                    "Interest Rate Query": {"total": 7, "p4_count": 7, "percentage": 100},
                    "Balance Inquiry Problem": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Processing Information Need": {"total": 11, "p4_count": 11, "percentage": 100},
                    "Corporate Account Issue": {"total": 11, "p4_count": 11, "percentage": 100},
                    "Cash Flow Issue": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Derivatives Question Alert": {"total": 14, "p4_count": 14, "percentage": 100},
                    "Bond Trading Problem": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Commercial Paper Issue": {"total": 12, "p4_count": 12, "percentage": 100}
                },
                "Systems & Technology": {
                    "Core Banking System": {"total": 10, "p4_count": 10, "percentage": 100},
                    "Reporting Platform Issue": {"total": 15, "p4_count": 15, "percentage": 100},
                    "Treasury System Error": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Treasury Service Problem": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Monitoring System Alert": {"total": 13, "p4_count": 13, "percentage": 100},
                    "Digital Wallet Issue": {"total": 9, "p4_count": 9, "percentage": 100},
                    "Custody Service Problem": {"total": 14, "p4_count": 14, "percentage": 100}
                },
                "Regulatory & Updates": {
                    "ECB Policy Update": {"total": 20, "p4_count": 20, "percentage": 100},
                    "Client Categorization Update": {"total": 14, "p4_count": 14, "percentage": 100},
                    "Beneficial Ownership Declaration": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Risk Assessment Update": {"total": 10, "p4_count": 10, "percentage": 100},
                    "Legal Advisor Update": {"total": 9, "p4_count": 9, "percentage": 100},
                    "Audit Trail Need": {"total": 11, "p4_count": 11, "percentage": 100},
                    "RegTech Alert": {"total": 14, "p4_count": 14, "percentage": 100},
                    "Third Party Provider": {"total": 14, "p4_count": 14, "percentage": 100},
                    "EU Taxonomy Reporting": {"total": 11, "p4_count": 11, "percentage": 100},
                    "Data Sharing Problem": {"total": 10, "p4_count": 10, "percentage": 100},
                    "Consent Management Dispute": {"total": 12, "p4_count": 12, "percentage": 100},
                    "Compliance Certificate Request": {"total": 10, "p4_count": 10, "percentage": 100},
                    "PSD2 Compliance Question": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Data Retention Question": {"total": 13, "p4_count": 13, "percentage": 100},
                    "Personal Data Inquiry": {"total": 12, "p4_count": 12, "percentage": 100}
                },
                "Network & Settlement": {
                    "ATM Network Problem": {"total": 9, "p4_count": 9, "percentage": 100},
                    "TARGET2 Settlement Issue": {"total": 11, "p4_count": 11, "percentage": 100},
                    "TPP Access Problem": {"total": 11, "p4_count": 11, "percentage": 100},
                    "Euro Clearing Problem": {"total": 8, "p4_count": 8, "percentage": 100},
                    "Beneficiary Bank Unavailable": {"total": 5, "p4_count": 5, "percentage": 100},
                    "Guarantee Execution Problem": {"total": 6, "p4_count": 6, "percentage": 100},
                    "Tax Withholding Problem": {"total": 10, "p4_count": 10, "percentage": 100}
                },
                "Process & HR": {
                    "Process Improvement Request": {"total": 13, "p4_count": 13, "percentage": 100},
                    "Training Need Alert": {"total": 13, "p4_count": 13, "percentage": 100},
                    "Staff Performance Issue": {"total": 10, "p4_count": 10, "percentage": 100},
                    "Performance Management": {"total": 22, "p4_count": 4, "percentage": 18},
                    "Vendor Management": {"total": 22, "p4_count": 4, "percentage": 18},
                    "Salary Leave Issues": {"total": 23, "p4_count": 5, "percentage": 22},
                    "EU Passporting Service": {"total": 9, "p4_count": 9, "percentage": 100},
                    "Audit Firm Communication": {"total": 9, "p4_count": 9, "percentage": 100},
                    "Reporting Discrepancy Alert": {"total": 9, "p4_count": 9, "percentage": 100}
                }
            },
            
            # P5 - VERY LOW (300 records - 15%)
            "P5": {
                "Reporting & Feedback": {
                    "Customer Feedback Report": {"total": 4, "p5_count": 20, "percentage": 100},
                    "Satisfaction Survey Result": {"total": 5, "p5_count": 25, "percentage": 100},
                    "Media Inquiry Email": {"total": 7, "p5_count": 15, "percentage": 100},
                    "Consulting Project Report": {"total": 8, "p5_count": 20, "percentage": 100},
                    "Market Research Report": {"total": 0, "p5_count": 20, "percentage": 100}
                },
                "Internal Communications & Audits": {
                    "Internal Audit": {"total": 11, "p5_count": 30, "percentage": 100},
                    "Quality Assurance Issue": {"total": 12, "p5_count": 25, "percentage": 100},
                    "Internal Newsletter": {"total": 0, "p5_count": 25, "percentage": 100}
                },
                "Informational/Policy Updates": {
                    "Policy Update Notification": {"total": 0, "p5_count": 30, "percentage": 100},
                    "Industry News Digest": {"total": 0, "p5_count": 25, "percentage": 100},
                    "Compliance Training Material": {"total": 0, "p5_count": 20, "percentage": 100},
                    "Best Practices Guide": {"total": 0, "p5_count": 15, "percentage": 100},
                    "Quarterly Performance Report": {"total": 0, "p5_count": 15, "percentage": 100},
                    "Risk Committee Minutes": {"total": 0, "p5_count": 15, "percentage": 100}
                }
            }
        }
        
        total_updated = 0
        priority_counts = {"P1": 0, "P2": 0, "P3": 0, "P4": 0, "P5": 0}
        
        # Update priority for each level
        for priority_level, categories in priority_topics.items():
            print(f"\n=== UPDATING {priority_level} PRIORITY ===")
            
            for category_name, topics in categories.items():
                print(f"\n{category_name}:")
                
                for topic, config in topics.items():
                    priority_count = config[f"{priority_level.lower()}_count"]
                    total_records = config["total"]
                    percentage = config["percentage"]
                    
                    # Get all documents with this dominant_topic
                    docs_with_topic = list(collection.find(
                        {'dominant_topic': topic},
                        {'_id': 1}
                    ))
                    
                    available_count = len(docs_with_topic)
                    print(f"  {topic}: Available={available_count}, Required={priority_count} ({percentage}%)")
                    
                    if available_count >= priority_count:
                        # Randomly select the required number of documents
                        random.shuffle(docs_with_topic)
                        selected_docs = docs_with_topic[:priority_count]
                        selected_ids = [doc['_id'] for doc in selected_docs]
                        
                        # Update selected documents
                        result = collection.update_many(
                            {'_id': {'$in': selected_ids}},
                            {'$set': {'priority': priority_level}}
                        )
                        
                        print(f"    Updated {result.modified_count} records to priority: {priority_level}")
                        total_updated += result.modified_count
                        priority_counts[priority_level] += result.modified_count
                    else:
                        print(f"    ⚠ Warning: Only {available_count} records available, but {priority_count} required")
                        if available_count > 0:
                            # Update all available records
                            doc_ids = [doc['_id'] for doc in docs_with_topic]
                            result = collection.update_many(
                                {'_id': {'$in': doc_ids}},
                                {'$set': {'priority': priority_level}}
                            )
                            print(f"    Updated all {result.modified_count} available records to priority: {priority_level}")
                            total_updated += result.modified_count
                            priority_counts[priority_level] += result.modified_count
        
        print(f"\nTotal records updated: {total_updated}")
        print(f"Priority distribution:")
        for level, count in priority_counts.items():
            print(f"  {level}: {count} records")
        
        # Handle remaining null priority records - assign all to P5
        print(f"\n=== ASSIGNING REMAINING NULL PRIORITY RECORDS TO P5 ===")
        null_priority_docs = list(collection.find(
            {'priority': {'$exists': False}},
            {'_id': 1}
        ))
        
        null_count = len(null_priority_docs)
        print(f"Found {null_count} records without priority field")
        
        if null_count > 0:
            # Assign all remaining records to P5 priority
            doc_ids = [doc['_id'] for doc in null_priority_docs]
            
            result = collection.update_many(
                {'_id': {'$in': doc_ids}},
                {'$set': {'priority': 'P5'}}
            )
            
            print(f"Updated all {result.modified_count} remaining records to priority: P5")
            priority_counts["P5"] += result.modified_count
        
        print(f"\nFinal priority distribution:")
        for level, count in priority_counts.items():
            print(f"  {level}: {count} records")
        
        # Verification
        print(f"\n=== VERIFICATION ===")
        total_docs = collection.count_documents({})
        docs_with_priority = collection.count_documents({'priority': {'$exists': True}})
        docs_without_priority = collection.count_documents({'priority': {'$exists': False}})
        
        print(f"Total documents: {total_docs}")
        print(f"Documents with priority field: {docs_with_priority}")
        print(f"Documents without priority field: {docs_without_priority}")
        
        # Find documents without dominant_topic or with unmatched topics
        docs_without_topic = collection.count_documents({'dominant_topic': {'$exists': False}})
        docs_with_null_topic = collection.count_documents({'dominant_topic': None})
        docs_with_empty_topic = collection.count_documents({'dominant_topic': ''})
        
        print(f"\nDocuments without dominant_topic field: {docs_without_topic}")
        print(f"Documents with null dominant_topic: {docs_with_null_topic}")
        print(f"Documents with empty dominant_topic: {docs_with_empty_topic}")
        
        # Show documents that don't have priority (if any)
        if docs_without_priority > 0:
            print(f"\nDocuments without priority:")
            unmatched_docs = collection.find(
                {'priority': {'$exists': False}},
                {'dominant_topic': 1, '_id': 1}
            ).limit(10)
            
            for doc in unmatched_docs:
                print(f"  ID: {doc['_id']}, dominant_topic: {doc.get('dominant_topic', 'MISSING')}")
        
        # Show documents without dominant_topic (if any)
        if docs_without_topic > 0 or docs_with_null_topic > 0 or docs_with_empty_topic > 0:
            print(f"\nDocuments without valid dominant_topic:")
            invalid_topic_docs = collection.find(
                {'$or': [
                    {'dominant_topic': {'$exists': False}},
                    {'dominant_topic': None},
                    {'dominant_topic': ''}
                ]},
                {'dominant_topic': 1, '_id': 1}
            ).limit(10)
            
            for doc in invalid_topic_docs:
                print(f"  ID: {doc['_id']}, dominant_topic: {doc.get('dominant_topic', 'MISSING')}")
        
        for level in ["P1", "P2", "P3", "P4", "P5"]:
            count = collection.count_documents({'priority': level})
            print(f"Documents with priority: {level}: {count}")
        
        # Show sample documents by priority
        print(f"\nSample documents by priority:")
        for level in ["P1", "P2", "P3", "P4", "P5"]:
            sample_docs = collection.find(
                {'priority': level},
                {'dominant_topic': 1, 'priority': 1}
            ).limit(3)
            
            docs_list = list(sample_docs)
            if docs_list:
                print(f"\n{level} samples:")
                for doc in docs_list:
                    print(f"  {doc['dominant_topic']}: priority = {doc['priority']}")
        
        # Close MongoDB connection
        client.close()
        
        return {
            'total_documents': total_docs,
            'total_updated': total_updated,
            'priority_counts': priority_counts,
            'docs_without_priority': docs_without_priority
        }
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting updated priority level distribution process...\n")
    
    # Display updated priority structure
    priority_structure = {
        "P1": "CRITICAL - Business Stop, Must Resolve Now (301 records - 15%)",
        "P2": "HIGH - Major Issue, Needs Fast Action (401 records - 20%)",
        "P3": "MEDIUM - Standard Issues, Manageable Timelines (501 records - 25%)",
        "P4": "LOW - Minor Issues, No Major Business Impact (501 records - 25%)",
        "P5": "VERY LOW - Informational, FYI, Archival (300 records - 15%)"
    }
    
    for level, description in priority_structure.items():
        print(f"{level}: {description}")
    
    print(f"\nProcess:")
    print("1. Remove all existing priority fields")
    print("2. Update records based on topic-specific priority assignments")
    print("3. Randomly distribute remaining records without priority field")
    print()
    
    result = update_priority_levels()
    
    if result:
        print(f"\nProcess completed successfully!")
        print(f"Total documents: {result['total_documents']}")
        print(f"Total updated: {result['total_updated']}")
        print(f"Priority distribution:")
        for level, count in result['priority_counts'].items():
            print(f"  {level}: {count} records")
        print(f"Documents without priority: {result['docs_without_priority']} records")
    else:
        print("\nProcess failed!")