# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Get collection
voice_collection = db['voice_new']

# EXTERNAL CALLS (Customer ↔ Agent): ~92%
EXTERNAL_TOPICS = [
    # Transactional & Account Services (35%)
    "Balance Inquiry Call",
    "Payment Problem Discussion",
    "Transfer Instruction Call",
    "Transaction History Inquiry",
    "Payment Authorization Request",
    "Payment Processing Inquiry",
    "Payment Delay Complaint",
    "Payment Cancellation Request",
    "Account Statement Request",
    "Refund Status Request",
    "Standing Order Setup",
    "Direct Debit Setup",
    
    # Account Management (18%)
    "Account Opening Process",
    "Account Update Call",
    "Account Closure Request",
    "Profile Update Request",
    "Account Verification Call",
    "Account Security Review",
    "Multi Currency Account",
    "Signatory Change Request",
    "Business Account Setup",
    
    # Technical Support & Access (12%)
    "System Access Help",
    "Technical Support Request",
    "Digital Banking Help",
    "Mobile App Troubleshooting",
    "Password Reset Assistance",
    "Authentication Assistance Call",
    "Platform Navigation Help",
    "Card Activation Call",
    "Electronic Banking Training",
    
    # Product & Service Inquiries (10%)
    "Product Information Inquiry",
    "Card Service Request",
    "Fee Structure Inquiry",
    "Fee Dispute Discussion",
    "Service Availability Question",
    "Banking Hours Inquiry",
    "Investment Product Inquiry",
    "Currency Exchange Inquiry",
    "Interest Rate Inquiry",
    
    # Complaints & Escalations (8%)
    "Complaint Escalation Request",
    "Service Quality Complaint",
    "Complaint Follow Up",
    "Escalation Management Request",
    "Service Feedback Call",
    
    # Specialized Banking Services (9%)
    "Trade Finance Consultation",
    "International Transfer Call",
    "Credit Facility Inquiry",
    "Credit Line Request",
    "Treasury Service Consultation",
    "Cash Management Consultation",
    "Corporate Card Request",
    "Payroll Service Discussion",
    "Letter Credit Discussion",
    "Bank Guarantee Inquiry",
    "Documentary Collection Call",
    "Invoice Financing Request",
    "Export Finance Consultation",
    "Trade Settlement Inquiry",
    "Forex Trading Call",
    "Working Capital Discussion",
    "Investment Advisory Call",
    "Portfolio Review Call",
    "Financial Planning Consultation",
    "Liquidity Management Call",
    "KYC Verification Call"
]

# INTERNAL CALLS (Employee ↔ Employee): ~8%
INTERNAL_TOPICS = [
    # Compliance & Risk Management (3%)
    "AML Inquiry Discussion",
    "Enhanced Due Diligence",
    "Suspicious Activity Report",
    "Transaction Monitoring Call",
    "Sanctions Screening Call",
    "Beneficial Ownership Discussion",
    "Source Funds Verification",
    "Regulatory Reporting Assistance",
    "Audit Preparation Call",
    "Operational Risk Assessment",
    "Credit Risk Evaluation",
    "Settlement Risk Analysis",
    
    # System & Operations (2%)
    "System Error Discussion",
    "System Maintenance Notice",
    "System Upgrade Notice",
    "IT Infrastructure Support",
    "Data Backup Discussion",
    "Business Continuity Planning",
    "Disaster Recovery Discussion",
    "API Integration Support",
    "ERP System Connection",
    "Batch Processing Assistance",
    
    # Internal Coordination & Training (2%)
    "Relationship Manager Introduction",
    "Governance Review Meeting",
    "Internal Control Discussion",
    "Strategic Planning Consultation",
    "Training Session Scheduling",
    "Compliance Training Call",
    "Workshop Registration Call",
    "Account Migration Notice",
    "Customer Retention Call",
    "Cross Selling Conversation",
    
    # Fraud & Security (1%)
    "Fraud Alert Call",
    "Security Breach Report",
    "Fraud Prevention Discussion",
    "Security Warning Call",
    "Cybersecurity Consultation Call"
]

def add_category_field():
    """
    Add category field to voice_new documents based on dominant_topic:
    - External: Customer ↔ Agent calls (~92%)
    - Internal: Employee ↔ Employee calls (~8%)
    """
    
    print("=" * 80)
    print("ADDING CATEGORY FIELD TO VOICE DOCUMENTS BASED ON DOMINANT_TOPIC")
    print("=" * 80)
    
    # Check if documents already have category field
    sample = voice_collection.find_one({'category': {'$exists': True}})
    if sample:
        print("\n⚠ Documents already have 'category' field.")
        print("Re-assigning categories based on dominant_topic...")
    
    # Get all documents
    total_docs = voice_collection.count_documents({})
    print(f"\nTotal documents in voice_new collection: {total_docs}")
    
    if total_docs == 0:
        print("No documents found in collection!")
        return
    
    # Get all unique dominant topics to verify coverage
    print("\nFetching all unique dominant topics...")
    unique_topics = voice_collection.distinct('dominant_topic')
    print(f"Found {len(unique_topics)} unique dominant topics")
    
    # Create sets for faster lookup
    external_topics_set = set(EXTERNAL_TOPICS)
    internal_topics_set = set(INTERNAL_TOPICS)
    
    # Track statistics
    external_updated = 0
    internal_updated = 0
    unmatched_topics = set()
    topic_counts = {}
    
    # Process all documents
    print("\nAnalyzing dominant topics and preparing bulk updates...")
    all_docs = list(voice_collection.find({}, {'_id': 1, 'dominant_topic': 1}))
    
    # Prepare bulk operations
    from pymongo import UpdateOne
    bulk_operations = []
    
    for i, doc in enumerate(all_docs):
        dominant_topic = doc.get('dominant_topic', '').strip()
        
        # Track topic counts
        if dominant_topic:
            topic_counts[dominant_topic] = topic_counts.get(dominant_topic, 0) + 1
        
        # Determine category based on dominant_topic
        if dominant_topic in external_topics_set:
            category = 'External'
            external_updated += 1
        elif dominant_topic in internal_topics_set:
            category = 'Internal'
            internal_updated += 1
        else:
            # Default to External if not found (since External is majority)
            category = 'External'
            external_updated += 1
            unmatched_topics.add(dominant_topic)
        
        # Add to bulk operations
        bulk_operations.append(
            UpdateOne(
                {'_id': doc['_id']},
                {'$set': {'category': category}}
            )
        )
    
    # Execute bulk update
    print(f"Analyzed {len(all_docs)} documents. Executing bulk update...")
    if bulk_operations:
        result = voice_collection.bulk_write(bulk_operations, ordered=False)
        print(f"Bulk update completed: {result.modified_count} documents updated.")
    
    # Verification
    print("\n" + "=" * 80)
    print("VERIFICATION - CATEGORY DISTRIBUTION")
    print("=" * 80)
    
    external_count = voice_collection.count_documents({'category': 'External'})
    internal_count = voice_collection.count_documents({'category': 'Internal'})
    
    print(f"\n{'Category':<15} {'Count':<10} {'Percentage':<12}")
    print("-" * 40)
    print(f"{'External':<15} {external_count:<10} {(external_count/total_docs*100):.2f}%")
    print(f"{'Internal':<15} {internal_count:<10} {(internal_count/total_docs*100):.2f}%")
    print("-" * 40)
    print(f"{'Total':<15} {external_count + internal_count:<10} {((external_count + internal_count)/total_docs*100):.2f}%")
    
    # Report unmatched topics
    if unmatched_topics:
        print("\n" + "=" * 80)
        print("UNMATCHED DOMINANT TOPICS (Assigned to External by default)")
        print("=" * 80)
        print(f"\nFound {len(unmatched_topics)} unmatched topic(s):")
        for topic in sorted(unmatched_topics):
            count = topic_counts.get(topic, 0)
            print(f"  - '{topic}' ({count} documents)")
    
    # Export topic distribution to JSON
    print("\n" + "=" * 80)
    topic_distribution = {
        'total_documents': total_docs,
        'external_count': external_count,
        'internal_count': internal_count,
        'external_percentage': round(external_count/total_docs*100, 2),
        'internal_percentage': round(internal_count/total_docs*100, 2),
        'topic_counts': dict(sorted(topic_counts.items(), key=lambda x: x[1], reverse=True)),
        'unmatched_topics': list(unmatched_topics)
    }
    
    json_filename = f"voice_category_distribution.json"
    with open(json_filename, 'w') as f:
        json.dump(topic_distribution, f, indent=2)
    print(f"Topic distribution exported to: {json_filename}")
    print("=" * 80)
    
    print("\n✓ Category field added successfully based on dominant_topic!")

if __name__ == "__main__":
    try:
        add_category_field()
        print("\nOperation completed successfully!")
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Close the connection
        client.close()

