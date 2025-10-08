# Import required libraries
from pymongo import MongoClient
import os
import pandas as pd
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

# Get collection
email_collection = db['email_new']

def get_action_pending_from_stats():
    """
    Get statistics showing percentage distribution of action_pending_from
    across all combinations of urgency, follow_up_required, action_pending_status, priority
    """
    
    print("=" * 80)
    print("ACTION_PENDING_FROM DISTRIBUTION STATISTICS")
    print("=" * 80)
    
    # Fetch all documents with required fields
    print("Fetching data from MongoDB...")
    emails = list(email_collection.find({}, {
        'urgency': 1,
        'follow_up_required': 1,
        'action_pending_status': 1,
        'priority': 1,
        'action_pending_from': 1,
        '_id': 0
    }))
    
    print(f"Total documents fetched: {len(emails)}")
    
    if len(emails) == 0:
        print("No data found in collection")
        return
    
    # Convert to DataFrame
    df = pd.DataFrame(emails)
    
    # Check if DataFrame is empty
    if df.empty:
        print("No data found in collection")
        return
    
    print(f"\nDataFrame shape: {df.shape}")
    print(f"Columns: {df.columns.tolist()}")
    
    # Check for missing action_pending_from field
    if 'action_pending_from' not in df.columns:
        print("❌ action_pending_from field not found in collection")
        return
    
    # Show sample values
    print(f"\nSample action_pending_from values:")
    sample_values = df['action_pending_from'].value_counts().head(10)
    for value, count in sample_values.items():
        print(f"  {value}: {count} records")
    
    # Group by all combinations and count action_pending_from
    print(f"\n{'='*80}")
    print("DETAILED DISTRIBUTION BY COMBINATIONS")
    print(f"{'='*80}")
    
    # Group by all combinations
    grouped = df.groupby(['urgency', 'follow_up_required', 'action_pending_status', 'priority', 'action_pending_from']).size().reset_index(name='count')
    
    # Calculate total records
    total_records = len(df)
    
    # Calculate percentage
    grouped['percentage'] = (grouped['count'] / total_records * 100).round(2)
    
    # Sort by count descending
    grouped = grouped.sort_values('count', ascending=False)
    
    # Display results
    print(f"\n{'Urgency':<8} {'Follow-up':<12} {'Action':<12} {'Priority':<15} {'Action_From':<20} {'Count':<10} {'Percentage':<12}")
    print("-" * 100)
    
    for _, row in grouped.iterrows():
        urgency = str(row['urgency'])
        follow_up = str(row['follow_up_required'])
        action_status = str(row['action_pending_status'])
        priority = str(row['priority'])
        action_from = str(row['action_pending_from'])
        count = row['count']
        percentage = row['percentage']
        
        print(f"{urgency:<8} {follow_up:<12} {action_status:<12} {priority:<15} {action_from:<20} {count:<10} {percentage:.2f}%")
    
    # Summary statistics by action_pending_from
    print(f"\n{'='*80}")
    print("SUMMARY BY ACTION_PENDING_FROM")
    print(f"{'='*80}")
    
    action_from_summary = df['action_pending_from'].value_counts()
    print(f"\nAction Pending From Distribution:")
    for action_from, count in action_from_summary.items():
        percentage = (count / total_records * 100)
        print(f"  {action_from}: {count} records ({percentage:.2f}%)")
    
    # Summary by urgency
    print(f"\n{'='*80}")
    print("SUMMARY BY URGENCY")
    print(f"{'='*80}")
    
    urgency_summary = df.groupby(['urgency', 'action_pending_from']).size().reset_index(name='count')
    urgency_summary['percentage'] = (urgency_summary['count'] / total_records * 100).round(2)
    
    for urgency in sorted(df['urgency'].unique()):
        print(f"\nUrgency: {urgency}")
        urgency_data = urgency_summary[urgency_summary['urgency'] == urgency]
        for _, row in urgency_data.iterrows():
            action_from = str(row['action_pending_from'])
            count = row['count']
            percentage = row['percentage']
            print(f"  {action_from}: {count} records ({percentage:.2f}%)")
    
    # Summary by priority
    print(f"\n{'='*80}")
    print("SUMMARY BY PRIORITY")
    print(f"{'='*80}")
    
    priority_summary = df.groupby(['priority', 'action_pending_from']).size().reset_index(name='count')
    priority_summary['percentage'] = (priority_summary['count'] / total_records * 100).round(2)
    
    for priority in sorted(df['priority'].unique()):
        print(f"\nPriority: {priority}")
        priority_data = priority_summary[priority_summary['priority'] == priority]
        for _, row in priority_data.iterrows():
            action_from = str(row['action_pending_from'])
            count = row['count']
            percentage = row['percentage']
            print(f"  {action_from}: {count} records ({percentage:.2f}%)")
    
    # Export to CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'email_action_pending_from_stats_{timestamp}.csv'
    grouped.to_csv(output_file, index=False)
    print(f"\n✓ Results exported to: {output_file}")
    
    return grouped

def get_field_analysis():
    """
    Analyze field completeness and data quality
    """
    print(f"\n{'='*80}")
    print("FIELD ANALYSIS")
    print(f"{'='*80}")
    
    # Get sample document
    sample_doc = email_collection.find_one()
    if sample_doc:
        print(f"Sample document fields: {list(sample_doc.keys())}")
    
    # Check field completeness
    fields_to_check = ['urgency', 'follow_up_required', 'action_pending_status', 'priority', 'action_pending_from']
    
    print(f"\nField Completeness:")
    for field in fields_to_check:
        count = email_collection.count_documents({field: {'$exists': True, '$ne': None, '$ne': ''}})
        total = email_collection.count_documents({})
        percentage = (count / total * 100) if total > 0 else 0
        print(f"  {field}: {count}/{total} ({percentage:.1f}%)")
    
    # Check for null/empty values
    print(f"\nNull/Empty Value Analysis:")
    for field in fields_to_check:
        null_count = email_collection.count_documents({field: {'$in': [None, '', 'null']}})
        total = email_collection.count_documents({})
        percentage = (null_count / total * 100) if total > 0 else 0
        print(f"  {field} (null/empty): {null_count}/{total} ({percentage:.1f}%)")

if __name__ == "__main__":
    try:
        # Field analysis
        get_field_analysis()
        
        # Main statistics
        stats_df = get_action_pending_from_stats()
        
        print("\n" + "="*80)
        print("ANALYSIS COMPLETED SUCCESSFULLY!")
        print("="*80)
        
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Close connection
        client.close()
        print("\nDone!")