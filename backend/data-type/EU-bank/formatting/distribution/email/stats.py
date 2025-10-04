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

def get_email_stats():
    """Generate comprehensive statistics for email_new collection"""
    
    # Connect to MongoDB
    client = MongoClient(MONGO_CONNECTION_STRING)
    db = client[MONGO_DATABASE_NAME]
    collection = db['email_new']
    
    print("Connected to MongoDB successfully!")
    
    # Get total count
    total_records = collection.count_documents({})
    print(f"Total records: {total_records}")
    
    # Individual field statistics
    urgency_true = collection.count_documents({"urgency": True})
    urgency_false = collection.count_documents({"urgency": False})
    
    follow_up_yes = collection.count_documents({"follow_up_required": "yes"})
    follow_up_no = collection.count_documents({"follow_up_required": "no"})
    
    action_pending_true = collection.count_documents({"action_pending_status": "true"})
    action_pending_false = collection.count_documents({"action_pending_status": "false"})
    
    # Create basic stats DataFrame
    basic_stats = {
        'Metric': [
            'Total Records',
            'Urgency True',
            'Urgency False',
            'Follow Up Required Yes',
            'Follow Up Required No',
            'Action Pending Status True',
            'Action Pending Status False'
        ],
        'Count': [
            total_records,
            urgency_true,
            urgency_false,
            follow_up_yes,
            follow_up_no,
            action_pending_true,
            action_pending_false
        ]
    }
    
    # Generate all possible combinations
    combinations = []
    
    # All possible values for each field
    urgency_values = [True, False]
    follow_up_values = ["yes", "no"]
    action_pending_values = ["true", "false"]
    
    # Generate all combinations
    for urgency in urgency_values:
        for follow_up in follow_up_values:
            for action_pending in action_pending_values:
                count = collection.count_documents({
                    "urgency": urgency,
                    "follow_up_required": follow_up,
                    "action_pending_status": action_pending
                })
                
                combinations.append({
                    'Urgency': urgency,
                    'Follow_Up_Required': follow_up,
                    'Action_Pending_Status': action_pending,
                    'Count': count,
                    'Percentage': round((count / total_records) * 100, 2) if total_records > 0 else 0
                })
    
    # Create DataFrames
    basic_df = pd.DataFrame(basic_stats)
    combinations_df = pd.DataFrame(combinations)
    
    # Save to CSV files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    basic_filename = f"email_basic_stats_{timestamp}.csv"
    combinations_filename = f"email_combinations_stats_{timestamp}.csv"
    
    basic_df.to_csv(basic_filename, index=False)
    combinations_df.to_csv(combinations_filename, index=False)
    
    print(f"\nBasic statistics saved to: {basic_filename}")
    print(f"Combination statistics saved to: {combinations_filename}")
    
    # Display results
    print("\n=== BASIC STATISTICS ===")
    print(basic_df.to_string(index=False))
    
    print("\n=== COMBINATION STATISTICS ===")
    print(combinations_df.to_string(index=False))
    
    # Summary statistics
    print(f"\n=== SUMMARY ===")
    print(f"Total records analyzed: {total_records}")
    print(f"Records with urgency: {urgency_true} ({round((urgency_true/total_records)*100, 2)}%)")
    print(f"Records requiring follow-up: {follow_up_yes} ({round((follow_up_yes/total_records)*100, 2)}%)")
    print(f"Records with pending action: {action_pending_true} ({round((action_pending_true/total_records)*100, 2)}%)")
    
    # Find most common combination
    most_common = combinations_df.loc[combinations_df['Count'].idxmax()]
    print(f"\nMost common combination:")
    print(f"Urgency: {most_common['Urgency']}, Follow-up: {most_common['Follow_Up_Required']}, Action Pending: {most_common['Action_Pending_Status']}")
    print(f"Count: {most_common['Count']} ({most_common['Percentage']}%)")
    
    client.close()
    return basic_df, combinations_df

if __name__ == "__main__":
    try:
        basic_stats, combination_stats = get_email_stats()
        print("\nStats generation completed successfully!")
    except Exception as e:
        print(f"Error occurred: {str(e)}")
