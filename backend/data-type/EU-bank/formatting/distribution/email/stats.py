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

def get_email_stats():
    """
    Get statistics for all combinations of urgency, follow_up_required, and category
    """
    
    print("=" * 80)
    print("EMAIL STATISTICS - URGENCY, FOLLOW-UP, AND CATEGORY ANALYSIS")
    print("=" * 80)
    
    # Get total count
    total_docs = email_collection.count_documents({})
    print(f"\nTotal documents in email_new collection: {total_docs}")
    
    if total_docs == 0:
        print("No documents found in collection!")
        return
    
    print("\n" + "=" * 80)
    print("DETAILED STATISTICS BY COMBINATION")
    print("=" * 80)
    
    # Define all possible combinations
    urgency_values = [True, False]
    follow_up_values = ["yes", "no"]
    category_values = ["External", "Internal"]
    
    # Store results for summary table
    results = []
    
    # Query each combination
    for urgency in urgency_values:
        for follow_up in follow_up_values:
            for category in category_values:
                query = {
                    'urgency': urgency,
                    'follow_up_required': follow_up,
                    'category': category
                }
                
                count = email_collection.count_documents(query)
                percentage = (count / total_docs * 100) if total_docs > 0 else 0
                
                results.append({
                    'urgency': urgency,
                    'follow_up': follow_up,
                    'category': category,
                    'count': count,
                    'percentage': percentage
                })
                
                # Print individual result
                print(f"\nUrgency: {str(urgency):5} | Follow-up: {follow_up:3} | Category: {category:8}")
                print(f"  Count: {count:5} | Percentage: {percentage:6.2f}%")
                print(f"  Query: {query}")
    
    print("\n" + "=" * 80)
    print("SUMMARY TABLE")
    print("=" * 80)
    
    # Create DataFrame for better visualization
    df = pd.DataFrame(results)
    df['urgency_str'] = df['urgency'].apply(lambda x: str(x))
    
    # Print formatted table
    print(f"\n{'Urgency':<10} {'Follow-up':<12} {'Category':<12} {'Count':<10} {'Percentage':<12}")
    print("-" * 80)
    
    for _, row in df.iterrows():
        print(f"{row['urgency_str']:<10} {row['follow_up']:<12} {row['category']:<12} {row['count']:<10} {row['percentage']:.2f}%")
    
    # Additional aggregated statistics
    print("\n" + "=" * 80)
    print("AGGREGATED STATISTICS")
    print("=" * 80)
    
    # By Urgency
    print("\n--- By Urgency ---")
    urgent_count = email_collection.count_documents({'urgency': True})
    not_urgent_count = email_collection.count_documents({'urgency': False})
    print(f"Urgent (True):      {urgent_count:5} ({(urgent_count/total_docs*100):.2f}%)")
    print(f"Not Urgent (False): {not_urgent_count:5} ({(not_urgent_count/total_docs*100):.2f}%)")
    
    # By Follow-up Required
    print("\n--- By Follow-up Required ---")
    follow_up_yes = email_collection.count_documents({'follow_up_required': 'yes'})
    follow_up_no = email_collection.count_documents({'follow_up_required': 'no'})
    print(f"Follow-up Yes: {follow_up_yes:5} ({(follow_up_yes/total_docs*100):.2f}%)")
    print(f"Follow-up No:  {follow_up_no:5} ({(follow_up_no/total_docs*100):.2f}%)")
    
    # By Category
    print("\n--- By Category ---")
    external_count = email_collection.count_documents({'category': 'External'})
    internal_count = email_collection.count_documents({'category': 'Internal'})
    print(f"External: {external_count:5} ({(external_count/total_docs*100):.2f}%)")
    print(f"Internal: {internal_count:5} ({(internal_count/total_docs*100):.2f}%)")
    
    # Cross-tabulation: Urgency vs Category
    print("\n" + "=" * 80)
    print("CROSS-TABULATION: URGENCY vs CATEGORY")
    print("=" * 80)
    
    urgent_external = email_collection.count_documents({'urgency': True, 'category': 'External'})
    urgent_internal = email_collection.count_documents({'urgency': True, 'category': 'Internal'})
    not_urgent_external = email_collection.count_documents({'urgency': False, 'category': 'External'})
    not_urgent_internal = email_collection.count_documents({'urgency': False, 'category': 'Internal'})
    
    print(f"\n{'':15} {'External':<15} {'Internal':<15}")
    print("-" * 50)
    print(f"{'True':<15} {urgent_external:<5} ({(urgent_external/total_docs*100):5.2f}%)  {urgent_internal:<5} ({(urgent_internal/total_docs*100):5.2f}%)")
    print(f"{'False':<15} {not_urgent_external:<5} ({(not_urgent_external/total_docs*100):5.2f}%)  {not_urgent_internal:<5} ({(not_urgent_internal/total_docs*100):5.2f}%)")
    
    # Cross-tabulation: Follow-up vs Category
    print("\n" + "=" * 80)
    print("CROSS-TABULATION: FOLLOW-UP vs CATEGORY")
    print("=" * 80)
    
    follow_up_yes_external = email_collection.count_documents({'follow_up_required': 'yes', 'category': 'External'})
    follow_up_yes_internal = email_collection.count_documents({'follow_up_required': 'yes', 'category': 'Internal'})
    follow_up_no_external = email_collection.count_documents({'follow_up_required': 'no', 'category': 'External'})
    follow_up_no_internal = email_collection.count_documents({'follow_up_required': 'no', 'category': 'Internal'})
    
    print(f"\n{'':15} {'External':<15} {'Internal':<15}")
    print("-" * 50)
    print(f"{'Follow-up Yes':<15} {follow_up_yes_external:<5} ({(follow_up_yes_external/total_docs*100):5.2f}%)  {follow_up_yes_internal:<5} ({(follow_up_yes_internal/total_docs*100):5.2f}%)")
    print(f"{'Follow-up No':<15} {follow_up_no_external:<5} ({(follow_up_no_external/total_docs*100):5.2f}%)  {follow_up_no_internal:<5} ({(follow_up_no_internal/total_docs*100):5.2f}%)")
    
    # Cross-tabulation: Urgency vs Follow-up
    print("\n" + "=" * 80)
    print("CROSS-TABULATION: URGENCY vs FOLLOW-UP")
    print("=" * 80)
    
    urgent_followup_yes = email_collection.count_documents({'urgency': True, 'follow_up_required': 'yes'})
    urgent_followup_no = email_collection.count_documents({'urgency': True, 'follow_up_required': 'no'})
    not_urgent_followup_yes = email_collection.count_documents({'urgency': False, 'follow_up_required': 'yes'})
    not_urgent_followup_no = email_collection.count_documents({'urgency': False, 'follow_up_required': 'no'})
    
    print(f"\n{'':15} {'Follow-up Yes':<15} {'Follow-up No':<15}")
    print("-" * 50)
    print(f"{'True':<15} {urgent_followup_yes:<5} ({(urgent_followup_yes/total_docs*100):5.2f}%)  {urgent_followup_no:<5} ({(urgent_followup_no/total_docs*100):5.2f}%)")
    print(f"{'False':<15} {not_urgent_followup_yes:<5} ({(not_urgent_followup_yes/total_docs*100):5.2f}%)  {not_urgent_followup_no:<5} ({(not_urgent_followup_no/total_docs*100):5.2f}%)")
    
    # Export to CSV (optional)
    print("\n" + "=" * 80)
    csv_filename = f"email_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    df_export = df[['urgency_str', 'follow_up', 'category', 'count', 'percentage']]
    df_export.columns = ['Urgency', 'Follow_up_Required', 'Category', 'Count', 'Percentage']
    df_export.to_csv(csv_filename, index=False)
    print(f"Statistics exported to: {csv_filename}")
    print("=" * 80)

if __name__ == "__main__":
    try:
        get_email_stats()
        print("\nOperation completed successfully!")
    except Exception as e:
        print(f"\nError occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # Close the connection
        client.close()