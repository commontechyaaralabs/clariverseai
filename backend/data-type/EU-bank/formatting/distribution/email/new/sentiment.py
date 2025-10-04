# Import required libraries
from pymongo import MongoClient, UpdateOne
import os
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')
COLLECTION_NAME = "email_new"

# Priority to sentiment mapping
PRIORITY_SENTIMENT_MAP = {
    "P1-Critical": 5,
    "P2-High": 4,
    "P3-Medium": 3,
    "P4-Low": 2,
    "P5-Very Low": 1
}

def connect_to_database():
    """Connect to MongoDB database"""
    try:
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        
        # Test connection
        client.admin.command('ping')
        print(f"✅ Connected to MongoDB: {MONGO_DATABASE_NAME}.{COLLECTION_NAME}")
        return client, db, collection
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return None, None, None

def remove_existing_overall_sentiment(collection):
    """Remove existing overall_sentiment field from all records"""
    print("\n🔄 Step 1: Removing existing overall_sentiment fields...")
    
    # Check how many records have the overall_sentiment field
    records_with_overall_sentiment = collection.count_documents({"overall_sentiment": {"$exists": True}})
    
    if records_with_overall_sentiment == 0:
        print("✅ No existing overall_sentiment fields found")
        return
    
    print(f"📊 Found {records_with_overall_sentiment} records with existing overall_sentiment field")
    
    # Remove overall_sentiment field from all records
    try:
        result = collection.update_many(
            {"overall_sentiment": {"$exists": True}},
            {"$unset": {"overall_sentiment": ""}}
        )
        
        print(f"✅ Successfully removed overall_sentiment field from {result.modified_count} records")
        
        # Verify removal
        remaining_overall_sentiment = collection.count_documents({"overall_sentiment": {"$exists": True}})
        if remaining_overall_sentiment == 0:
            print("✅ All existing overall_sentiment fields have been successfully removed!")
        else:
            print(f"⚠️ {remaining_overall_sentiment} records still have overall_sentiment field")
            
    except Exception as e:
        print(f"❌ Error removing overall_sentiment field: {e}")

def update_sentiment_based_on_priority(collection):
    """Update overall_sentiment based on priority field"""
    print("\n🔄 Step 2: Adding overall_sentiment based on priority...")
    
    # Get all records that have priority field
    query = {"priority": {"$exists": True, "$ne": None, "$ne": ""}}
    total_records = collection.count_documents(query)
    print(f"📊 Total records with priority field: {total_records}")
    
    if total_records == 0:
        print("⚠️ No records found with priority field")
        return
    
    # Process records in batches
    batch_size = 1000
    updated_count = 0
    skipped_count = 0
    error_count = 0
    
    for i in range(0, total_records, batch_size):
        batch_records = list(collection.find(query).skip(i).limit(batch_size))
        
        if not batch_records:
            break
            
        print(f"📦 Processing batch {i//batch_size + 1}: records {i+1}-{min(i+batch_size, total_records)}")
        
        bulk_operations = []
        
        for record in batch_records:
            try:
                priority = record.get('priority', '').strip()
                record_id = record.get('_id')
                
                if priority in PRIORITY_SENTIMENT_MAP:
                    new_sentiment = PRIORITY_SENTIMENT_MAP[priority]
                    
                    # Update the record with new overall_sentiment
                    bulk_operations.append(
                        UpdateOne(
                            {'_id': record_id},
                            {'$set': {'overall_sentiment': new_sentiment}}
                        )
                    )
                    updated_count += 1
                else:
                    print(f"⚠️ Unknown priority '{priority}' in record {record_id}")
                    skipped_count += 1
                    
            except Exception as e:
                print(f"❌ Error processing record {record.get('_id', 'unknown')}: {e}")
                error_count += 1
        
        # Execute bulk operations
        if bulk_operations:
            try:
                result = collection.bulk_write(bulk_operations, ordered=False)
                print(f"✅ Batch updated: {result.modified_count} records")
            except Exception as e:
                print(f"❌ Batch update failed: {e}")
                error_count += len(bulk_operations)
    
    print(f"\n📈 Update Summary:")
    print(f"  ✅ Successfully updated: {updated_count} records")
    print(f"  ⚠️ Skipped (unknown priority): {skipped_count} records")
    print(f"  ❌ Errors: {error_count} records")

def get_sentiment_statistics(collection):
    """Get statistics about sentiment distribution"""
    print("\n📊 Sentiment Statistics:")
    
    # Total records
    total_records = collection.count_documents({})
    print(f"📋 Total records in collection: {total_records}")
    
    # Records with sentiment
    records_with_sentiment = collection.count_documents({"overall_sentiment": {"$exists": True, "$ne": None}})
    print(f"📊 Records with overall_sentiment: {records_with_sentiment}")
    
    # Sentiment distribution
    pipeline = [
        {"$match": {"overall_sentiment": {"$exists": True, "$ne": None}}},
        {"$group": {"_id": "$overall_sentiment", "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}}
    ]
    
    sentiment_dist = list(collection.aggregate(pipeline))
    
    print(f"\n🎯 Sentiment Distribution:")
    for item in sentiment_dist:
        sentiment_value = item['_id']
        count = item['count']
        percentage = (count / records_with_sentiment * 100) if records_with_sentiment > 0 else 0
        
        # Map sentiment value to description
        sentiment_desc = {
            5: "P1-Critical (Very Negative)",
            4: "P2-High (Negative)",
            3: "P3-Medium (Neutral)",
            2: "P4-Low (Positive)",
            1: "P5-Very Low (Very Positive)"
        }.get(sentiment_value, f"Unknown ({sentiment_value})")
        
        print(f"  {sentiment_desc}: {count} records ({percentage:.1f}%)")
    
    # Priority distribution
    print(f"\n🏷️ Priority Distribution:")
    priority_pipeline = [
        {"$match": {"priority": {"$exists": True, "$ne": None, "$ne": ""}}},
        {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    
    priority_dist = list(collection.aggregate(priority_pipeline))
    
    for item in priority_dist:
        priority = item['_id']
        count = item['count']
        percentage = (count / total_records * 100) if total_records > 0 else 0
        print(f"  {priority}: {count} records ({percentage:.1f}%)")
    
    # Records without sentiment
    records_without_sentiment = total_records - records_with_sentiment
    if records_without_sentiment > 0:
        print(f"\n⚠️ Records without overall_sentiment: {records_without_sentiment}")
    
    # Completion rate
    completion_rate = (records_with_sentiment / total_records * 100) if total_records > 0 else 0
    print(f"\n✅ Sentiment completion rate: {completion_rate:.1f}%")

def main():
    """Main function"""
    print("🚀 Starting Overall Sentiment Update Process")
    print("=" * 60)
    print("Priority to Sentiment Mapping:")
    for priority, sentiment in PRIORITY_SENTIMENT_MAP.items():
        print(f"  {priority} → overall_sentiment: {sentiment}")
    print("=" * 60)
    
    # Connect to database
    client, db, collection = connect_to_database()
    if collection is None:
        return
    
    try:
        # Show initial statistics
        print("\n📊 Initial Statistics:")
        get_sentiment_statistics(collection)
        
        # Step 1: Remove existing overall_sentiment fields
        remove_existing_overall_sentiment(collection)
        
        # Step 2: Add new overall_sentiment based on priority
        update_sentiment_based_on_priority(collection)
        
        # Show final statistics
        print("\n" + "=" * 60)
        print("📊 Final Statistics:")
        get_sentiment_statistics(collection)
        
        print("\n✅ Process completed successfully!")
        
    except Exception as e:
        print(f"❌ Process failed: {e}")
    finally:
        if client:
            client.close()
            print("🔌 Database connection closed")

if __name__ == "__main__":
    main()