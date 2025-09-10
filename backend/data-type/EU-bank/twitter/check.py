# Tweet ID Uniqueness Fixer
import os
import random
from pymongo import MongoClient
from collections import defaultdict
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")
COLLECTION_NAME = "twitter"  # Change this to your actual collection name

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
tweets_col = db[COLLECTION_NAME]

def generate_unique_tweet_id():
    """Generate a unique tweet ID in format T#### (T + 4 digits)"""
    return f"T{random.randint(1000, 9999)}"

def get_all_existing_tweet_ids():
    """Get all existing tweet_id values from the collection"""
    pipeline = [
        {"$group": {"_id": "$tweet_id", "count": {"$sum": 1}}},
        {"$project": {"tweet_id": "$_id", "count": 1, "_id": 0}}
    ]
    
    result = list(tweets_col.aggregate(pipeline))
    return {item["tweet_id"]: item["count"] for item in result}

def find_duplicate_tweet_ids():
    """Find all duplicate tweet_id values"""
    tweet_id_counts = get_all_existing_tweet_ids()
    duplicates = {tweet_id: count for tweet_id, count in tweet_id_counts.items() if count > 1}
    
    print(f"📊 Total unique tweet_ids: {len(tweet_id_counts)}")
    print(f"🔍 Found {len(duplicates)} duplicate tweet_ids:")
    
    for tweet_id, count in duplicates.items():
        print(f"   {tweet_id}: appears {count} times")
    
    return duplicates

def generate_new_unique_tweet_id(existing_ids):
    """Generate a new tweet_id that doesn't exist in the collection"""
    max_attempts = 1000
    attempts = 0
    
    while attempts < max_attempts:
        new_id = generate_unique_tweet_id()
        if new_id not in existing_ids:
            return new_id
        attempts += 1
    
    # Fallback: use timestamp-based ID if random generation fails
    import time
    timestamp_id = f"T{int(time.time()) % 10000}"
    return timestamp_id

def fix_duplicate_tweet_ids():
    """Fix all duplicate tweet_id values by updating duplicates with unique IDs"""
    
    print("🔄 Starting tweet_id uniqueness fix...")
    print("=" * 60)
    
    # Get current state
    duplicates = find_duplicate_tweet_ids()
    
    if not duplicates:
        print("✅ No duplicate tweet_ids found. All tweet_ids are already unique!")
        return
    
    # Get all existing tweet_ids for uniqueness check
    all_existing_ids = set(get_all_existing_tweet_ids().keys())
    
    total_updates = 0
    
    for duplicate_tweet_id, count in duplicates.items():
        print(f"\n🔧 Fixing duplicate tweet_id: {duplicate_tweet_id} ({count} occurrences)")
        
        # Find all documents with this duplicate tweet_id
        duplicate_docs = list(tweets_col.find({"tweet_id": duplicate_tweet_id}))
        
        # Keep the first occurrence, update the rest
        docs_to_update = duplicate_docs[1:]  # Skip first document
        
        print(f"   📝 Keeping first occurrence, updating {len(docs_to_update)} duplicates...")
        
        for i, doc in enumerate(docs_to_update, 1):
            # Generate new unique tweet_id
            new_tweet_id = generate_new_unique_tweet_id(all_existing_ids)
            all_existing_ids.add(new_tweet_id)  # Add to set to avoid future duplicates
            
            # Update the document
            try:
                result = tweets_col.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"tweet_id": new_tweet_id}}
                )
                
                if result.modified_count > 0:
                    total_updates += 1
                    print(f"   ✅ Updated document {i}/{len(docs_to_update)}: {duplicate_tweet_id} → {new_tweet_id}")
                else:
                    print(f"   ❌ Failed to update document {i}/{len(docs_to_update)}")
                    
            except Exception as e:
                print(f"   ❌ Error updating document {i}: {e}")
    
    print(f"\n🎯 Fix Complete!")
    print(f"📊 Total documents updated: {total_updates}")
    
    return total_updates

def verify_uniqueness():
    """Verify that all tweet_ids are now unique"""
    print("\n🔍 Verifying tweet_id uniqueness...")
    
    tweet_id_counts = get_all_existing_tweet_ids()
    duplicates = {tweet_id: count for tweet_id, count in tweet_id_counts.items() if count > 1}
    
    if duplicates:
        print("❌ Still found duplicates:")
        for tweet_id, count in duplicates.items():
            print(f"   {tweet_id}: {count} occurrences")
        return False
    else:
        print("✅ All tweet_ids are now unique!")
        print(f"📊 Total unique tweet_ids: {len(tweet_id_counts)}")
        return True

def show_sample_tweet_ids():
    """Show sample of tweet_ids after fix"""
    print("\n📋 Sample of tweet_ids:")
    
    sample_docs = list(tweets_col.find({}, {"tweet_id": 1, "_id": 0}).limit(10))
    
    for i, doc in enumerate(sample_docs, 1):
        print(f"   {i}. {doc.get('tweet_id', 'N/A')}")
    
    if len(sample_docs) == 10:
        total_count = tweets_col.count_documents({})
        print(f"   ... and {total_count - 10} more")

def create_unique_index():
    """Create a unique index on tweet_id to prevent future duplicates"""
    try:
        print("\n🔒 Creating unique index on tweet_id...")
        tweets_col.create_index("tweet_id", unique=True)
        print("✅ Unique index created successfully!")
        print("   Future duplicate tweet_ids will be automatically prevented.")
    except Exception as e:
        print(f"❌ Failed to create unique index: {e}")
        print("   Note: Index creation might fail if duplicates still exist.")

def main():
    """Main function to fix tweet_id duplicates"""
    print("🐦 Tweet ID Uniqueness Fixer")
    print("=" * 60)
    print(f"🔗 Database: {DB_NAME}")
    print(f"📁 Collection: {COLLECTION_NAME}")
    
    # Check collection exists and has data
    doc_count = tweets_col.count_documents({})
    if doc_count == 0:
        print("❌ Collection is empty or doesn't exist!")
        return
    
    print(f"📊 Total documents in collection: {doc_count}")
    
    # Fix duplicates
    updates_made = fix_duplicate_tweet_ids()
    
    # Verify fix worked
    if verify_uniqueness():
        show_sample_tweet_ids()
        
        # Ask user if they want to create unique index
        if updates_made > 0:
            print("\n🤔 Would you like to create a unique index to prevent future duplicates?")
            print("   This will ensure tweet_id uniqueness is enforced at database level.")
            # Uncomment the line below if you want to automatically create the index
            # create_unique_index()
    
    print("\n✨ Script completed!")

if __name__ == "__main__":
    main()