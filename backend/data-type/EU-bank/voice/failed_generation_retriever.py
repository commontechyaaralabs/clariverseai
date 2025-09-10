# Failed Generation Retriever for Voice Calls
import os
import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")
VOICE_COLLECTION = "voice"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
voice_col = db[VOICE_COLLECTION]

def get_failed_generation_calls():
    """Retrieve call IDs where LLM generation failed"""
    
    # Find calls that don't have conversation data but have required fields
    failed_calls = list(voice_col.find({
        "conversation": {"$exists": False},
        "dominant_topic": {"$exists": True},
        "customer_name": {"$exists": True}
    }))
    
    return failed_calls

def get_failed_calls_by_date_range(start_date=None, end_date=None):
    """Retrieve failed calls within a specific date range"""
    
    if not start_date:
        start_date = datetime.now() - timedelta(days=7)  # Default to last 7 days
    
    if not end_date:
        end_date = datetime.now()
    
    # Find calls without conversation data within date range
    failed_calls = list(voice_col.find({
        "conversation": {"$exists": False},
        "dominant_topic": {"$exists": True},
        "customer_name": {"$exists": True},
        "created_at": {
            "$gte": start_date,
            "$lte": end_date
        }
    }))
    
    return failed_calls

def get_failed_calls_by_topic(topic=None):
    """Retrieve failed calls by specific topic"""
    
    query = {
        "conversation": {"$exists": False},
        "dominant_topic": {"$exists": True},
        "customer_name": {"$exists": True}
    }
    
    if topic:
        query["dominant_topic"] = topic
    
    failed_calls = list(voice_col.find(query))
    
    return failed_calls

def export_failed_calls_to_json(filename=None):
    """Export failed calls to JSON file"""
    
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"failed_generation_calls_{timestamp}.json"
    
    failed_calls = get_failed_generation_calls()
    
    # Convert ObjectId to string for JSON serialization
    export_data = []
    for call in failed_calls:
        call_copy = call.copy()
        call_copy['_id'] = str(call_copy['_id'])
        export_data.append(call_copy)
    
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(export_data, f, indent=2, default=str, ensure_ascii=False)
    
    print(f"✅ Exported {len(export_data)} failed calls to {filename}")
    return filename

def print_failed_calls_summary():
    """Print a summary of failed generation calls"""
    
    failed_calls = get_failed_generation_calls()
    
    if not failed_calls:
        print("✅ No failed generation calls found!")
        return
    
    print(f"❌ Found {len(failed_calls)} calls with failed LLM generation:")
    print("=" * 80)
    
    # Group by topic for better analysis
    topic_counts = {}
    for call in failed_calls:
        topic = call.get('dominant_topic', 'Unknown')
        topic_counts[topic] = topic_counts.get(topic, 0) + 1
    
    print("📊 Failed calls by topic:")
    for topic, count in sorted(topic_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"   {topic}: {count} calls")
    
    print("\n📋 Sample failed call IDs:")
    for i, call in enumerate(failed_calls[:10]):  # Show first 10
        call_id = call.get('call_id', 'N/A')
        customer = call.get('customer_name', 'Unknown')
        topic = call.get('dominant_topic', 'Unknown')
        print(f"   {i+1}. Call ID: {call_id} | Customer: {customer} | Topic: {topic}")
    
    if len(failed_calls) > 10:
        print(f"   ... and {len(failed_calls) - 10} more calls")

def retry_failed_generations():
    """Get list of call IDs that can be retried"""
    
    failed_calls = get_failed_generation_calls()
    
    if not failed_calls:
        print("✅ No failed calls to retry!")
        return []
    
    print(f"🔄 Found {len(failed_calls)} calls that can be retried:")
    
    retry_list = []
    for call in failed_calls:
        call_id = call.get('call_id', 'N/A')
        customer = call.get('customer_name', 'Unknown')
        topic = call.get('dominant_topic', 'Unknown')
        
        retry_list.append({
            'call_id': call_id,
            'customer_name': customer,
            'dominant_topic': topic,
            'subtopics': call.get('subtopics', ''),
            'mongo_id': str(call['_id'])
        })
        
        print(f"   📞 {call_id} | {customer} | {topic}")
    
    return retry_list

def get_failed_calls_stats():
    """Get detailed statistics about failed generations"""
    
    failed_calls = get_failed_generation_calls()
    
    if not failed_calls:
        return {"total_failed": 0}
    
    # Basic stats
    stats = {
        "total_failed": len(failed_calls),
        "topics": {},
        "customers": {},
        "date_range": {}
    }
    
    # Topic analysis
    for call in failed_calls:
        topic = call.get('dominant_topic', 'Unknown')
        stats["topics"][topic] = stats["topics"].get(topic, 0) + 1
    
    # Customer analysis
    for call in failed_calls:
        customer = call.get('customer_name', 'Unknown')
        stats["customers"][customer] = stats["customers"].get(customer, 0) + 1
    
    # Date analysis (if available)
    dates = []
    for call in failed_calls:
        if 'created_at' in call:
            dates.append(call['created_at'])
    
    if dates:
        stats["date_range"] = {
            "earliest": min(dates),
            "latest": max(dates)
        }
    
    return stats

if __name__ == "__main__":
    print("🔍 Failed Generation Call Retriever")
    print("=" * 60)
    
    # Get and display failed calls
    print_failed_calls_summary()
    
    print("\n" + "=" * 60)
    
    # Get retry list
    retry_list = retry_failed_generations()
    
    print("\n" + "=" * 60)
    
    # Get statistics
    stats = get_failed_calls_stats()
    print(f"📊 Statistics:")
    print(f"   Total failed calls: {stats['total_failed']}")
    
    if stats['topics']:
        print(f"   Top failing topic: {max(stats['topics'], key=stats['topics'].get)}")
    
    if stats['customers']:
        print(f"   Customer with most failures: {max(stats['customers'], key=stats['customers'].get)}")
    
    # Export option
    if retry_list:
        print(f"\n💾 Export failed calls to JSON? (y/n): ", end="")
        try:
            user_input = input().lower().strip()
            if user_input in ['y', 'yes']:
                filename = export_failed_calls_to_json()
                print(f"📁 Exported to: {filename}")
        except KeyboardInterrupt:
            print("\n\n✨ Script completed!")
    
    print("\n✨ Script completed!")
