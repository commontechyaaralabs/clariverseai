# Simplified script to populate chat-chunks with sender/receiver data from llm_email_data
import os
import random
import uuid
import time
import signal
import threading
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime, timezone

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")
EMAIL_COLLECTION = "emailmessages"
CHAT_COLLECTION = "chat-chunks"  # You can rename this to "chat-chunks" if needed

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
email_col = db[EMAIL_COLLECTION]
chat_col = db[CHAT_COLLECTION]

# Global shutdown flag
shutdown_flag = threading.Event()

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        print(f"\n🛑 Received signal {signum}. Initiating graceful shutdown...")
        shutdown_flag.set()
        print("⏳ Please wait for current operations to complete...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

# Generate unique message ID (random number similar to timestamp format)
def generate_message_id():
    """Generate a unique message ID as a random number"""
    return random.randint(1000000000000, 9999999999999)

# Generate unique chat ID
def generate_chat_id():
    """Generate a unique chat ID"""
    return uuid.uuid4().hex

# Generate unique user ID
def generate_user_id():
    """Generate a unique user ID"""
    return uuid.uuid4().hex

# Get random sample of senders excluding those already in chatmessages
def get_random_sample_senders(sample_size=1200):
    """Get a random sample of senders from the email collection, excluding those already in chatmessages"""
    try:
        print("🔍 Checking for existing sender_names in chatmessages collection...")
        
        # Get all unique sender_names from chatmessages collection
        existing_sender_names = set(chat_col.distinct("chat_members.display_name"))
        print(f"📋 Found {len(existing_sender_names)} unique sender_names already in chatmessages")
        
        # Get all senders from email collection with email field
        print("🔍 Fetching all senders from llm_email_data collection...")
        all_email_senders = list(email_col.find({}, {"sender_name": 1, "sender_id": 1, "email": 1, "_id": 1}))
        print(f"📊 Total senders in llm_email_data: {len(all_email_senders)}")
        
        # Filter out senders that already exist in chatmessages
        available_senders = []
        for sender in all_email_senders:
            if sender["sender_name"] not in existing_sender_names:
                available_senders.append(sender)
        
        print(f"✅ Available new senders (not in chatmessages): {len(available_senders)}")
        
        if len(available_senders) == 0:
            print("⚠️ No new senders available - all sender_names already exist in chatmessages")
            return []
        
        # Get random sample from available senders
        if len(available_senders) <= sample_size:
            print(f"📊 Using all {len(available_senders)} available senders (less than requested sample size)")
            sample_senders = available_senders
        else:
            # Use MongoDB's $sample aggregation for better randomness
            try:
                # Create a match condition to exclude existing sender_names
                pipeline = [
                    {"$match": {"sender_name": {"$nin": list(existing_sender_names)}}},
                    {"$sample": {"size": sample_size}},
                    {"$project": {"sender_name": 1, "sender_id": 1, "email": 1, "_id": 1}}
                ]
                
                sample_senders = list(email_col.aggregate(pipeline))
                
                if len(sample_senders) < sample_size:
                    print(f"⚠️ MongoDB $sample returned {len(sample_senders)} senders, less than requested {sample_size}")
                else:
                    print(f"📊 Retrieved random sample of {len(sample_senders)} new senders using MongoDB aggregation")
                
            except Exception as e:
                print(f"⚠️ MongoDB aggregation failed: {e}. Using Python random sampling...")
                # Fallback to Python random sampling
                sample_senders = random.sample(available_senders, sample_size)
                print(f"📊 Retrieved random sample of {len(sample_senders)} new senders (fallback method)")
        
        # Double-check that no sender_names are duplicated in our sample
        sender_names_in_sample = [s["sender_name"] for s in sample_senders]
        unique_names_in_sample = set(sender_names_in_sample)
        
        if len(sender_names_in_sample) != len(unique_names_in_sample):
            print("⚠️ Duplicate sender_names found in sample, removing duplicates...")
            # Remove duplicates while preserving order
            seen = set()
            deduplicated_senders = []
            for sender in sample_senders:
                if sender["sender_name"] not in seen:
                    seen.add(sender["sender_name"])
                    deduplicated_senders.append(sender)
            sample_senders = deduplicated_senders
            print(f"✅ After deduplication: {len(sample_senders)} unique senders")
        
        # Final verification - ensure none of these sender_names exist in chatmessages
        verification_count = 0
        for sender in sample_senders:
            if chat_col.count_documents({"chat_members.display_name": sender["sender_name"]}) > 0:
                verification_count += 1
        
        if verification_count > 0:
            print(f"❌ VERIFICATION FAILED: {verification_count} senders already exist in chatmessages!")
            # Filter them out
            final_senders = []
            for sender in sample_senders:
                if chat_col.count_documents({"chat_members.display_name": sender["sender_name"]}) == 0:
                    final_senders.append(sender)
            sample_senders = final_senders
            print(f"🔧 After final filtering: {len(sample_senders)} confirmed new senders")
        else:
            print("✅ Verification passed: All selected senders are new to chatmessages")
        
        return sample_senders
        
    except Exception as e:
        print(f"❌ Error getting filtered random sample: {e}")
        return []

# Main logic with random sampling
def populate_chat_chunks_streaming(sample_size=1200):
    """Populate chat chunks with sender/receiver data"""
    
    # Get random sample of senders
    senders = get_random_sample_senders(sample_size)
    
    if len(senders) < 2:
        print("⚠️ Not enough new senders in sample to generate pairs.")
        print("💡 This might happen if most sender_names already exist in chatmessages.")
        return
    
    # Shuffle the sample for additional randomness
    random.shuffle(senders)
    
    # Create a mapping of sender info to consistent user_id
    user_id_mapping = {}
    for sender in senders:
        sender_key = f"{sender['sender_name']}_{sender['sender_id']}"
        if sender_key not in user_id_mapping:
            user_id_mapping[sender_key] = generate_user_id()
    
    total_pairs = len(senders) // 2
    print(f"🔗 Total pairs to generate from sample: {total_pairs} (from {len(senders)} senders)")
    print(f"📋 Sample size: {len(senders)} senders → {total_pairs} conversation pairs")
    
    total_inserted = 0
    successful_pairs = 0
    
    try:
        for i in range(0, total_pairs * 2, 2):
            if shutdown_flag.is_set():
                print(f"\n🛑 Shutdown requested. Stopping at pair {i//2 + 1}")
                break
                
            person1 = senders[i]
            person2 = senders[i + 1]
            
            # Get consistent user_ids for both persons
            person1_key = f"{person1['sender_name']}_{person1['sender_id']}"
            person2_key = f"{person2['sender_name']}_{person2['sender_id']}"
            person1_user_id = user_id_mapping[person1_key]
            person2_user_id = user_id_mapping[person2_key]
            
            chat_id = generate_chat_id()
            
            print(f"🔄 Creating conversation {i//2 + 1}/{total_pairs}: {person1['sender_name']} ↔ {person2['sender_name']}")
            
            # Create chat_members array
            chat_members = [
                {
                    "id": person1_user_id,
                    "roles": ["participant"],
                    "display_name": person1["sender_name"],
                    "user_id": person1_user_id,
                    "email": person1.get("email", f"{person1['sender_name'].lower().replace(' ', '')}@example.com"),
                    "tenant_id": str(person1["_id"])
                },
                {
                    "id": person2_user_id,
                    "roles": ["participant"],
                    "display_name": person2["sender_name"],
                    "user_id": person2_user_id,
                    "email": person2.get("email", f"{person2['sender_name'].lower().replace(' ', '')}@example.com"),
                    "tenant_id": str(person2["_id"])
                }
            ]
            
            # Create the chat document
            chat_doc = {
                "chat_id": chat_id,
                "chat_members": chat_members
            }
            
            try:
                chat_col.insert_one(chat_doc)
                total_inserted += 1
                successful_pairs += 1
                print(f"✅ Pair {i//2 + 1}/{total_pairs} | chat_id: {chat_id} | members: {person1['sender_name']}, {person2['sender_name']}")
            except Exception as e:
                print(f"❌ Database insertion error: {e}")
                continue
            
            # Progress update every 10 pairs
            if (i//2 + 1) % 10 == 0:
                success_rate = (successful_pairs / (i//2 + 1)) * 100
                print(f"📊 Progress: {((i//2 + 1) / total_pairs) * 100:.1f}% | Success rate: {success_rate:.1f}% | Total chats: {total_inserted}")
            
            # Small delay between pairs
            if not shutdown_flag.is_set():
                time.sleep(0.1)
        
        if shutdown_flag.is_set():
            print(f"\n🛑 Chat population interrupted gracefully!")
        else:
            print(f"\n🎯 Chat chunks population complete!")
            
        print(f"📊 Sample size: {len(senders)} senders")
        print(f"🔗 Pairs attempted: {min(i//2 + 1, total_pairs)}")
        print(f"✅ Successful pairs: {successful_pairs}")
        print(f"💬 Total chats inserted: {total_inserted}")
        print(f"📈 Success rate: {(successful_pairs / min(i//2 + 1, total_pairs)) * 100:.1f}%")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        shutdown_flag.set()

# Helper function to query all chats for a specific user
def get_chats_by_user_id(user_id):
    """Query all chats where a specific user_id participated"""
    chats = list(chat_col.find({"chat_members.user_id": user_id}))
    return chats

# Helper function to get all conversations for a specific user
def get_conversations_by_user_id(user_id):
    """Get all chat_ids where a specific user participated"""
    chat_ids = chat_col.distinct("chat_id", {"chat_members.user_id": user_id})
    conversations = {}
    
    for chat_id in chat_ids:
        chat = chat_col.find_one({"chat_id": chat_id})
        if chat:
            conversations[chat_id] = chat
    
    return conversations

# Helper function to get chat statistics
def get_chat_statistics():
    """Get statistics about the chat collection"""
    total_chats = chat_col.count_documents({})
    
    # Get unique participants using a safer approach
    all_chats = list(chat_col.find({}, {"chat_members": 1}))
    total_participants = set()
    total_members = 0
    
    for chat in all_chats:
        if "chat_members" in chat:
            for member in chat["chat_members"]:
                if "user_id" in member:
                    total_participants.add(member["user_id"])
                    total_members += 1
    
    print(f"\n📊 Chat Collection Statistics:")
    print(f"Total chats: {total_chats}")
    print(f"Unique participants: {len(total_participants)}")
    
    if total_chats > 0:
        avg_members_per_chat = total_members / total_chats
        print(f"Total members across all chats: {total_members}")
        print(f"Average members per chat: {avg_members_per_chat:.1f}")
    
    return {
        "total_chats": total_chats,
        "total_participants": len(total_participants)
    }

# Helper function to get sample conversations
def get_sample_conversations(limit=2):
    """Get sample conversations for testing"""
    sample_chats = list(chat_col.find().limit(limit))
    
    conversations = {}
    for chat in sample_chats:
        if "chat_id" in chat:
            conversations[chat["chat_id"]] = chat
    
    return conversations

# Main execution
def main():
    """Main function to initialize and run the chat chunks populator"""
    print("💬 Banking Chat Chunks Populator Starting...")
    print(f"🎯 Target: Take 1200 senders from llm_email_data and pair them as 600 conversation pairs")
    print(f"💾 Database: {DB_NAME}")
    print(f"📂 Collections: {EMAIL_COLLECTION} -> {CHAT_COLLECTION}")
    
    # Setup signal handlers
    setup_signal_handlers()
    
    try:
        # Get initial statistics
        print("\n📊 Initial Collection Statistics:")
        total_email_senders = email_col.count_documents({})
        total_chat_participants = len(chat_col.distinct("chat_members.display_name"))
        print(f"📧 Total senders in llm_email_data: {total_email_senders}")
        print(f"💬 Unique participants in chat-chunks: {total_chat_participants}")
        print(f"🆕 Potential new participants: {total_email_senders - total_chat_participants}")
        
        # Run the chat population with random sample of 1200 senders (only new ones) to create 600 pairs
        populate_chat_chunks_streaming(sample_size=1200)
        
        # Print final statistics
        print("\n📊 Final Collection Statistics:")
        get_chat_statistics()
        
        # Show sample conversations
        print(f"\n💬 Sample conversations:")
        sample_conversations = get_sample_conversations(2)
        
        for i, (chat_id, chat) in enumerate(sample_conversations.items(), 1):
            print(f"\n--- Sample Conversation {i} (Chat ID: {chat_id}) ---")
            for member in chat["chat_members"]:
                print(f"👤 {member['display_name']} ({member['email']}) - User ID: {member['user_id']}")
        
        print(f"\n🔍 Helper functions available:")
        print("- get_chats_by_user_id('user_id_here')")
        print("- get_conversations_by_user_id('user_id_here')")
        print("- get_chat_statistics()")
        print("- get_sample_conversations(3)")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Population interrupted by user!")
    except Exception as e:
        print(f"\n❌ Unexpected error in main: {e}")
    finally:
        if client:
            client.close()
            print("✅ Database connection closed")

# Run the chat populator
if __name__ == "__main__":
    main()