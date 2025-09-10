# Updated script to stream chat message generation with random sample of 700 records using Ollama
import os
import random
import uuid
import time
import json
import requests
import signal
import threading
import re
from pymongo import MongoClient
from dotenv import load_dotenv
import backoff

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")
EMAIL_COLLECTION = "llm_email_data"
CHAT_COLLECTION = "chatmessages"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
email_col = db[EMAIL_COLLECTION]
chat_col = db[CHAT_COLLECTION]

# Ollama setup - Use the same configuration as the email generator
OLLAMA_BASE_URL = "http://34.147.17.26:31100"
OLLAMA_TOKEN = "b805213e7b048d21f02dae5922973e9639ef971b0bc6bf804efad9c707527249"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"
OLLAMA_MODEL = "gemma3:27b"
REQUEST_TIMEOUT = 120  # Increased timeout
MAX_RETRIES = 3
RETRY_DELAY = 2

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

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=180,
    base=RETRY_DELAY,
    on_backoff=lambda details: print(f"🔄 Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_ollama_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call Ollama API with exponential backoff and better error handling"""
    if shutdown_flag.is_set():
        raise KeyboardInterrupt("Shutdown requested")
    
    # Prepare headers for remote endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {OLLAMA_TOKEN}' if OLLAMA_TOKEN else None
    }
    # Remove None values
    headers = {k: v for k, v in headers.items() if v is not None}
        
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.7,
            "num_predict": 1200,  # Increased to ensure complete response
            "top_k": 20,
            "top_p": 0.8,
            "num_ctx": 4096
        }
    }
    
    try:
        print(f"🌐 Calling remote Ollama endpoint...")
        response = requests.post(
            OLLAMA_URL, 
            json=payload, 
            headers=headers,
            timeout=timeout
        )
        
        print(f"📡 Response status: {response.status_code}")
        
        # Check if response is empty
        if not response.text.strip():
            raise ValueError("Empty response from Ollama API")
        
        response.raise_for_status()
        
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"❌ JSON decode error. Response text: {response.text[:200]}...")
            raise
        
        if "response" not in result:
            print(f"❌ No 'response' field. Available fields: {list(result.keys())}")
            raise KeyError("No 'response' field in Ollama response")
            
        return result["response"]
        
    except requests.exceptions.Timeout:
        print(f"⏰ Request timed out after {timeout} seconds")
        raise
    except requests.exceptions.ConnectionError:
        print("🔌 Connection error - check remote Ollama endpoint")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP error: {e.response.status_code} - {e.response.text[:200]}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"❌ Ollama API error: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON response: {e}")
        raise
    except (KeyError, ValueError) as e:
        print(f"❌ API response error: {e}")
        raise

def test_ollama_connection():
    """Test if remote Ollama is running and model is available"""
    try:
        print(f"🌐 Testing connection to remote Ollama: {OLLAMA_BASE_URL}")
        
        # Prepare headers for remote endpoint
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {OLLAMA_TOKEN}' if OLLAMA_TOKEN else None
        }
        # Remove None values
        headers = {k: v for k, v in headers.items() if v is not None}
        
        # Test simple generation
        test_payload = {
            "model": OLLAMA_MODEL,
            "prompt": "Say hello",
            "stream": False,
            "options": {"num_predict": 10}
        }
        
        test_response = requests.post(
            OLLAMA_URL, 
            json=test_payload,
            headers=headers,
            timeout=30
        )
        
        print(f"📡 Generation test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            print("❌ Empty response from generation endpoint")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "response" in result:
                print("✅ Ollama connection test successful")
                print(f"🔍 Test response: {result['response'][:50]}...")
                return True
            else:
                print(f"❌ No 'response' field in test. Fields: {list(result.keys())}")
                return False
        except json.JSONDecodeError as e:
            print(f"❌ Generation test returned invalid JSON: {e}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Ollama connection test failed: {e}")
        return False

# Generate unique message ID (random number similar to timestamp format)
def generate_message_id():
    """Generate a unique message ID as a random number"""
    return random.randint(1000000000000, 9999999999999)

# Get random sample of senders excluding those already in chatmessages
def get_random_sample_senders(sample_size=700):
    """Get a random sample of senders from the email collection, excluding those already in chatmessages"""
    try:
        print("🔍 Checking for existing sender_names in chatmessages collection...")
        
        # Get all unique sender_names from chatmessages collection
        existing_sender_names = set(chat_col.distinct("sender_name"))
        print(f"📋 Found {len(existing_sender_names)} unique sender_names already in chatmessages")
        
        # Get all senders from email collection
        print("🔍 Fetching all senders from llm_email_data collection...")
        all_email_senders = list(email_col.find({}, {"sender_name": 1, "sender_id": 1, "_id": 1}))
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
                    {"$project": {"sender_name": 1, "sender_id": 1, "_id": 1}}
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
            if chat_col.count_documents({"sender_name": sender["sender_name"]}) > 0:
                verification_count += 1
        
        if verification_count > 0:
            print(f"❌ VERIFICATION FAILED: {verification_count} senders already exist in chatmessages!")
            # Filter them out
            final_senders = []
            for sender in sample_senders:
                if chat_col.count_documents({"sender_name": sender["sender_name"]}) == 0:
                    final_senders.append(sender)
            sample_senders = final_senders
            print(f"🔧 After final filtering: {len(sample_senders)} confirmed new senders")
        else:
            print("✅ Verification passed: All selected senders are new to chatmessages")
        
        return sample_senders
        
    except Exception as e:
        print(f"❌ Error getting filtered random sample: {e}")
        return []

def extract_json_from_response(response_text):
    """Extract JSON array from response text with improved parsing"""
    try:
        # Remove any markdown formatting
        clean_text = response_text.strip()
        if "```json" in clean_text:
            clean_text = re.sub(r'```json\s*', '', clean_text)
            clean_text = re.sub(r'```\s*$', '', clean_text)
        elif "```" in clean_text:
            clean_text = re.sub(r'```\s*', '', clean_text)
        
        # Try to find JSON array patterns
        json_patterns = [
            r'\[[\s\S]*\]',  # Complete array
            r'\[\s*\{[\s\S]*\}\s*\]',  # Array with objects
        ]
        
        for pattern in json_patterns:
            matches = re.findall(pattern, clean_text, re.MULTILINE | re.DOTALL)
            for match in matches:
                try:
                    # Try to parse the match
                    parsed = json.loads(match)
                    if isinstance(parsed, list) and len(parsed) > 0:
                        return parsed
                except json.JSONDecodeError:
                    continue
        
        # If no pattern works, try the original approach
        json_start = clean_text.find('[')
        json_end = clean_text.rfind(']') + 1
        
        if json_start != -1 and json_end > json_start:
            json_str = clean_text[json_start:json_end]
            try:
                parsed = json.loads(json_str)
                if isinstance(parsed, list):
                    return parsed
            except json.JSONDecodeError:
                pass
        
        # Last resort: try to fix common JSON issues
        if json_start != -1:
            json_str = clean_text[json_start:]
            # Try to complete incomplete JSON
            if not json_str.rstrip().endswith(']'):
                # Find last complete object
                brace_count = 0
                last_complete = -1
                for i, char in enumerate(json_str):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            last_complete = i
                
                if last_complete > -1:
                    json_str = json_str[:last_complete + 1] + ']'
                    try:
                        parsed = json.loads(json_str)
                        if isinstance(parsed, list):
                            return parsed
                    except json.JSONDecodeError:
                        pass
        
        return None
        
    except Exception as e:
        print(f"❌ JSON extraction error: {e}")
        return None

# Generate conversational chat messages using Ollama LLM
def generate_conversation(person1, person2, turn_count):
    """Generate conversation using Ollama with improved JSON parsing"""
    prompt = f"""Generate a realistic banking employee chat conversation between two colleagues.

IMPORTANT: Return ONLY a valid JSON array. No extra text, explanations, or markdown.

Requirements:
- Exactly {turn_count} messages
- Alternate between the two participants
- Each message must have: "sender", "message_text", "timestamp"
- Use realistic banking scenarios (account issues, loan queries, card problems, etc.)
- Keep messages conversational and work-focused
- Use ISO 8601 timestamps starting from 2025-07-31T10:00:00Z

Participants:
- {person1} (Banking Employee)
- {person2} (Banking Employee)

Example format:
[
  {{
    "sender": "{person1}",
    "message_text": "Hey, need help with a customer's account verification issue",
    "timestamp": "2025-07-31T10:00:00Z"
  }},
  {{
    "sender": "{person2}",  
    "message_text": "Sure, what's the account number?",
    "timestamp": "2025-07-31T10:01:00Z"
  }}
]

Generate exactly {turn_count} messages following this format:"""
    
    try:
        if shutdown_flag.is_set():
            return []
            
        time.sleep(0.5)  # Rate limiting
        
        # Use Ollama instead of OpenAI client
        response = call_ollama_with_backoff(prompt)
        
        if not response or not response.strip():
            print("⚠ Empty response from Ollama API")
            return []
        
        # Use improved JSON extraction
        parsed_json = extract_json_from_response(response)
        
        if not parsed_json:
            print("⚠ No valid JSON array found in response")
            print(f"Response preview: {response[:300]}...")
            return []
        
        if not isinstance(parsed_json, list):
            print("⚠ Response is not a JSON array")
            return []
        
        # Validate and clean the messages
        valid_messages = []
        current_time = time.time()
        
        for i, msg in enumerate(parsed_json):
            if not isinstance(msg, dict):
                continue
                
            required_fields = ["sender", "message_text", "timestamp"]
            if not all(field in msg for field in required_fields):
                continue
            
            # Clean and validate the message
            cleaned_msg = {
                "sender": str(msg["sender"]).strip(),
                "message_text": str(msg["message_text"]).strip(),
                "timestamp": str(msg["timestamp"]).strip()
            }
            
            # Ensure sender is one of our participants
            if cleaned_msg["sender"] not in [person1, person2]:
                # Try to match partial names
                if person1.split()[0] in cleaned_msg["sender"] or cleaned_msg["sender"] in person1:
                    cleaned_msg["sender"] = person1
                elif person2.split()[0] in cleaned_msg["sender"] or cleaned_msg["sender"] in person2:
                    cleaned_msg["sender"] = person2
                else:
                    # Assign based on alternating pattern
                    cleaned_msg["sender"] = person1 if i % 2 == 0 else person2
            
            # Validate timestamp format
            if not re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z?', cleaned_msg["timestamp"]):
                # Generate a proper timestamp
                timestamp_offset = i * 60  # 1 minute apart
                from datetime import datetime, timezone
                dt = datetime.fromtimestamp(current_time + timestamp_offset, tz=timezone.utc)
                cleaned_msg["timestamp"] = dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            
            valid_messages.append(cleaned_msg)
        
        # Ensure we have a reasonable number of messages
        if len(valid_messages) < turn_count // 2:
            print(f"⚠ Only got {len(valid_messages)} valid messages out of {turn_count} expected")
            return []
        
        # Trim to exact count if we have too many
        if len(valid_messages) > turn_count:
            valid_messages = valid_messages[:turn_count]
        
        print(f"✅ Generated {len(valid_messages)} valid messages")
        return valid_messages
            
    except KeyboardInterrupt:
        print("🛑 Conversation generation interrupted")
        return []
    except Exception as e:
        print(f"❌ LLM generation error: {e}")
        return []

# Main logic with random sampling
def generate_chat_data_streaming(sample_size=700):
    """Generate chat data with Ollama instead of OpenRouter"""
    
    # Test Ollama connection first
    if not test_ollama_connection():
        print("❌ Cannot proceed without Ollama connection")
        return
    
    # Get random sample of senders
    senders = get_random_sample_senders(sample_size)
    
    if len(senders) < 2:
        print("⚠️ Not enough new senders in sample to generate pairs.")
        print("💡 This might happen if most sender_names already exist in chatmessages.")
        return
    
    # Shuffle the sample for additional randomness
    random.shuffle(senders)
    
    # Create a mapping of sender info to consistent sender_id
    sender_id_mapping = {}
    for sender in senders:
        sender_key = f"{sender['sender_name']}_{sender['sender_id']}"
        if sender_key not in sender_id_mapping:
            sender_id_mapping[sender_key] = uuid.uuid4().hex
    
    total_pairs = len(senders) // 2
    print(f"🔗 Total pairs to generate from sample: {total_pairs}")
    print(f"📋 Sample size: {len(senders)} senders")
    
    total_inserted = 0
    consecutive_failures = 0
    successful_pairs = 0
    
    try:
        for i in range(0, total_pairs * 2, 2):
            if shutdown_flag.is_set():
                print(f"\n🛑 Shutdown requested. Stopping at pair {i//2 + 1}")
                break
                
            person1 = senders[i]
            person2 = senders[i + 1]
            
            # Get consistent sender_ids for both persons
            person1_key = f"{person1['sender_name']}_{person1['sender_id']}"
            person2_key = f"{person2['sender_name']}_{person2['sender_id']}"
            person1_sender_id = sender_id_mapping[person1_key]
            person2_sender_id = sender_id_mapping[person2_key]
            
            chat_id = uuid.uuid4().hex
            turns = random.choice([7, 9, 11, 13])
            
            print(f"🔄 Generating conversation {i//2 + 1}/{total_pairs}: {person1['sender_name']} ↔ {person2['sender_name']}")
            
            # Generate conversation using Ollama
            messages = generate_conversation(person1["sender_name"], person2["sender_name"], turns)
            
            if not messages or len(messages) < 3:  # Need at least 3 messages for a meaningful conversation
                print(f"⚠️ Skipping pair {i//2 + 1} due to LLM error or insufficient messages.")
                consecutive_failures += 1
                
                # Check for too many consecutive failures
                if consecutive_failures >= 5:
                    print(f"❌ Too many consecutive failures ({consecutive_failures}). Stopping generation.")
                    break
                continue
            
            consecutive_failures = 0  # Reset failure counter on success
            successful_pairs += 1
            
            # Insert messages into database
            inserted_now = 0
            for msg in messages:
                if shutdown_flag.is_set():
                    break
                    
                # Determine which person sent this message
                if msg["sender"] == person1["sender_name"]:
                    sender = person1
                    current_sender_id = person1_sender_id
                else:
                    sender = person2
                    current_sender_id = person2_sender_id
                
                doc = {
                    "message_id": generate_message_id(),  # Unique message ID
                    "chat_id": chat_id,
                    "sender_id": current_sender_id,  # Consistent sender ID for each person
                    "mail_id": str(sender["_id"]),
                    "sender_name": sender["sender_name"],
                    "original_sender_id": sender["sender_id"],  # Keep original sender_id for reference
                    "message_text": msg["message_text"],
                    "timestamp": msg["timestamp"]
                }
                
                try:
                    chat_col.insert_one(doc)
                    inserted_now += 1
                except Exception as e:
                    print(f"❌ Database insertion error: {e}")
                    break
            
            total_inserted += inserted_now
            print(f"✅ Pair {i//2 + 1}/{total_pairs} | chat_id: {chat_id} | messages: {inserted_now} | total so far: {total_inserted}")
            
            # Progress update every 10 pairs
            if (i//2 + 1) % 10 == 0:
                success_rate = (successful_pairs / (i//2 + 1)) * 100
                print(f"📊 Progress: {((i//2 + 1) / total_pairs) * 100:.1f}% | Success rate: {success_rate:.1f}% | Total messages: {total_inserted}")
            
            # Small delay between pairs to prevent overwhelming the API
            if not shutdown_flag.is_set():
                time.sleep(2)
        
        if shutdown_flag.is_set():
            print(f"\n🛑 Chat generation interrupted gracefully!")
        else:
            print(f"\n🎯 Chat message generation complete!")
            
        print(f"📊 Sample size: {len(senders)} senders")
        print(f"🔗 Pairs attempted: {min(i//2 + 1, total_pairs)}")
        print(f"✅ Successful pairs: {successful_pairs}")
        print(f"💬 Total messages inserted: {total_inserted}")
        print(f"📈 Success rate: {(successful_pairs / min(i//2 + 1, total_pairs)) * 100:.1f}%")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        shutdown_flag.set()

# Helper function to query all messages for a specific sender
def get_messages_by_sender_id(sender_id):
    """Query all messages for a specific sender_id"""
    messages = list(chat_col.find({"sender_id": sender_id}).sort("timestamp", 1))
    return messages

# Helper function to get all conversations for a specific sender
def get_conversations_by_sender_id(sender_id):
    """Get all chat_ids where a specific sender participated"""
    chat_ids = chat_col.distinct("chat_id", {"sender_id": sender_id})
    conversations = {}
    
    for chat_id in chat_ids:
        messages = list(chat_col.find({"chat_id": chat_id}).sort("timestamp", 1))
        conversations[chat_id] = messages
    
    return conversations

# Helper function to get chat statistics
def get_chat_statistics():
    """Get statistics about the chat collection"""
    total_messages = chat_col.count_documents({})
    total_chats = len(chat_col.distinct("chat_id"))
    total_senders = len(chat_col.distinct("sender_id"))
    
    print(f"\n📊 Chat Collection Statistics:")
    print(f"Total messages: {total_messages}")
    print(f"Total conversations: {total_chats}")
    print(f"Unique senders: {total_senders}")
    
    if total_chats > 0:
        avg_messages_per_chat = total_messages / total_chats
        print(f"Average messages per chat: {avg_messages_per_chat:.1f}")
    
    return {
        "total_messages": total_messages,
        "total_chats": total_chats,
        "total_senders": total_senders
    }

# Helper function to get sample conversations
def get_sample_conversations(limit=2):
    """Get sample conversations for testing"""
    chat_ids = chat_col.distinct("chat_id")
    sample_chat_ids = random.sample(chat_ids, min(limit, len(chat_ids)))
    
    conversations = {}
    for chat_id in sample_chat_ids:
        messages = list(chat_col.find({"chat_id": chat_id}).sort("timestamp", 1))
        conversations[chat_id] = messages
    
    return conversations

# Main execution
def main():
    """Main function to initialize and run the chat generator"""
    print("💬 Banking Chat Data Generator Starting... (Ollama Version)")
    print(f"🎯 Target: Generate chat conversations with unique sender_names")
    print(f"💾 Database: {DB_NAME}")
    print(f"📂 Collections: {EMAIL_COLLECTION} -> {CHAT_COLLECTION}")
    print(f"🤖 Model: {OLLAMA_MODEL}")
    print(f"🔗 Ollama URL: {OLLAMA_BASE_URL}")
    
    # Setup signal handlers
    setup_signal_handlers()
    
    try:
        # Get initial statistics
        print("\n📊 Initial Collection Statistics:")
        total_email_senders = email_col.count_documents({})
        total_chat_senders = len(chat_col.distinct("sender_name"))
        print(f"📧 Total senders in llm_email_data: {total_email_senders}")
        print(f"💬 Unique sender_names in chatmessages: {total_chat_senders}")
        print(f"🆕 Potential new senders: {total_email_senders - total_chat_senders}")
        
        # Run the chat generation with random sample of 700 senders (only new ones)
        generate_chat_data_streaming(sample_size=700)
        
        # Print final statistics
        print("\n📊 Final Collection Statistics:")
        get_chat_statistics()
        
        # Show sample conversations
        print(f"\n💬 Sample conversations:")
        sample_conversations = get_sample_conversations(2)
        
        for i, (chat_id, messages) in enumerate(sample_conversations.items(), 1):
            print(f"\n--- Sample Conversation {i} (Chat ID: {chat_id}) ---")
            for msg in messages[:5]:  # Show first 5 messages
                print(f"{msg['sender_name']}: {msg['message_text']}")
            if len(messages) > 5:
                print(f"... and {len(messages) - 5} more messages")
        
        print(f"\n🔍 Example helper functions:")
        print("- get_messages_by_sender_id('sender_id_here')")
        print("- get_conversations_by_sender_id('sender_id_here')")
        print("- get_chat_statistics()")
        print("- get_sample_conversations(3)")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Generation interrupted by user!")
    except Exception as e:
        print(f"\n❌ Unexpected error in main: {e}")
    finally:
        if client:
            client.close()
            print("✅ Database connection closed")

# Run the chat generator
if __name__ == "__main__":
    main()