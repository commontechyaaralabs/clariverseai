# Chat Raw Segments Generator
import os
import random
import time
import json
import requests
import backoff
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")
CHAT_CHUNKS_COLLECTION = "chat-chunks"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
chat_chunks_col = db[CHAT_CHUNKS_COLLECTION]

# Ollama setup - Updated to use Cloudflare Tunnel endpoint
OLLAMA_BASE_URL = "https://sleeve-applying-sri-tells.trycloudflare.com"
OLLAMA_TOKEN = "d5823ebcd546e7c6b61a0abebe1d8481d6acb2587b88d1cadfbe651fc4f6c6d5"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_MODEL = "gemma3:27b"

# Request configuration
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_DELAY = 1

if not OLLAMA_TOKEN:
    raise ValueError("Ollama token not configured")

def generate_message_id():
    """Generate a unique message ID as a random number"""
    return random.randint(1000000000000, 9999999999999)

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=180,
    base=RETRY_DELAY,
    on_backoff=lambda details: print(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_ollama_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call Ollama API with exponential backoff and better error handling"""
    
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
            "num_predict": 800,
            "top_k": 30,
            "top_p": 0.9,
            "num_ctx": 4096
        }
    }
    
    try:
        response = requests.post(
            OLLAMA_URL, 
            json=payload, 
            headers=headers,
            timeout=timeout
        )
        
        # Check if response is empty
        if not response.text.strip():
            raise ValueError("Empty response from Ollama API")
        
        response.raise_for_status()
        
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            print(f"JSON decode error. Response text: {response.text[:200]}...")
            raise
        
        if "response" not in result:
            print(f"No 'response' field. Available fields: {list(result.keys())}")
            raise KeyError("No 'response' field in Ollama response")
            
        return result["response"]
        
    except requests.exceptions.Timeout:
        print(f"Request timed out after {timeout} seconds")
        raise
    except requests.exceptions.ConnectionError:
        print("Connection error - check Ollama endpoint")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e.response.status_code} - {e.response.text[:200]}")
        raise
    except requests.exceptions.RequestException as e:
        print(f"Ollama API error: {e}")
        raise
    except json.JSONDecodeError as e:
        print(f"Invalid JSON response: {e}")
        raise
    except (KeyError, ValueError) as e:
        print(f"API response error: {e}")
        raise

def generate_chat_conversation(chat_members, dominant_topic, subtopics, turn_count=9):
    """Generate realistic chat conversation based on topic and subtopics"""
    
    member1 = chat_members[0]
    member2 = chat_members[1]
    
    # Generate timestamps first
    timestamps = generate_timestamps(turn_count)
    
    prompt = f"""
Generate a realistic workplace chat conversation between two colleagues about "{dominant_topic}".

Context/Subtopics to include: {subtopics}

Participants:
- {member1['display_name']} (starts the conversation)
- {member2['display_name']}

Requirements:
- Generate EXACTLY {turn_count} messages
- STRICTLY alternate between participants (member1 starts, then member2, then member1, etc.)
- Make conversation natural, professional, and topic-relevant
- Include realistic details, questions, and responses
- Keep messages concise but informative (1-2 sentences each)

Return ONLY a JSON array of {turn_count} message objects in this exact format:
[
  {{"text": "message content here"}},
  {{"text": "response message here"}},
  ...
]

Do not include any other fields - just the text field for each message.
Make sure the conversation flows naturally and stays on topic.
"""
    
    try:
        time.sleep(0.5)  # Rate limiting
        
        response = call_ollama_with_backoff(prompt)
        
        if not response or not response.strip():
            raise ValueError("Empty response from LLM")
        
        # Clean up potential markdown formatting
        reply = response.strip()
        if reply.startswith("```json"):
            reply = reply[7:]
        if reply.endswith("```"):
            reply = reply[:-3]
        
        # Find JSON array in response
        json_start = reply.find('[')
        json_end = reply.rfind(']') + 1
        
        if json_start == -1 or json_end <= json_start:
            raise ValueError("No valid JSON array found in LLM response")
        
        reply = reply[json_start:json_end]
        
        try:
            messages = json.loads(reply)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
        # Ensure we have exactly the right number of messages
        if len(messages) != turn_count:
            print(f"⚠️ LLM generated {len(messages)} messages instead of {turn_count}, adjusting...")
            if len(messages) > turn_count:
                messages = messages[:turn_count]
            else:
                # If too few, skip this generation
                return []
        
        # Build raw_segments with proper alternating structure
        raw_segments = []
        for i, message in enumerate(messages):
            # Alternate between member1 (even indices) and member2 (odd indices)
            current_member = member1 if i % 2 == 0 else member2
            
            segment = {
                "text": message["text"],
                "timestamp": timestamps[i],
                "sender_id": current_member["id"],
                "sender_name": current_member["display_name"],
                "message_id": {"$numberLong": str(generate_message_id())}
            }
            raw_segments.append(segment)
        
        return raw_segments
        
    except Exception as e:
        print(f"❌ LLM generation error: {e}")
        return []

def generate_timestamps(count, start_time=None):
    """Generate realistic increasing timestamps"""
    if start_time is None:
        start_time = datetime.now()
    
    timestamps = []
    current_time = start_time
    
    for i in range(count):
        timestamps.append(current_time.isoformat() + "Z")
        # Add 2-5 minutes between messages
        current_time += timedelta(minutes=random.randint(2, 5))
    
    return timestamps

def test_ollama_connection():
    """Test if Ollama is running and model is available"""
    try:
        print(f"Testing connection to Ollama: {OLLAMA_BASE_URL}")
        
        # Simple test prompt
        test_prompt = "Generate a JSON array with one object: [{'text': 'hello'}]"
        
        response = call_ollama_with_backoff(test_prompt, timeout=30)
        
        if response and "hello" in response.lower():
            print("✅ Ollama connection test successful")
            return True
        else:
            print("❌ Ollama connection test failed - unexpected response")
            return False
            
    except Exception as e:
        print(f"❌ Ollama connection test failed: {e}")
        return False

def process_chat_chunks():
    """Process existing chat-chunks and add raw_segments"""
    
    # Test Ollama connection first
    if not test_ollama_connection():
        print("❌ Cannot proceed without Ollama connection")
        return
    
    # Find chat-chunks that don't have raw_segments yet
    chunks_to_process = list(chat_chunks_col.find({
        "raw_segments": {"$exists": False},
        "dominant_topic": {"$exists": True},
        "chat_members": {"$exists": True, "$size": 2}
    }))
    
    if not chunks_to_process:
        print("⚠️ No chat-chunks found that need raw_segments generation.")
        return
    
    total_chunks = len(chunks_to_process)
    print(f"🔄 Found {total_chunks} chat-chunks to process")
    
    processed_count = 0
    
    for chunk in chunks_to_process:
        chat_id = chunk.get('chat_id')
        chat_members = chunk.get('chat_members', [])
        dominant_topic = chunk.get('dominant_topic', 'General Discussion')
        subtopics = chunk.get('subtopics', '')
        
        if len(chat_members) < 2:
            print(f"⚠️ Skipping chat_id {chat_id} - insufficient members")
            continue
        
        print(f"🔄 Processing chat_id: {chat_id}")
        print(f"   Topic: {dominant_topic}")
        print(f"   Subtopics: {subtopics}")
        
        # Generate conversation
        turn_count = random.choice([7, 9, 11])  # Random conversation length
        raw_segments = generate_chat_conversation(
            chat_members, 
            dominant_topic, 
            subtopics, 
            turn_count
        )
        
        if not raw_segments:
            print(f"❌ Failed to generate conversation for chat_id: {chat_id}")
            continue
        
        # Update the document with raw_segments
        try:
            result = chat_chunks_col.update_one(
                {"_id": chunk["_id"]},
                {"$set": {"raw_segments": raw_segments}}
            )
            
            if result.modified_count > 0:
                processed_count += 1
                print(f"✅ Updated chat_id {chat_id} with {len(raw_segments)} messages")
            else:
                print(f"❌ Failed to update chat_id: {chat_id}")
                
        except Exception as e:
            print(f"❌ Database update error for chat_id {chat_id}: {e}")
        
        print(f"📊 Progress: {processed_count}/{total_chunks}")
        print("-" * 50)
    
    print(f"\n🎯 Processing complete!")
    print(f"📊 Successfully processed: {processed_count}/{total_chunks} chat-chunks")

def verify_results():
    """Verify the generated raw_segments"""
    chunks_with_segments = list(chat_chunks_col.find({
        "raw_segments": {"$exists": True}
    }).limit(5))
    
    print(f"\n🔍 Verification: Found {len(chunks_with_segments)} chunks with raw_segments")
    
    for chunk in chunks_with_segments:
        segments_count = len(chunk.get('raw_segments', []))
        print(f"   chat_id: {chunk['chat_id']} | segments: {segments_count}")

if __name__ == "__main__":
    print("🚀 Starting Chat Raw Segments Generation")
    print("=" * 60)
    print(f"🤖 Model: {OLLAMA_MODEL}")
    print(f"🔗 Ollama URL: {OLLAMA_BASE_URL}")
    print(f"⏱️ Request Timeout: {REQUEST_TIMEOUT}s")
    print(f"🔄 Max Retries: {MAX_RETRIES}")
    
    process_chat_chunks()
    verify_results()
    
    print("\n✨ Script completed successfully!")