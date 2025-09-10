# Voice Call Conversation Generator
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
VOICE_COLLECTION = "voice"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
voice_col = db[VOICE_COLLECTION]

# Ollama setup - Updated to use Cloudflare Tunnel endpoint
OLLAMA_BASE_URL = "https://metallic-heel-about-prizes.trycloudflare.com/"
OLLAMA_TOKEN = "eac6f9ff2fd50c497cc54348ccf3961bb4022eed77c001012b2aeba0dfc7d76e"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_MODEL = "gemma3:27b"

# Request configuration
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_DELAY = 1

if not OLLAMA_TOKEN:
    raise ValueError("Ollama token not configured")

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
            "temperature": 0.8,
            "num_predict": 1200,
            "top_k": 40,
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

# Removed determine_call_context function - LLM handles everything

def generate_voice_conversation(customer_name, dominant_topic, subtopics, turn_count):
    """Generate realistic voice conversation between customer and bank agent - LLM determines all fields"""
    
    prompt = f"""
Generate a realistic phone conversation between a customer and a virtual voice agent at an EU bank call centre.

Customer: {customer_name}
Main Topic: {dominant_topic}
Specific Areas: {subtopics}

Requirements:
- Generate EXACTLY {turn_count} exchanges (customer speaks, then agent responds)
- Customer always starts the conversation
- STRICTLY alternate: customer -> agent -> customer -> agent...
- Make it sound like a real phone banking conversation
- Agent should be professional, helpful, and follow EU banking protocols
- Include realistic banking terminology and procedures
- Keep responses natural and concise (1-2 sentences each)
- Agent should ask for verification details when appropriate
- Conversation should address the specific topic and subtopics

Based on the conversation you generate, determine:
- Urgency: true/false - Is this an urgent banking matter requiring immediate attention?
- Priority: Choose from "P1 - Critical", "P2 - High", "P3 - Medium", "P4 - Low" based on severity
- Call Purpose: Brief description of what the call is about (e.g., "Account balance inquiry", "Report fraud", etc.)
- Sentiment: Positive/Negative/Neutral based on customer's tone throughout the conversation
- Resolution Status: Resolved/In Progress/Escalated/Unresolved based on how the call ends

Return ONLY a JSON object in this exact format:
{{
  "urgency": true,
  "priority": "P2 - High",
  "conversation": [
    {{"speaker": "user", "text": "customer message here"}},
    {{"speaker": "agent", "text": "agent response here"}},
    ...
  ],
  "sentiment": "Neutral",
  "call_purpose": "Account inquiry",
  "resolution_status": "Resolved"
}}

Generate the conversation first, then analyze it to determine the metadata fields. Make sure everything is realistic for EU banking context.
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
        
        # Find JSON object in response
        json_start = reply.find('{')
        json_end = reply.rfind('}') + 1
        
        if json_start == -1 or json_end <= json_start:
            raise ValueError("No valid JSON object found in LLM response")
        
        reply = reply[json_start:json_end]
        
        try:
            result = json.loads(reply)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
        # Validate response structure - all fields must be present from LLM
        required_fields = ["conversation", "urgency", "priority", "call_purpose", "sentiment", "resolution_status"]
        for field in required_fields:
            if field not in result:
                raise ValueError(f"Missing '{field}' field in LLM response")
        
        conversation = result["conversation"]
        
        # Ensure we have exactly the right number of exchanges
        if len(conversation) != turn_count:
            print(f"⚠️ LLM generated {len(conversation)} exchanges instead of {turn_count}, adjusting...")
            if len(conversation) > turn_count:
                conversation = conversation[:turn_count]
            else:
                # If too few, skip this generation
                return None
        
        # Validate alternating pattern (user starts, then agent, then user...)
        for i, turn in enumerate(conversation):
            expected_speaker = "user" if i % 2 == 0 else "agent"
            if turn.get("speaker") != expected_speaker:
                print(f"⚠️ Speaker pattern error at index {i}: expected {expected_speaker}, got {turn.get('speaker')}")
                return None
        
        # Return exactly what LLM generated - no code modifications
        result["conversation"] = conversation  # Use the potentially trimmed conversation
        return result
        
    except Exception as e:
        print(f"❌ LLM generation error: {e}")
        return None

def test_ollama_connection():
    """Test if Ollama is running and model is available"""
    try:
        print(f"Testing connection to Ollama: {OLLAMA_BASE_URL}")
        
        # Simple test prompt
        test_prompt = 'Generate a JSON object: {"test": "hello", "status": "ok"}'
        
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

def process_voice_calls():
    """Process existing voice collection and add conversation data"""
    
    # Test Ollama connection first
    if not test_ollama_connection():
        print("❌ Cannot proceed without Ollama connection")
        return
    
    # Find voice calls that don't have conversation data yet
    calls_to_process = list(voice_col.find({
        "conversation": {"$exists": False},
        "dominant_topic": {"$exists": True},
        "customer_name": {"$exists": True}
    }))
    
    if not calls_to_process:
        print("⚠️ No voice calls found that need conversation generation.")
        return
    
    total_calls = len(calls_to_process)
    print(f"🔄 Found {total_calls} voice calls to process")
    
    processed_count = 0
    
    for call in calls_to_process:
        call_id = call.get('call_id')
        customer_name = call.get('customer_name', 'Customer')
        dominant_topic = call.get('dominant_topic', 'General Banking Inquiry')
        subtopics = call.get('subtopics', '')
        
        print(f"🔄 Processing call_id: {call_id}")
        print(f"   Customer: {customer_name}")
        print(f"   Topic: {dominant_topic}")
        print(f"   Subtopics: {subtopics}")
        
        # Generate conversation with random length
        turn_count = random.choice([9, 11, 13, 15])
        voice_data = generate_voice_conversation(
            customer_name, 
            dominant_topic, 
            subtopics, 
            turn_count
        )
        
        if not voice_data:
            print(f"❌ Failed to generate conversation for call_id: {call_id}")
            continue
        
        # Update the document with conversation data
        try:
            result = voice_col.update_one(
                {"_id": call["_id"]},
                {"$set": voice_data}
            )
            
            if result.modified_count > 0:
                processed_count += 1
                conversation_length = len(voice_data["conversation"])
                print(f"✅ Updated call_id {call_id} with {conversation_length} exchanges")
                print(f"   Priority: {voice_data['priority']} | Sentiment: {voice_data['sentiment']} | Urgent: {voice_data['urgency']}")
            else:
                print(f"❌ Failed to update call_id: {call_id}")
                
        except Exception as e:
            print(f"❌ Database update error for call_id {call_id}: {e}")
        
        print(f"📊 Progress: {processed_count}/{total_calls}")
        print("-" * 50)
    
    print(f"\n🎯 Processing complete!")
    print(f"📊 Successfully processed: {processed_count}/{total_calls} voice calls")

def verify_results():
    """Verify the generated conversation data"""
    calls_with_conversations = list(voice_col.find({
        "conversation": {"$exists": True}
    }).limit(5))
    
    print(f"\n🔍 Verification: Found {len(calls_with_conversations)} calls with conversations")
    
    for call in calls_with_conversations:
        conversation_count = len(call.get('conversation', []))
        priority = call.get('priority', 'N/A')
        sentiment = call.get('sentiment', 'N/A')
        status = call.get('resolution_status', 'N/A')
        
        print(f"   call_id: {call['call_id']} | exchanges: {conversation_count}")
        print(f"     Priority: {priority} | Sentiment: {sentiment} | Status: {status}")

def show_sample_conversation():
    """Show a sample generated conversation"""
    sample_call = voice_col.find_one({
        "conversation": {"$exists": True}
    })
    
    if sample_call:
        print(f"\n💬 Sample Conversation from call_id: {sample_call['call_id']}")
        print(f"Customer: {sample_call['customer_name']}")
        print(f"Topic: {sample_call['dominant_topic']}")
        print(f"Priority: {sample_call.get('priority', 'N/A')}")
        print("=" * 60)
        
        for i, turn in enumerate(sample_call['conversation'][:6], 1):  # Show first 6 exchanges
            speaker = "🗣️ Customer" if turn['speaker'] == 'user' else "🎧 Agent"
            print(f"{speaker}: {turn['text']}")
        
        if len(sample_call['conversation']) > 6:
            print("... (conversation continues)")
        
        print("=" * 60)
        print(f"Sentiment: {sample_call.get('sentiment', 'N/A')}")
        print(f"Resolution: {sample_call.get('resolution_status', 'N/A')}")

if __name__ == "__main__":
    print("📞 Starting Voice Call Conversation Generation")
    print("=" * 60)
    print(f"🤖 Model: {OLLAMA_MODEL}")
    print(f"🔗 Ollama URL: {OLLAMA_BASE_URL}")
    print(f"⏱️ Request Timeout: {REQUEST_TIMEOUT}s")
    print(f"🔄 Max Retries: {MAX_RETRIES}")
    print(f"💼 Target: EU Bank Call Centre Conversations")
    
    process_voice_calls()
    verify_results()
    show_sample_conversation()
    
    print("\n✨ Script completed successfully!")