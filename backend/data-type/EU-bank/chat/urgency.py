# Chat Urgency Analyzer
import os
import json
import requests
import backoff
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

# OpenRouter setup
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"

# Request configuration
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3
RETRY_DELAY = 1

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=180,
    base=RETRY_DELAY,
    on_backoff=lambda details: print(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_openrouter_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call OpenRouter API with exponential backoff and error handling"""
    
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {OPENROUTER_API_KEY}',
        'HTTP-Referer': 'https://your-site.com',  # Optional: for analytics
        'X-Title': 'Chat Urgency Analyzer'  # Optional: for analytics
    }
    
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You are an expert at analyzing workplace communications to determine urgency. You must respond with ONLY 'true' or 'false' - nothing else."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        "temperature": 0.1,  # Low temperature for consistent results
        "max_tokens": 10,    # We only need true/false
        "top_p": 0.9
    }
    
    try:
        response = requests.post(
            OPENROUTER_URL,
            json=payload,
            headers=headers,
            timeout=timeout
        )
        
        response.raise_for_status()
        
        result = response.json()
        
        if "choices" not in result or len(result["choices"]) == 0:
            raise KeyError("No choices in OpenRouter response")
        
        content = result["choices"][0]["message"]["content"].strip().lower()
        
        # Parse the response to boolean
        if content == "true":
            return True
        elif content == "false":
            return False
        else:
            # If response is not exactly true/false, try to interpret
            if "true" in content:
                return True
            elif "false" in content:
                return False
            else:
                raise ValueError(f"Unexpected response: {content}")
        
    except requests.exceptions.Timeout:
        print(f"Request timed out after {timeout} seconds")
        raise
    except requests.exceptions.ConnectionError:
        print("Connection error - check OpenRouter endpoint")
        raise
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error: {e.response.status_code} - {e.response.text[:200]}")
        raise
    except Exception as e:
        print(f"OpenRouter API error: {e}")
        raise

def extract_conversation_text(raw_segments):
    """Extract conversation text from raw_segments for analysis"""
    if not raw_segments:
        return ""
    
    conversation_lines = []
    for segment in raw_segments:
        sender_name = segment.get('sender_name', 'Unknown')
        text = segment.get('text', '')
        conversation_lines.append(f"{sender_name}: {text}")
    
    return "\n".join(conversation_lines)

def analyze_urgency(conversation_text):
    """Analyze EU banking conversation to determine urgency using OpenRouter"""
    
    prompt = f"""
Analyze this EU banking workplace conversation and determine if it indicates URGENT communication that requires immediate attention.

Consider something URGENT (true) if it involves:
- Regulatory compliance deadlines or violations (ECB, EBA, national regulators)
- Customer account security breaches or fraud alerts
- System outages affecting banking operations
- Liquidity issues or capital adequacy concerns
- Payment processing failures or settlement issues
- Risk management escalations or limit breaches
- Audit findings requiring immediate action
- Anti-money laundering (AML) or sanctions alerts
- GDPR data breaches or privacy incidents
- Market disruptions affecting trading or operations
- Customer complaints requiring urgent resolution
- Time-sensitive regulatory reporting (today, ASAP, urgent)
- Crisis management or business continuity issues

Consider something NOT URGENT (false) if it involves:
- Routine regulatory reporting preparation
- Regular compliance reviews and planning
- Standard operational procedures and training
- General policy discussions
- Scheduled meetings and coordination
- Information sharing without time pressure
- Routine customer service matters
- Regular financial reporting and analysis
- Standard risk assessment discussions
- General banking product discussions

Conversation:
{conversation_text}

Respond with ONLY 'true' if urgent or 'false' if not urgent. No explanation needed.
"""
    
    return call_openrouter_with_backoff(prompt)

def test_openrouter_connection():
    """Test OpenRouter API connection"""
    try:
        print(f"Testing connection to OpenRouter API...")
        
        test_prompt = "Is this urgent: 'Can we meet tomorrow?' Respond only true or false."
        result = call_openrouter_with_backoff(test_prompt, timeout=30)
        
        if isinstance(result, bool):
            print("✅ OpenRouter connection test successful")
            return True
        else:
            print("❌ OpenRouter connection test failed - unexpected response type")
            return False
            
    except Exception as e:
        print(f"❌ OpenRouter connection test failed: {e}")
        return False

def process_urgency_analysis():
    """Process chat-chunks and add urgency field"""
    
    # Test OpenRouter connection first
    if not test_openrouter_connection():
        print("❌ Cannot proceed without OpenRouter connection")
        return
    
    # Find chat-chunks that have raw_segments but no urgency field
    chunks_to_analyze = list(chat_chunks_col.find({
        "raw_segments": {"$exists": True, "$ne": []},
        "urgency": {"$exists": False}
    }))
    
    if not chunks_to_analyze:
        print("⚠️ No chat-chunks found that need urgency analysis.")
        return
    
    total_chunks = len(chunks_to_analyze)
    print(f"🔄 Found {total_chunks} chat-chunks to analyze for urgency")
    
    processed_count = 0
    urgent_count = 0
    
    for chunk in chunks_to_analyze:
        chat_id = chunk.get('chat_id')
        raw_segments = chunk.get('raw_segments', [])
        
        print(f"🔄 Analyzing urgency for chat_id: {chat_id}")
        
        # Extract conversation text
        conversation_text = extract_conversation_text(raw_segments)
        
        if not conversation_text.strip():
            print(f"⚠️ Skipping chat_id {chat_id} - no conversation text")
            continue
        
        try:
            # Analyze urgency
            is_urgent = analyze_urgency(conversation_text)
            
            # Update the document with urgency field
            result = chat_chunks_col.update_one(
                {"_id": chunk["_id"]},
                {"$set": {"urgency": is_urgent}}
            )
            
            if result.modified_count > 0:
                processed_count += 1
                if is_urgent:
                    urgent_count += 1
                print(f"✅ Updated chat_id {chat_id} - Urgency: {is_urgent}")
            else:
                print(f"❌ Failed to update chat_id: {chat_id}")
                
        except Exception as e:
            print(f"❌ Error processing chat_id {chat_id}: {e}")
            continue
        
        print(f"📊 Progress: {processed_count}/{total_chunks} (Urgent: {urgent_count})")
        print("-" * 50)
    
    print(f"\n🎯 Urgency analysis complete!")
    print(f"📊 Successfully processed: {processed_count}/{total_chunks} chat-chunks")
    print(f"🚨 Urgent conversations: {urgent_count}/{processed_count}")

def verify_urgency_results():
    """Verify the urgency analysis results"""
    chunks_with_urgency = list(chat_chunks_col.find({
        "urgency": {"$exists": True}
    }))
    
    urgent_count = len([c for c in chunks_with_urgency if c.get('urgency')])
    not_urgent_count = len([c for c in chunks_with_urgency if not c.get('urgency')])
    
    print(f"\n🔍 Verification Results:")
    print(f"   Total analyzed: {len(chunks_with_urgency)}")
    print(f"   Urgent: {urgent_count}")
    print(f"   Not Urgent: {not_urgent_count}")
    
    # Show some examples
    print(f"\n📋 Sample Results:")
    urgent_examples = chat_chunks_col.find({"urgency": True}).limit(3)
    not_urgent_examples = chat_chunks_col.find({"urgency": False}).limit(3)
    
    print("   Urgent conversations:")
    for chunk in urgent_examples:
        chat_id = chunk.get('chat_id')
        topic = chunk.get('dominant_topic', 'N/A')
        print(f"     - chat_id: {chat_id} | topic: {topic}")
    
    print("   Not urgent conversations:")
    for chunk in not_urgent_examples:
        chat_id = chunk.get('chat_id')
        topic = chunk.get('dominant_topic', 'N/A')
        print(f"     - chat_id: {chat_id} | topic: {topic}")

if __name__ == "__main__":
    print("🚀 Starting Chat Urgency Analysis")
    print("=" * 60)
    print(f"🤖 Model: {OPENROUTER_MODEL}")
    print(f"🔗 OpenRouter API URL: {OPENROUTER_URL}")
    print(f"⏱️ Request Timeout: {REQUEST_TIMEOUT}s")
    print(f"🔄 Max Retries: {MAX_RETRIES}")
    
    process_urgency_analysis()
    verify_urgency_results()
    
    print("\n✨ Urgency analysis completed successfully!")