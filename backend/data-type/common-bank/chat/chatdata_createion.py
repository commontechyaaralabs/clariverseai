# Updated script to stream chat message generation and insert into DB with intermediate results
import os
import random
import uuid
import time
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")
EMAIL_COLLECTION = "processed_email_messages"
CHAT_COLLECTION = "chatmessages"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
email_col = db[EMAIL_COLLECTION]
chat_col = db[CHAT_COLLECTION]

# OpenRouter API setup (using new OpenAI v1.0+ client)
openai_client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)

if not os.getenv("OPENROUTER_API_KEY"):
    raise ValueError("OPENROUTER_API_KEY not found in .env")

# Generate unique message ID (random number similar to timestamp format)
def generate_message_id():
    """Generate a unique message ID as a random number"""
    return random.randint(1000000000000, 9999999999999)

# Generate conversational chat messages using LLM
def generate_conversation(person1, person2, turn_count):
    prompt = f"""
Generate a customer support-style chat between two people.
Return JSON array of {turn_count} messages, alternating between them.
Each message should have:
- "message_text": 1 line of natural chat text (avoid formal email style)
- "timestamp": realistic ISO 8601 timestamp, increasing with each message starting from now

Participants:
- person_1: "{person1}"
- person_2: "{person2}"

Make the conversation helpful, informal, and realistic.
Return only JSON array of objects like:
[
  {{
    "sender": "{person1}",
    "message_text": "Hello!",
    "timestamp": "2025-07-29T11:15:00Z"
  }},
  ...
]
"""
    
    try:
        time.sleep(0.5)  # Rate limiting
        
        response = openai_client.chat.completions.create(
            model="openai/gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.7,
            max_tokens=600
        )
        
        reply = response.choices[0].message.content.strip()
        
        # Clean up potential markdown formatting
        if reply.startswith("```json"):
            reply = reply[7:]
        if reply.endswith("```"):
            reply = reply[:-3]
        
        return json.loads(reply)
    
    except Exception as e:
        print(f"❌ LLM generation error: {e}")
        return []

# Main logic
def generate_chat_data_streaming():
    # Get all unique senders
    senders = list(email_col.find({}, {"sender_name": 1, "sender_mail": 1, "_id": 1}))
    random.shuffle(senders)
    
    if len(senders) < 2:
        print("⚠️ Not enough unique senders to generate pairs.")
        return
    
    # Create a mapping of sender info to consistent sender_id
    sender_id_mapping = {}
    for sender in senders:
        sender_key = f"{sender['sender_name']}_{sender['sender_mail']}"
        if sender_key not in sender_id_mapping:
            sender_id_mapping[sender_key] = uuid.uuid4().hex
    
    total_pairs = len(senders) // 2
    print(f"🔗 Total pairs to generate: {total_pairs}")
    
    total_inserted = 0
    
    for i in range(0, total_pairs * 2, 2):
        person1 = senders[i]
        person2 = senders[i + 1]
        
        # Get consistent sender_ids for both persons
        person1_key = f"{person1['sender_name']}_{person1['sender_mail']}"
        person2_key = f"{person2['sender_name']}_{person2['sender_mail']}"
        person1_sender_id = sender_id_mapping[person1_key]
        person2_sender_id = sender_id_mapping[person2_key]
        
        chat_id = uuid.uuid4().hex
        turns = random.choice([5, 7, 9])
        
        # Generate conversation
        messages = generate_conversation(person1["sender_name"], person2["sender_name"], turns)
        
        if not messages or len(messages) != turns:
            print(f"⚠️ Skipping pair {i//2 + 1} due to LLM error.")
            continue
        
        # Insert messages into database
        inserted_now = 0
        for msg in messages:
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
                "sender_mail": sender["sender_mail"],
                "message_text": msg["message_text"],
                "timestamp": msg["timestamp"]
            }
            
            chat_col.insert_one(doc)
            inserted_now += 1
        
        total_inserted += inserted_now
        print(f"✅ Pair {i//2 + 1}/{total_pairs} | chat_id: {chat_id} | messages: {inserted_now} | total so far: {total_inserted}")
    
    print(f"\n🎯 Chat message generation complete. Total inserted: {total_inserted}")

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

# Run the streaming generator
if __name__ == "__main__":
    generate_chat_data_streaming()
    
    # Example usage of helper functions:
    # messages = get_messages_by_sender_id("some_sender_id")
    # conversations = get_conversations_by_sender_id("some_sender_id")