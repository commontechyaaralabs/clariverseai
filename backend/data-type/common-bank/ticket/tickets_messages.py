# Banking Trouble Ticket Dataset Generator
import os
import random
import time
import json
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = os.getenv("MONGO_DATABASE_NAME")
TICKET_COLLECTION = "banking_trouble_tickets"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
ticket_col = db[TICKET_COLLECTION]

# OpenRouter API setup (using new OpenAI v1.0+ client)
openai_client = OpenAI(
    api_key=os.getenv("OPENROUTER_API_KEY"),
    base_url="https://openrouter.ai/api/v1"
)

if not os.getenv("OPENROUTER_API_KEY"):
    raise ValueError("OPENROUTER_API_KEY not found in .env")

def generate_ticket_number():
    """Generate a unique ticket number in format: T{YYYYMMDD}.{4-digit-number}"""
    # Random date within last 2 years
    start_date = datetime.now() - timedelta(days=730)
    end_date = datetime.now()
    random_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    
    date_str = random_date.strftime("%Y%m%d")
    ticket_num = random.randint(1000, 9999)
    
    return f"T{date_str}.{ticket_num}"

# Generate banking trouble tickets using LLM
def generate_banking_tickets(batch_size, max_retries=3):
    prompt = f"""
Generate {batch_size} realistic banking trouble tickets for a major bank's IT department.
Each ticket should represent common banking system issues like ATM problems, online banking glitches, 
payment processing errors, security concerns, network issues, etc.

Return JSON array with exactly {batch_size} objects, each having:
- "title": Should follow format like "Ticket Assigned to Your Team - [TICKET-ID]: [Brief Issue Description]"
- "description": Should start with "From [system/email] <email@domain.com>: Hello..." and include detailed technical description with:
  * Problem symptoms
  * Affected systems/users  
  * Error messages (if applicable)
  * Business impact
  * Next steps or escalation info
- "priority": Should be one of: "P1 - Critical", "P2 - High", "P3 - Medium", "P4 - Low", "P5 - Very Low"
  * P1: System down, security breach, major financial impact
  * P2: High business impact, multiple users affected
  * P3: Moderate impact, some users affected
  * P4: Low impact, minor inconvenience
  * P5: Very low impact, cosmetic issues

Make titles concise (60-100 characters) and descriptions detailed (150-300 words).
Use realistic banking technical terminology and system names.

Example format:
- Title: "Ticket Assigned to Your Team - ATM-7845: Card reader malfunction at downtown branch"
- Description: "From \"JIRA automation\" <automation@bankingsystems.com>: Hello team, we have received multiple reports of card reader failures at ATM location DT-001..."
- Priority: "P2 - High"

Return only JSON array of objects like:
[
  {{
    "title": "Ticket Assigned to Your Team - [ID]: [Issue]",
    "description": "From \"[System]\" <email@domain.com>: Hello...",
    "priority": "P3 - Medium"
  }},
  ...
]
"""
    
    for attempt in range(max_retries):
        try:
            # Exponential backoff for rate limiting
            if attempt > 0:
                wait_time = 2 ** attempt  # 2, 4, 8 seconds
                print(f"🔄 Retry attempt {attempt + 1}/{max_retries} - Waiting {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                time.sleep(0.5)  # Normal rate limiting
            
            response = openai_client.chat.completions.create(
                model="google/gemma-3-27b-it:free",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=2500
            )
            
            reply = response.choices[0].message.content.strip()
            
            # Clean up potential markdown formatting
            if reply.startswith("```json"):
                reply = reply[7:]
            if reply.endswith("```"):
                reply = reply[:-3]
            
            return json.loads(reply)
        
        except Exception as e:
            error_str = str(e)
            if "429" in error_str and "rate-limited" in error_str.lower():
                print(f"⚠️ Rate limit hit (attempt {attempt + 1}/{max_retries}): {error_str}")
                if attempt == max_retries - 1:
                    print(f"❌ Max retries reached. Skipping this batch.")
                    return []
                continue
            else:
                print(f"❌ LLM generation error (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt == max_retries - 1:
                    return []
                continue
    
    return []

# Main logic
def generate_banking_ticket_dataset():
    total_tickets = 25000
    batch_size = 10
    
    print(f"🎯 Starting generation of {total_tickets} banking trouble tickets...")
    print(f"📦 Batch size: {batch_size}")
    
    total_batches = (total_tickets + batch_size - 1) // batch_size
    total_inserted = 0
    
    for batch_num in range(1, total_batches + 1):
        # Calculate tickets for this batch (handle remainder)
        tickets_this_batch = min(batch_size, total_tickets - total_inserted)
        
        print(f"🔄 Processing batch {batch_num}/{total_batches} ({tickets_this_batch} tickets)...")
        
        # Generate tickets using LLM
        tickets = generate_banking_tickets(tickets_this_batch)
        
        if not tickets or len(tickets) != tickets_this_batch:
            print(f"⚠ Skipping batch {batch_num} due to LLM error.")
            continue
        
        # Insert tickets into database
        for ticket in tickets:
            doc = {
                "ticket_number": generate_ticket_number(),
                "title": ticket["title"],
                "description": ticket["description"],
                "priority": ticket["priority"],
                "created": datetime.now().strftime("%d/%m/%Y %H:%M")
            }
            
            ticket_col.insert_one(doc)
            total_inserted += 1
        
        print(f"✅ Batch {batch_num} complete | tickets: {len(tickets)} | total so far: {total_inserted}")
        
        # Progress update every 50 batches
        if batch_num % 50 == 0:
            progress = (total_inserted / total_tickets) * 100
            print(f"📊 Progress: {progress:.1f}% ({total_inserted}/{total_tickets})")
    
    print(f"\n🎯 Banking ticket generation complete. Total inserted: {total_inserted}")

# Helper function to query tickets by ticket number
def get_ticket_by_number(ticket_number):
    """Query ticket by ticket number"""
    return ticket_col.find_one({"ticket_number": ticket_number}, {"_id": 0})

# Helper function to search tickets by title
def search_tickets_by_title(search_term):
    """Search tickets by title keyword"""
    tickets = list(ticket_col.find(
        {"title": {"$regex": search_term, "$options": "i"}}, 
        {"_id": 0}
    ).limit(10))
    return tickets

# Helper function to get all tickets
def get_all_tickets():
    """Get all tickets (use with caution for large datasets)"""
    return list(ticket_col.find({}, {"_id": 0}))

# Helper function to get ticket count
def get_ticket_count():
    """Get total number of tickets"""
    return ticket_col.count_documents({})

# Run the ticket generator
if __name__ == "__main__":
    generate_banking_ticket_dataset()
    
    print(f"\n📊 Total tickets in database: {get_ticket_count()}")
    
    # Example usage of helper functions:
    # ticket = get_ticket_by_number("T20250121.0087")
    # atm_tickets = search_tickets_by_title("ATM")
    # all_tickets = get_all_tickets()