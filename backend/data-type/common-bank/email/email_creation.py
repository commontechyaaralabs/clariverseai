# Banking Email Messages Dataset Generator - Improved with Graceful Shutdown
import os
import random
import time
import json
import secrets
import requests
import signal
import sys
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from faker import Faker
import backoff
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import atexit

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
EMAIL_COLLECTION = "llm_email_data"

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
client = None
db = None
email_col = None

# Ollama setup - Optimized settings for remote endpoint
OLLAMA_BASE_URL = "http://34.147.17.26:31100"
OLLAMA_TOKEN = "b805213e7b048d21f02dae5922973e9639ef971b0bc6bf804efad9c707527249"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"
OLLAMA_MODEL = "gemma3:27b"
MAX_CONCURRENT_REQUESTS = 1  # Reduced to 1 to prevent overload
REQUEST_TIMEOUT = 90  # Increased for remote endpoint
MAX_RETRIES = 3  # Reduced retries
RETRY_DELAY = 2  # Reduced retry delay

fake = Faker()

# Thread-safe counters
class Counter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def increment(self):
        with self._lock:
            self._value += 1
            return self._value
    
    @property
    def value(self):
        with self._lock:
            return self._value

success_counter = Counter()
failure_counter = Counter()

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        print(f"\n🛑 Received signal {signum}. Initiating graceful shutdown...")
        shutdown_flag.set()
        print("⏳ Please wait for current operations to complete...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def cleanup_resources():
    """Cleanup database connections and resources"""
    global client
    if client:
        try:
            client.close()
            print("✅ Database connection closed")
        except Exception as e:
            print(f"⚠ Error closing database connection: {e}")

def init_database():
    """Initialize database connection with error handling"""
    global client, db, email_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        db = client[DB_NAME]
        email_col = db[EMAIL_COLLECTION]
        
        # Create indexes for better performance
        email_col.create_index("message_id")
        email_col.create_index("conversation_id") 
        email_col.create_index("sender_id")
        email_col.create_index("timestamp")
        print("✅ Database connection established and indexes created")
        return True
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def generate_hexadecimal_id(length=12):
    """Generate unique hexadecimal code"""
    return secrets.token_hex(length)

def generate_unique_gmail_users(num_records=50000):
    """Generate unique Gmail users in memory"""
    if shutdown_flag.is_set():
        return []
        
    print("📧 Generating unique Gmail users in memory...")
    
    unique_senders = set()
    unique_receivers = set()
    
    def generate_unique_gmail_user(existing_emails):
        while True and not shutdown_flag.is_set():
            name = fake.name()
            base_username = name.lower().replace(" ", "").replace(".", "")
            email = f"{base_username}{fake.random_int(1000,9999)}@gmail.com"
            if email not in existing_emails:
                existing_emails.add(email)
                return name, email
        return None, None
    
    data = []
    while len(data) < num_records and not shutdown_flag.is_set():
        sender_name, sender_email = generate_unique_gmail_user(unique_senders)
        receiver_name, receiver_email = generate_unique_gmail_user(unique_receivers)
        
        if sender_email and receiver_email and sender_email != receiver_email:
            data.append({
                'sender_name': sender_name, 
                'sender_email': sender_email, 
                'receiver_name': receiver_name, 
                'receiver_email': receiver_email
            })
    
    print(f"✅ {len(data)} unique Gmail users generated in memory")
    return data

def generate_random_timestamp():
    """Generate random timestamp in the specified format"""
    start_date = datetime.now() - timedelta(days=730)
    end_date = datetime.now()
    random_date = start_date + timedelta(
        days=random.randint(0, (end_date - start_date).days),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
        seconds=random.randint(0, 59)
    )
    return random_date.strftime("%Y-%m-%dT%H:%M:%SZ")

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=180,  # Reduced max time
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
            "temperature": 0.3,  # Reduced for more consistent output
            "num_predict": 800,   # Increased for longer responses
            "top_k": 20,         # Reduced for faster processing
            "top_p": 0.8,        # Slightly reduced
            "num_ctx": 4096      # Increased context window for longer prompts
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

def generate_context_block_for_email(sender_name, receiver_names):
    """Generate the context block with sender and receiver information"""
    if len(receiver_names) == 1:
        greeting = f"Dear {receiver_names[0]},"
    elif len(receiver_names) == 2:
        greeting = f"Dear {receiver_names[0]} and {receiver_names[1]},"
    else:
        greeting = "Dear All,"
    
    context_block = f"""
**Email Context:**
- Sender: {sender_name}
- Receivers: {', '.join(receiver_names)}
- Required Greeting: "{greeting}"
- Required Signature: "Best regards,\\n{sender_name}"
"""
    return context_block

def generate_simple_banking_email(sender_name, receiver_names):
    """Generate a single banking email with the new detailed prompt"""
    if shutdown_flag.is_set():
        return None
    
    batch_size = 1  # Generate one email at a time
    context_block = generate_context_block_for_email(sender_name, receiver_names)
    
    # Use the exact prompt specified
    prompt = f"""
Generate {batch_size} realistic banking-related email messages for internal bank communications.
These emails should represent various banking scenarios like:

1. **Manager-Employee Communications**: Performance reviews, task assignments, meeting schedules, policy updates
2. **Operational Emails**: Transaction alerts, system maintenance, compliance updates
3. **Customer Service**: Account inquiries, loan applications, complaint resolutions, service requests
4. **Regulatory/Compliance**: Reserve bank communications, regulatory updates, compliance training
5. **Administrative**: Certificate renewals, security updates, staff announcements, training programs
6. **Inter-departmental**: Risk management updates, IT security alerts, financial reporting, branch communications

Each email should be 300-500 words and feel authentic to banking operations.

**AVOID THESE EMAIL TYPES - DO NOT GENERATE:**
- Fraud detection system updates or alerts
- Internal audit notifications or reviews
- Any emails with "Critical:", "Immediate Action Required", "Fraud Detection", "Internal Audit", "Transaction Review" in subject or content
- Emergency security breach notifications
- Suspicious activity alerts

**SUBJECT LINE UNIQUENESS REQUIREMENT:**
- Each of the {batch_size} emails MUST have a completely different subject line
- Avoid similar wording, topics, or patterns in subject lines
- Ensure variety across different banking departments and functions
- Use diverse terminology and avoid repetitive phrases
**FOCUS ON THESE PREFERRED EMAIL TYPES (ENSURE VARIETY):**
- Routine policy updates and training announcements
- Standard customer service communications
- Scheduled system maintenance notifications
- Administrative updates and staff communications
- Regular compliance training and updates
- Branch performance reports and metrics
- Customer account opening/closing procedures
- Loan processing status updates
- Meeting schedules and agenda items

**CRITICAL REQUIREMENTS - MUST FOLLOW EXACTLY:**
{context_block}

**Email Structure Rules:**
- Use the EXACT greeting specified for each email
- If single receiver: "Dear [Receiver Name],"
- If 2 receivers: "Dear [Name1] and [Name2],"  
- If 3+ receivers: "Dear All,"
- End with EXACT sender name: "Best regards,\\n[Sender Name]"

Return JSON array with exactly {batch_size} objects, each having:
- "subject": **UNIQUE** professional banking email subject (50-80 characters) - AVOID prohibited terms and ensure NO DUPLICATES
- "message_text": Detailed email body content (300-500 words) with:
  * EXACT greeting as specified above
  * Clear banking context and purpose
  * Specific banking details/processes
  * Professional tone and terminology
  * EXACT sender signature as specified

Example subjects (PREFERRED TYPES - ALL MUST BE UNIQUE):
- "Q4 Compliance Training - Mandatory Completion by Dec 15th"
- "System Maintenance Alert - Core Banking Platform Downtime"
- "Customer Account Verification Required - Priority Review"
- "RBI Circular Update - New KYC Guidelines Implementation"
- "Branch Performance Review - November 2024 Results"
- "Staff Meeting Schedule - January 2025 Planning Session"
- "Customer Service Training Workshop - Registration Open"
- "Monthly Branch Targets - Q1 2025 Objectives"
- "Holiday Schedule Announcement - Banking Hours Update"
- "New Employee Onboarding Process - HR Guidelines"
- "Loan Documentation Checklist - Updated Requirements"
- "Digital Banking Features - Customer Education Materials"
- "Cash Management Procedures - Daily Reconciliation"
- "Interest Rate Changes - Customer Communication Template"
- "Branch Renovation Update - Temporary Service Adjustments"

**Format Example:**
If Email 1 has sender "John Smith" and receiver "Mary Johnson":
{{
  "subject": "Monthly Performance Review - December 2024",
  "message_text": "Dear Mary Johnson,\\n\\nI hope this message finds you well. I am writing to schedule your monthly performance review for December 2024...\\n\\n[300-500 words of banking content]\\n\\nPlease let me know your availability for next week.\\n\\nBest regards,\\nJohn Smith"
}}

**IMPORTANT**: 
- Follow the exact sender and receiver names provided above. Do not use generic names.
- Strictly avoid any fraud detection, internal audit, or emergency alert content
- Focus on routine, positive, and standard banking operations
- **ENSURE ALL EMAIL SUBJECTS ARE COMPLETELY UNIQUE** - No repeated or similar subject lines
- Vary the email types and topics to maintain diversity across all {batch_size} emails

Return only JSON array (maintain exact order as listed above):
[
  {{
    "subject": "Email subject line here",
    "message_text": "[EXACT greeting], [Email body content], [EXACT signature]"
  }},
  ...
]
""".strip()

    try:
        response = call_ollama_with_backoff(prompt)
        
        if not response or not response.strip():
            print("⚠ Empty response from API")
            raise ValueError("Empty response from LLM")
        
        # Clean response more aggressively
        reply = response.strip()
        
        # Remove markdown formatting
        if "```" in reply:
            reply = reply.replace("```json", "").replace("```", "")
        
        # Find JSON array
        json_start = reply.find('[')
        json_end = reply.rfind(']') + 1
        
        if json_start == -1 or json_end <= json_start:
            # Try to find single JSON object instead
            json_start = reply.find('{')
            json_end = reply.rfind('}') + 1
            
            if json_start == -1 or json_end <= json_start:
                print("⚠ No valid JSON found in response")
                raise ValueError("No valid JSON found in LLM response")
            
            reply = reply[json_start:json_end]
            
            try:
                single_result = json.loads(reply)
                # Validate required fields
                if "subject" not in single_result or "message_text" not in single_result:
                    print("⚠ Missing required fields in JSON")
                    raise ValueError("Missing required fields in JSON response")
                return single_result
            except json.JSONDecodeError as e:
                print(f"⚠ JSON parsing failed: {e}")
                raise ValueError(f"JSON parsing failed: {e}")
        
        reply = reply[json_start:json_end]
        
        try:
            result_array = json.loads(reply)
            if isinstance(result_array, list) and len(result_array) > 0:
                # Return the first email from the array
                email_data = result_array[0]
                
                # Validate required fields
                if "subject" not in email_data or "message_text" not in email_data:
                    print("⚠ Missing required fields in JSON")
                    raise ValueError("Missing required fields in JSON response")
                
                return email_data
            else:
                print("⚠ Invalid array format")
                raise ValueError("Invalid array format in LLM response")
                
        except json.JSONDecodeError as e:
            print(f"⚠ JSON parsing failed: {e}")
            raise ValueError(f"JSON parsing failed: {e}")
        
    except KeyboardInterrupt:
        print("🛑 Email generation interrupted")
        return None
    except Exception as e:
        print(f"⚠ Email generation failed: {str(e)[:50]}...")
        # Let the backoff decorator in call_ollama_with_backoff handle retries
        raise

def process_single_email_task(task_data):
    """Process a single email generation task"""
    if shutdown_flag.is_set():
        return None
        
    try:
        sender_user, receiver_names, receiver_ids = task_data
        
        # Generate email content
        email_content = generate_simple_banking_email(
            sender_user['sender_name'], 
            receiver_names
        )
        
        if not email_content:
            failure_counter.increment()
            return None
        
        # Create document
        doc = {
            "message_id": generate_hexadecimal_id(16),
            "conversation_id": generate_hexadecimal_id(12),
            "sender_id": sender_user['sender_email'],
            "sender_name": sender_user['sender_name'],
            "receiver_ids": receiver_ids,
            "receiver_names": receiver_names,
            "timestamp": generate_random_timestamp(),
            "subject": email_content['subject'],
            "message_text": email_content['message_text']
        }
        
        success_counter.increment()
        return doc
        
    except Exception as e:
        print(f"❌ Task processing error: {str(e)}")
        failure_counter.increment()
        return None

def generate_banking_email_dataset_parallel():
    """Generate banking email dataset using parallel processing with graceful shutdown"""
    total_emails = 25000
    batch_size = 10  # Smaller batches for better control
    
    print(f"🎯 Starting generation of {total_emails} banking email messages...")
    print(f"📦 Batch size: {batch_size}")
    print(f"🧵 Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")
    print(f"⏰ Request timeout: {REQUEST_TIMEOUT}s")
    print(f"🔄 Max retries per request: {MAX_RETRIES}")
    
    # Test Ollama connection
    if not test_ollama_connection():
        print("❌ Cannot proceed without Ollama connection")
        return
    
    # Check current count and adjust target
    current_count = get_email_count()
    if current_count > 0:
        remaining = total_emails - current_count
        print(f"📊 Found {current_count} existing emails, generating {remaining} more")
        total_emails = remaining
    
    if total_emails <= 0:
        print("✅ Target already achieved!")
        return
    
    # Generate Gmail users
    users_data = generate_unique_gmail_users()
    if not users_data or shutdown_flag.is_set():
        print("❌ User generation failed or interrupted")
        return
        
    print(f"👥 Generated {len(users_data)} unique user pairs")
    
    total_batches = (total_emails + batch_size - 1) // batch_size
    total_inserted = 0
    consecutive_failures = 0
    
    try:
        for batch_num in range(1, total_batches + 1):
            if shutdown_flag.is_set():
                print(f"\n🛑 Shutdown requested. Stopping at batch {batch_num-1}/{total_batches}")
                break
                
            emails_this_batch = min(batch_size, total_emails - total_inserted)
            print(f"\n🔄 Processing batch {batch_num}/{total_batches} ({emails_this_batch} emails)...")
            
            # Prepare tasks for this batch
            tasks = []
            for _ in range(emails_this_batch):
                if shutdown_flag.is_set():
                    break
                    
                sender_user = random.choice(users_data)
                num_receivers = random.randint(1, 3)  # Reduced max receivers
                all_users = [u for u in users_data if u['sender_email'] != sender_user['sender_email']]
                receivers = random.sample(all_users, min(num_receivers, len(all_users)))
                receiver_names = [r['receiver_name'] for r in receivers]
                receiver_ids = [r['receiver_email'] for r in receivers]
                
                tasks.append((sender_user, receiver_names, receiver_ids))
            
            if not tasks:
                break
            
            # Process tasks sequentially if concurrent requests = 1, otherwise use ThreadPoolExecutor
            successful_docs = []
            
            if MAX_CONCURRENT_REQUESTS == 1:
                # Sequential processing for better stability
                for i, task in enumerate(tasks):
                    if shutdown_flag.is_set():
                        break
                        
                    print(f"  📧 Generating email {i+1}/{len(tasks)}...", end=" ")
                    doc = process_single_email_task(task)
                    if doc:
                        successful_docs.append(doc)
                        print("✅")
                    else:
                        print("❌")
                    
                    # Small delay between requests
                    if not shutdown_flag.is_set():
                        time.sleep(1)
            else:
                # Parallel processing
                with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_REQUESTS) as executor:
                    futures = []
                    for i, task in enumerate(tasks):
                        if shutdown_flag.is_set():
                            break
                            
                        future = executor.submit(process_single_email_task, task)
                        futures.append(future)
                        
                        # Stagger submissions more
                        if i % MAX_CONCURRENT_REQUESTS == 0 and i > 0:
                            time.sleep(3)
                    
                    # Collect results with timeout
                    try:
                        for future in as_completed(futures, timeout=REQUEST_TIMEOUT * 2):
                            if shutdown_flag.is_set():
                                print("🛑 Cancelling remaining tasks...")
                                for f in futures:
                                    f.cancel()
                                break
                                
                            doc = future.result(timeout=10)
                            if doc:
                                successful_docs.append(doc)
                                
                    except Exception as e:
                        print(f"⚠ Error collecting results: {e}")
            
            # Insert successful documents
            if successful_docs and not shutdown_flag.is_set():
                try:
                    email_col.insert_many(successful_docs)
                    total_inserted += len(successful_docs)
                    consecutive_failures = 0  # Reset failure counter
                    print(f"✅ Batch {batch_num} complete | Successful: {len(successful_docs)}/{emails_this_batch} | Total: {total_inserted}")
                except Exception as e:
                    print(f"❌ Database insertion error: {e}")
                    consecutive_failures += 1
            elif shutdown_flag.is_set():
                print(f"🛑 Batch {batch_num} interrupted - {len(successful_docs)} emails generated but not saved")
                break
            else:
                print(f"⚠ Batch {batch_num} failed - no successful emails generated")
                consecutive_failures += 1
            
            # Check for too many consecutive failures
            if consecutive_failures >= 5:
                print(f"❌ Too many consecutive failures ({consecutive_failures}). Stopping generation.")
                break
            
            # Progress update
            if batch_num % 10 == 0:
                current_total = get_email_count()
                progress = (current_total / 25000) * 100
                print(f"📊 Progress: {progress:.1f}% | Current total: {current_total} | Success: {success_counter.value} | Failures: {failure_counter.value}")
            
            # Longer pause between batches to prevent overload
            if not shutdown_flag.is_set():
                time.sleep(5 if consecutive_failures > 0 else 3)
        
        if shutdown_flag.is_set():
            print(f"\n🛑 Generation interrupted gracefully!")
        else:
            print(f"\n🎯 Banking email generation complete!")
            
        current_total = get_email_count()
        print(f"📈 Total in database: {current_total}")
        print(f"✅ Successful generations this session: {success_counter.value}")
        print(f"❌ Failed generations this session: {failure_counter.value}")
        print(f"💾 Data stored in MongoDB: {DB_NAME}.{EMAIL_COLLECTION}")
        
    except KeyboardInterrupt:
        print(f"\n🛑 Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        shutdown_flag.set()

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
        
        # Test basic connection
        response = requests.get(OLLAMA_TAGS_URL, headers=headers, timeout=15)
        print(f"📡 Tags endpoint status: {response.status_code}")
        
        if not response.text.strip():
            print("⚠ Empty response from tags endpoint, trying basic connection test...")
            # Try a simple generation test instead
            return test_simple_generation()
        
        response.raise_for_status()
        
        try:
            models = response.json()
        except json.JSONDecodeError:
            print("⚠ Tags endpoint returned non-JSON, trying simple generation test...")
            return test_simple_generation()
        
        if 'models' in models:
            model_names = [model['name'] for model in models.get('models', [])]
            if OLLAMA_MODEL in model_names:
                print(f"✅ Remote Ollama is running and {OLLAMA_MODEL} is available")
                return test_simple_generation()
            else:
                print(f"❌ Model {OLLAMA_MODEL} not found. Available: {model_names}")
                return False
        else:
            print("⚠ Unexpected response format, trying simple generation test...")
            return test_simple_generation()
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to remote Ollama: {e}")
        print("💡 Check the remote endpoint URL and token")
        return False

def test_simple_generation():
    """Test simple generation to verify the endpoint works"""
    try:
        print("🧪 Testing simple generation...")
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {OLLAMA_TOKEN}' if OLLAMA_TOKEN else None
        }
        headers = {k: v for k, v in headers.items() if v is not None}
        
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
                print("✅ Simple generation test successful")
                print(f"🔍 Test response: {result['response'][:50]}...")
                return True
            else:
                print(f"❌ No 'response' field in test. Fields: {list(result.keys())}")
                return False
        except json.JSONDecodeError as e:
            print(f"❌ Generation test returned invalid JSON: {e}")
            print(f"📄 Raw response: {test_response.text[:200]}...")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Simple generation test failed: {e}")
        return False

# Helper functions for querying the dataset
def get_email_by_message_id(message_id):
    """Query email by message ID"""
    return email_col.find_one({"message_id": message_id}, {"_id": 0})

def get_emails_by_conversation_id(conversation_id):
    """Get all emails in a conversation"""
    return list(email_col.find({"conversation_id": conversation_id}, {"_id": 0}))

def search_emails_by_subject(search_term):
    """Search emails by subject keyword"""
    emails = list(email_col.find(
        {"subject": {"$regex": search_term, "$options": "i"}}, 
        {"_id": 0}
    ).limit(10))
    return emails

def get_emails_by_sender(sender_id):
    """Get all emails from a specific sender"""
    return list(email_col.find({"sender_id": sender_id}, {"_id": 0}))

def get_emails_in_date_range(start_date, end_date):
    """Get emails within date range"""
    return list(email_col.find({
        "timestamp": {
            "$gte": start_date,
            "$lte": end_date
        }
    }, {"_id": 0}))

def get_email_count():
    """Get total number of emails"""
    return email_col.count_documents({})

def get_sample_emails(limit=5):
    """Get sample emails for testing"""
    return list(email_col.find({}, {"_id": 0}).limit(limit))

# Database statistics
def print_dataset_stats():
    """Print dataset statistics"""
    total_count = get_email_count()
    print(f"\n📊 Dataset Statistics:")
    print(f"Total emails: {total_count}")
    
    # Get unique senders count
    unique_senders = email_col.distinct("sender_id")
    print(f"Unique senders: {len(unique_senders)}")
    
    # Get sample subjects
    sample_subjects = list(email_col.find({}, {"subject": 1, "_id": 0}).limit(5))
    print(f"\nSample subjects:")
    for i, subj in enumerate(sample_subjects, 1):
        print(f"{i}. {subj['subject']}")

# Main execution function
def main():
    """Main function to initialize and run the email generator"""
    print("🏦 Banking Email Dataset Generator Starting... (Remote Ollama Version)")
    print(f"🎯 Target: 25,000 banking emails")
    print(f"💾 Database: {DB_NAME}")
    print(f"📂 Collection: {EMAIL_COLLECTION}")
    print(f"🤖 Model: {OLLAMA_MODEL}")
    print(f"🔗 Ollama URL: {OLLAMA_BASE_URL}")
    
    # Setup signal handlers and cleanup
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    # Initialize database
    if not init_database():
        print("❌ Cannot proceed without database connection")
        return
    
    try:
        # Run the email generation
        generate_banking_email_dataset_parallel()
        
        # Print final statistics
        print_dataset_stats()
        
        # Example usage information
        print(f"\n🔍 Example queries:")
        print("- get_email_by_message_id('message_id_here')")
        print("- search_emails_by_subject('compliance')")
        print("- get_emails_by_sender('sender@gmail.com')")
        print("- get_sample_emails(3)")
        
        # Show sample emails
        print(f"\n📧 Sample generated emails from {DB_NAME}.{EMAIL_COLLECTION}:")
        samples = get_sample_emails(2)
        for i, email in enumerate(samples, 1):
            print(f"\n--- Sample Email {i} ---")
            print(f"Message ID: {email['message_id']}")
            print(f"Conversation ID: {email['conversation_id']}")
            print(f"Subject: {email['subject']}")
            print(f"From: {email['sender_name']} ({email['sender_id']})")
            print(f"To: {', '.join(email['receiver_names'])}")
            print(f"Timestamp: {email['timestamp']}")
            print(f"Message Preview: {email['message_text'][:150]}...")
            
    except KeyboardInterrupt:
        print(f"\n🛑 Generation interrupted by user!")
    except Exception as e:
        print(f"\n❌ Unexpected error in main: {e}")
    finally:
        cleanup_resources()

# Legacy functions for backward compatibility
def generate_banking_email_dataset():
    """Legacy function - calls the new parallel version"""
    return generate_banking_email_dataset_parallel()

def create_final_email_document(email_content, email_data):
    """Create final email document with all required fields"""
    return {
        "message_id": generate_hexadecimal_id(16),
        "conversation_id": generate_hexadecimal_id(12),
        "sender_id": email_data['sender_user']['sender_email'],
        "sender_name": email_data['sender_user']['sender_name'],
        "receiver_ids": email_data['receiver_ids'],
        "receiver_names": email_data['receiver_names'],
        "timestamp": generate_random_timestamp(),
        "subject": email_content['subject'],
        "message_text": email_content['message_text']
    }

def create_email_document_with_sender(email_data, sender_user, users_data):
    """Create email document with specific sender and all required fields"""
    # Generate receiver data (1-4 receivers)
    num_receivers = random.randint(1, 4)
    all_users = users_data.copy()
    # Remove sender from potential receivers
    all_users = [u for u in all_users if u['sender_email'] != sender_user['sender_email']]
    
    receivers = random.sample(all_users, min(num_receivers, len(all_users)))
    
    receiver_ids = [r['receiver_email'] for r in receivers]
    receiver_names = [r['receiver_name'] for r in receivers]
    
    return {
        "message_id": generate_hexadecimal_id(16),
        "conversation_id": generate_hexadecimal_id(12),
        "sender_id": sender_user['sender_email'],
        "sender_name": sender_user['sender_name'],
        "receiver_ids": receiver_ids,
        "receiver_names": receiver_names,
        "timestamp": generate_random_timestamp(),
        "subject": email_data['subject'],
        "message_text": email_data['message_text']
    }

def create_email_document(email_data, users_data):
    """Create email document with all required fields (legacy function)"""
    # Select random users
    user_pair = random.choice(users_data)
    
    # Generate receiver data (1-4 receivers)
    num_receivers = random.randint(1, 4)
    all_users = users_data.copy()
    # Remove sender from potential receivers
    all_users = [u for u in all_users if u['sender_email'] != user_pair['sender_email']]
    
    receivers = random.sample(all_users, min(num_receivers, len(all_users)))
    
    receiver_ids = [r['receiver_email'] for r in receivers]
    receiver_names = [r['receiver_name'] for r in receivers]
    
    return {
        "message_id": generate_hexadecimal_id(16),
        "conversation_id": generate_hexadecimal_id(12),
        "sender_id": user_pair['sender_email'],
        "sender_name": user_pair['sender_name'],
        "receiver_ids": receiver_ids,
        "receiver_names": receiver_names,
        "timestamp": generate_random_timestamp(),
        "subject": email_data['subject'],
        "message_text": email_data['message_text']
    }

# Run the email generator
if __name__ == "__main__":
    main()