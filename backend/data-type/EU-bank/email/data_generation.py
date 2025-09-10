# EU Banking Email Content Generator - Fixed Version
import os
import random
import time
import json
import requests
import signal
import sys
import multiprocessing
import logging
from datetime import datetime, timedelta
from pymongo import MongoClient
from dotenv import load_dotenv
from faker import Faker
import backoff
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import threading
from queue import Queue
import atexit
import psutil
from pathlib import Path
from pymongo import UpdateOne

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
EMAIL_COLLECTION = "emailmessages"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"email_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"progress_{timestamp}.log"

# Configure main logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(MAIN_LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ]
)

# Configure specialized loggers
success_logger = logging.getLogger('success')
success_logger.setLevel(logging.INFO)
success_handler = logging.FileHandler(SUCCESS_LOG_FILE)
success_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
success_logger.addHandler(success_handler)
success_logger.propagate = False

failure_logger = logging.getLogger('failure')
failure_logger.setLevel(logging.ERROR)
failure_handler = logging.FileHandler(FAILURE_LOG_FILE)
failure_handler.setFormatter(logging.Formatter('%(asctime)s - ERROR - %(message)s'))
failure_logger.addHandler(failure_handler)
failure_logger.propagate = False

progress_logger = logging.getLogger('progress')
progress_logger.setLevel(logging.INFO)
progress_handler = logging.FileHandler(PROGRESS_LOG_FILE)
progress_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
progress_logger.addHandler(progress_handler)
progress_logger.propagate = False

logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
client = None
db = None
email_col = None

# Ollama setup - Optimized for maximum performance
OLLAMA_BASE_URL = "http://34.147.17.26:30407"
OLLAMA_TOKEN = "d4a1e31495a719806db6c941dbd27bf01c252ed9fdb44c3003ae4d5d253d6ad4"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"
OLLAMA_MODEL = "gemma3:27b"

# Optimized batch processing configuration
BATCH_SIZE = 10  # Process 10 records per batch as requested
CPU_COUNT = multiprocessing.cpu_count()
MAX_WORKERS = min(CPU_COUNT * 2, 16)  # Reduced for stability with batch size of 10
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_DELAY = 1

# Intermediate results storage
INTERMEDIATE_RESULTS_FILE = LOG_DIR / f"intermediate_results_{timestamp}.json"

fake = Faker()

# Thread-safe counters with logging
class LoggingCounter:
    def __init__(self, name):
        self._value = 0
        self._lock = threading.Lock()
        self._name = name
    
    def increment(self):
        with self._lock:
            self._value += 1
            progress_logger.info(f"{self._name}: {self._value}")
            return self._value
    
    @property
    def value(self):
        with self._lock:
            return self._value

success_counter = LoggingCounter("SUCCESS_COUNT")
failure_counter = LoggingCounter("FAILURE_COUNT")
update_counter = LoggingCounter("UPDATE_COUNT")

class IntermediateResultsManager:
    """Manages saving and loading of intermediate results"""
    
    def __init__(self, filename):
        self.filename = filename
        self.results = []
        self._lock = threading.Lock()
        self.load_existing_results()
    
    def load_existing_results(self):
        """Load existing intermediate results if file exists"""
        try:
            if self.filename.exists():
                with open(self.filename, 'r') as f:
                    self.results = json.load(f)
                logger.info(f"Loaded {len(self.results)} existing intermediate results")
        except Exception as e:
            logger.error(f"Error loading intermediate results: {e}")
            self.results = []
    
    def add_result(self, result):
        """Add a result to intermediate storage"""
        with self._lock:
            result['timestamp'] = datetime.now().isoformat()
            self.results.append(result)
            self.save_to_file()
    
    def add_batch_results(self, results_batch):
        """Add multiple results to intermediate storage"""
        with self._lock:
            for result in results_batch:
                result['timestamp'] = datetime.now().isoformat()
            self.results.extend(results_batch)
            self.save_to_file()
            logger.info(f"Added {len(results_batch)} results to intermediate storage")
    
    def save_to_file(self):
        """Save current results to file"""
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.results, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving intermediate results: {e}")
    
    def get_pending_updates(self):
        """Get results that haven't been saved to database yet"""
        return [r for r in self.results if not r.get('saved_to_db', False)]
    
    def mark_as_saved(self, message_ids):
        """Mark results as saved to database"""
        with self._lock:
            for result in self.results:
                if result.get('message_id') in message_ids:
                    result['saved_to_db'] = True
            self.save_to_file()

# Initialize intermediate results manager
results_manager = IntermediateResultsManager(INTERMEDIATE_RESULTS_FILE)

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        shutdown_flag.set()
        logger.info("Please wait for current operations to complete...")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

def cleanup_resources():
    """Cleanup database connections and resources"""
    global client
    if client:
        try:
            client.close()
            logger.info("Database connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {e}")

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
        email_col.create_index("dominant_topic")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def generate_realistic_banking_details():
    """Generate realistic banking details for use in emails"""
    details = {
        'account_number': f"{random.randint(100000000000, 999999999999)}",
        'sort_code': f"{random.randint(10, 99)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
        'swift_code': f"{random.choice(['ABNA', 'DEUT', 'BNPA', 'CITI', 'HSBC', 'BARC'])}{random.choice(['GB', 'DE', 'FR', 'NL', 'IT'])}2{random.choice(['L', 'X'])}{random.randint(100, 999)}",
        'iban': f"{random.choice(['GB', 'DE', 'FR', 'NL', 'IT'])}{random.randint(10, 99)} {random.choice(['ABNA', 'DEUT', 'BNPA'])} {random.randint(1000, 9999)} {random.randint(1000, 9999)} {random.randint(10, 99)}",
        'reference_number': f"REF{random.randint(100000, 999999)}",
        'transaction_id': f"TXN{random.randint(10000000, 99999999)}",
        'amount': f"{random.randint(100, 50000)}.{random.randint(10, 99)}",
        'currency': random.choice(['EUR', 'GBP', 'USD', 'CHF']),
        'branch_code': f"BR{random.randint(1000, 9999)}",
        'customer_id': f"CID{random.randint(100000, 999999)}",
        'loan_number': f"LN{random.randint(1000000, 9999999)}",
        'card_number': f"**** **** **** {random.randint(1000, 9999)}",
        'date': fake.date_between(start_date='-30d', end_date='today').strftime('%d/%m/%Y'),
        'time': f"{random.randint(9, 17)}:{random.randint(10, 59)}"
    }
    return details

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=180,
    base=RETRY_DELAY,
    on_backoff=lambda details: logger.warning(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
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
            "temperature": 0.4,
            "num_predict": 1000,
            "top_k": 30,
            "top_p": 0.9,
            "num_ctx": 6144
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
            logger.error(f"JSON decode error. Response text: {response.text[:200]}...")
            raise
        
        if "response" not in result:
            logger.error(f"No 'response' field. Available fields: {list(result.keys())}")
            raise KeyError("No 'response' field in Ollama response")
            
        return result["response"]
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out after {timeout} seconds")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - check remote Ollama endpoint")
        raise
    except requests.exceptions.HTTPError as e:
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text[:200]}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"Ollama API error: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response: {e}")
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"API response error: {e}")
        raise

def generate_eu_banking_email_content(email_data):
    """Generate EU banking email content based on dominant topic and subtopics"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    message_id = email_data.get('message_id', 'unknown')
    
    try:
        # Extract data from email record
        sender_name = email_data.get('sender_name', 'Unknown Sender')
        receiver_names = email_data.get('receiver_names', ['Dear Colleague'])
        dominant_topic = email_data.get('dominant_topic', 'General Banking')
        subtopics = email_data.get('subtopics', 'General operations')
        
        # Generate realistic banking details
        banking_details = generate_realistic_banking_details()
        
        # Generate appropriate greeting
        if len(receiver_names) == 1:
            greeting = f"Dear {receiver_names[0]},"
        elif len(receiver_names) == 2:
            greeting = f"Dear {receiver_names[0]} and {receiver_names[1]},"
        else:
            greeting = "Dear All,"
        
        # Enhanced prompt for EU banking context with realistic details
        prompt = f"""
Generate a professional EU banking email with subject and message body based on the following context:

**Email Context:**
- Sender: {sender_name}
- Receivers: {', '.join(receiver_names)}
- Required Greeting: "{greeting}"
- Required Signature: "Best regards,\\n{sender_name}"
- Dominant Topic: {dominant_topic}
- Subtopics: {subtopics}

**Banking Details to Use (when relevant to the topic):**
- Account Number: {banking_details['account_number']}
- Sort Code: {banking_details['sort_code']}
- SWIFT Code: {banking_details['swift_code']}
- IBAN: {banking_details['iban']}
- Reference Number: {banking_details['reference_number']}
- Transaction ID: {banking_details['transaction_id']}
- Amount: {banking_details['currency']} {banking_details['amount']}
- Branch Code: {banking_details['branch_code']}
- Customer ID: {banking_details['customer_id']}
- Loan Number: {banking_details['loan_number']}
- Card Number: {banking_details['card_number']}
- Date: {banking_details['date']}
- Time: {banking_details['time']}

**CRITICAL INSTRUCTIONS FOR BANKING DETAILS:**
- NEVER use placeholders like [Account Number], [Amount], or [Reference Number]
- ALWAYS use the specific banking details provided above when relevant to the email content
- Make the details feel natural and integrated into the email context
- Use realistic European banking terminology and formats
- Include specific details that make the email authentic and professional

**EU Banking Compliance Requirements:**
- Follow GDPR data protection principles
- Reference EU banking regulations (CRD IV, MiFID II, PSD2) where relevant
- Include EBA (European Banking Authority) guidelines if applicable
- Consider ECB (European Central Bank) directives
- Mention SEPA (Single Euro Payments Area) for payment-related topics
- Reference Basel III capital requirements where appropriate

**Email Generation Instructions:**
1. Create a professional subject line (50-80 characters) that reflects the dominant topic
2. Generate email body (300-450 words) that:
   - Uses the EXACT greeting specified above
   - Incorporates the dominant topic as the main theme
   - Weaves in the subtopics naturally throughout the content
   - Uses SPECIFIC banking details from the list above (NO placeholders)
   - Maintains EU banking context and terminology
   - Includes specific EU regulatory references where relevant
   - Uses professional, authoritative tone
   - Includes actionable items or requests where appropriate
   - Creates urgency if the topic requires immediate attention
   - Ends with the EXACT signature specified above
   - Makes banking details feel natural and contextual

**Examples of proper detail usage:**
- "Please reference transaction ID {banking_details['transaction_id']} for this transfer"
- "The amount of {banking_details['currency']} {banking_details['amount']} has been processed"
- "Your account {banking_details['account_number']} shows the following activity"
- "Please contact our branch (Code: {banking_details['branch_code']}) for assistance"

**Output Format:**
Return ONLY a JSON object with these exact fields:
{{
  "subject": "Professional email subject here",
  "message_text": "{greeting}\\n\\n[Email body content with REAL banking details, not placeholders]\\n\\nBest regards,\\n{sender_name}",
  "is_urgent": true/false
}}

Generate the email content now with REALISTIC banking details integrated naturally into the content.
""".strip()

        response = call_ollama_with_backoff(prompt)
        
        if not response or not response.strip():
            raise ValueError("Empty response from LLM")
        
        # Clean response
        reply = response.strip()
        
        # Remove markdown formatting
        if "```" in reply:
            reply = reply.replace("```json", "").replace("```", "")
        
        # Find JSON object
        json_start = reply.find('{')
        json_end = reply.rfind('}') + 1
        
        if json_start == -1 or json_end <= json_start:
            raise ValueError("No valid JSON found in LLM response")
        
        reply = reply[json_start:json_end]
        
        try:
            result = json.loads(reply)
            
            # Validate required fields
            required_fields = ['subject', 'message_text']
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            # Add default urgency if not specified
            if 'is_urgent' not in result:
                result['is_urgent'] = False
            
            # Validate that no placeholders exist in the content
            content_to_check = result['subject'] + result['message_text']
            placeholder_indicators = ['[', ']', 'placeholder', 'PLACEHOLDER', 'example', 'EXAMPLE']
            
            for indicator in placeholder_indicators:
                if indicator in content_to_check:
                    logger.warning(f"Potential placeholder detected in content for {message_id}")
                    break
            
            generation_time = time.time() - start_time
            
            # Log successful generation
            success_info = {
                'message_id': message_id,
                'sender_name': sender_name,
                'dominant_topic': dominant_topic,
                'subject': result['subject'],
                'generation_time': generation_time,
                'is_urgent': result['is_urgent']
            }
            success_logger.info(json.dumps(success_info))
            
            return result
            
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'message_id': message_id,
            'sender_name': email_data.get('sender_name', 'Unknown'),
            'dominant_topic': email_data.get('dominant_topic', 'Unknown'),
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_email_update(email_record):
    """Process a single email record to generate subject and message_text"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate email content based on existing data
        email_content = generate_eu_banking_email_content(email_record)
        
        if not email_content:
            failure_counter.increment()
            return None
        
        # Prepare update document - ONLY subject and message_text
        update_doc = {
            "subject": email_content['subject'],
            "message_text": email_content['message_text']
        }
        
        # Add urgency flag if present
        if 'is_urgent' in email_content:
            update_doc['is_urgent'] = email_content['is_urgent']
        
        success_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'message_id': email_record['message_id'],
            'update_doc': update_doc,
            'original_data': {
                'sender_name': email_record.get('sender_name'),
                'dominant_topic': email_record.get('dominant_topic'),
                'subtopics': email_record.get('subtopics', '')[:100] + '...' if len(str(email_record.get('subtopics', ''))) > 100 else email_record.get('subtopics', '')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'message_id': email_record['message_id'],
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {email_record.get('message_id', 'unknown')}: {str(e)[:100]}")
        failure_counter.increment()
        return None

def save_batch_to_database(batch_updates):
    """Save a batch of updates to the database using proper bulk write operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create proper UpdateOne operations
        bulk_operations = []
        message_ids = []
        
        for update_data in batch_updates:
            # Create UpdateOne operation properly
            operation = UpdateOne(
                filter={"message_id": update_data['message_id']},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            message_ids.append(update_data['message_id'])
        
        # Execute bulk write with proper error handling
        if bulk_operations:
            try:
                result = email_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Mark intermediate results as saved
                results_manager.mark_as_saved(message_ids)
                
                # Update counter
                update_counter._value += updated_count
                
                logger.info(f"Successfully saved {updated_count} records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved, total_updates: {update_counter.value}")
                
                # Log some details of what was saved
                if updated_count > 0:
                    sample_update = batch_updates[0]
                    logger.info(f"Sample update - ID: {sample_update['message_id']}, Subject: {sample_update['update_doc']['subject'][:50]}...")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Try individual updates as fallback
                logger.info("Attempting individual updates as fallback...")
                individual_success = 0
                
                for update_data in batch_updates:
                    try:
                        result = email_col.update_one(
                            {"message_id": update_data['message_id']},
                            {"$set": update_data['update_doc']}
                        )
                        if result.modified_count > 0:
                            individual_success += 1
                    except Exception as individual_error:
                        logger.error(f"Individual update failed for {update_data['message_id']}: {individual_error}")
                
                if individual_success > 0:
                    results_manager.mark_as_saved([up['message_id'] for up in batch_updates[:individual_success]])
                    update_counter._value += individual_success
                    logger.info(f"Fallback: {individual_success} records saved individually")
                
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def update_emails_with_content_parallel():
    """Update existing emails with generated subject and message_text using optimized batch processing"""
    
    logger.info("Starting EU Banking Email Content Generation...")
    logger.info(f"System Info: {CPU_COUNT} CPU cores detected")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Request timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"Max retries per request: {MAX_RETRIES}")
    
    # Test Ollama connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        return
    
    # Get all email records that need content generation
    logger.info("Fetching email records from database...")
    try:
        # Query for emails that don't have subject and message_text fields
        query = {
            "$or": [
                {"subject": {"$exists": False}},
                {"message_text": {"$exists": False}},
                {"subject": {"$in": [None, ""]}},
                {"message_text": {"$in": [None, ""]}}
            ]
        }
        
        email_records = list(email_col.find(query))
        total_emails = len(email_records)
        
        if total_emails == 0:
            logger.info("All emails already have subject and message_text!")
            return
            
        logger.info(f"Found {total_emails} emails needing content generation")
        progress_logger.info(f"BATCH_START: total_emails={total_emails}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching email records: {e}")
        return
    
    # Process in batches of BATCH_SIZE (10)
    total_batches = (total_emails + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []  # Accumulate updates for batch saving
    
    logger.info(f"Processing in {total_batches} batches of {BATCH_SIZE} emails each")
    
    try:
        for batch_num in range(1, total_batches + 1):
            if shutdown_flag.is_set():
                logger.info(f"Shutdown requested. Stopping at batch {batch_num-1}/{total_batches}")
                break
                
            batch_start = (batch_num - 1) * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_emails)
            batch_records = email_records[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_records)} emails)...")
            progress_logger.info(f"BATCH_START: batch={batch_num}/{total_batches}, records={len(batch_records)}")
            
            # Process batch with parallelization
            successful_updates = []
            batch_start_time = time.time()
            
            # Use ThreadPoolExecutor for I/O bound operations (API calls)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks for this batch
                futures = {
                    executor.submit(process_single_email_update, record): record 
                    for record in batch_records
                }
                
                # Collect results with progress tracking
                completed = 0
                try:
                    for future in as_completed(futures, timeout=REQUEST_TIMEOUT * 2):
                        if shutdown_flag.is_set():
                            logger.warning("Cancelling remaining tasks...")
                            for f in futures:
                                f.cancel()
                            break
                            
                        try:
                            result = future.result(timeout=30)
                            completed += 1
                            
                            if result:
                                successful_updates.append(result)
                            
                            # Progress indicator
                            if completed % 5 == 0:
                                progress = (completed / len(batch_records)) * 100
                                logger.info(f"Batch progress: {progress:.1f}% ({completed}/{len(batch_records)})")
                                
                        except Exception as e:
                            logger.error(f"Error processing future result: {e}")
                            completed += 1
                            
                except Exception as e:
                    logger.error(f"Error collecting batch results: {e}")
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # Add successful updates to accumulator
            batch_updates.extend(successful_updates)
            
            logger.info(f"Batch {batch_num} processing complete: {len(successful_updates)}/{len(batch_records)} successful")
            logger.info(f"Batch duration: {batch_duration:.2f}s")
            progress_logger.info(f"BATCH_COMPLETE: batch={batch_num}, successful={len(successful_updates)}, duration={batch_duration:.2f}s")
            
            # Save to database every 10 records (one batch)
            if len(batch_updates) >= BATCH_SIZE and not shutdown_flag.is_set():
                saved_count = save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear the accumulator
                
                logger.info(f"Database update complete: {saved_count} records saved")
            
            # Progress summary every 5 batches
            if batch_num % 5 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_emails) * 100
                logger.info(f"Overall Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value} | DB Updates: {total_updated}")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, success={success_counter.value}, failures={failure_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches
            if not shutdown_flag.is_set() and batch_num < total_batches:
                time.sleep(0.5)
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
            logger.info(f"Final batch saved: {saved_count} records")
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking email content generation complete!")
            
        logger.info(f"Total emails updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        logger.info(f"Data updated in MongoDB: {DB_NAME}.{EMAIL_COLLECTION}")
        
        # Final progress log
        progress_logger.info(f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        shutdown_flag.set()

def test_ollama_connection():
    """Test if remote Ollama is running and model is available"""
    try:
        logger.info(f"Testing connection to remote Ollama: {OLLAMA_BASE_URL}")
        
        # Prepare headers for remote endpoint
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {OLLAMA_TOKEN}' if OLLAMA_TOKEN else None
        }
        headers = {k: v for k, v in headers.items() if v is not None}
        
        # Test basic connection with simple generation
        logger.info("Testing simple generation...")
        
        test_payload = {
            "model": OLLAMA_MODEL,
            "prompt": "Generate a JSON object with 'test': 'success'",
            "stream": False,
            "options": {"num_predict": 20}
        }
        
        test_response = requests.post(
            OLLAMA_URL, 
            json=test_payload,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"Generation test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from generation endpoint")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "response" in result:
                logger.info("Ollama connection test successful")
                logger.info(f"Test response: {result['response'][:100]}...")
                return True
            else:
                logger.error(f"No 'response' field in test. Fields: {list(result.keys())}")
                return False
        except json.JSONDecodeError as e:
            logger.error(f"Generation test returned invalid JSON: {e}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection test failed: {e}")
        return False

def get_collection_stats():
    """Get collection statistics"""
    try:
        total_count = email_col.count_documents({})
        
        # Count records with and without content
        with_content = email_col.count_documents({
            "subject": {"$exists": True, "$ne": "", "$ne": None},
            "message_text": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_content = total_count - with_content
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(email_col.aggregate(pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"Total emails: {total_count}")
        logger.info(f"With content (subject & message_text): {with_content}")
        logger.info(f"Without content: {without_content}")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"{i}. {topic['_id']}: {topic['count']} emails")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_content={with_content}, without_content={without_content}")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_emails(limit=3):
    """Get sample emails with generated content"""
    try:
        samples = list(email_col.find({
            "subject": {"$exists": True, "$ne": "", "$ne": None},
            "message_text": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Email Content:")
        for i, email in enumerate(samples, 1):
            logger.info(f"--- Sample Email {i} ---")
            logger.info(f"Message ID: {email.get('message_id', 'N/A')}")
            logger.info(f"Sender: {email.get('sender_name', 'N/A')}")
            logger.info(f"Dominant Topic: {email.get('dominant_topic', 'N/A')}")
            logger.info(f"Subtopics: {str(email.get('subtopics', 'N/A'))[:100]}...")
            logger.info(f"Subject: {email.get('subject', 'N/A')}")
            logger.info(f"Message Preview: {str(email.get('message_text', 'N/A'))[:200]}...")
            if 'is_urgent' in email:
                logger.info(f"Urgent: {email['is_urgent']}")
            
    except Exception as e:
        logger.error(f"Error getting sample emails: {e}")

def generate_status_report():
    """Generate comprehensive status report"""
    try:
        report_file = LOG_DIR / f"status_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Get intermediate results stats
        pending_results = results_manager.get_pending_updates()
        total_intermediate = len(results_manager.results)
        
        # Get database stats
        total_count = email_col.count_documents({})
        with_content = email_col.count_documents({
            "subject": {"$exists": True, "$ne": "", "$ne": None},
            "message_text": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Generate report
        status_report = {
            "timestamp": datetime.now().isoformat(),
            "session_stats": {
                "successful_generations": success_counter.value,
                "failed_generations": failure_counter.value,
                "database_updates": update_counter.value,
                "success_rate": (success_counter.value / (success_counter.value + failure_counter.value)) * 100 if (success_counter.value + failure_counter.value) > 0 else 0
            },
            "intermediate_results": {
                "total_results": total_intermediate,
                "pending_database_saves": len(pending_results),
                "intermediate_file": str(INTERMEDIATE_RESULTS_FILE)
            },
            "database_stats": {
                "total_emails": total_count,
                "emails_with_content": with_content,
                "emails_without_content": total_count - with_content,
                "completion_percentage": (with_content / total_count) * 100 if total_count > 0 else 0
            },
            "system_info": {
                "cpu_cores": CPU_COUNT,
                "max_workers": MAX_WORKERS,
                "batch_size": BATCH_SIZE,
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent
            },
            "log_files": {
                "main_log": str(MAIN_LOG_FILE),
                "success_log": str(SUCCESS_LOG_FILE),
                "failure_log": str(FAILURE_LOG_FILE),
                "progress_log": str(PROGRESS_LOG_FILE)
            }
        }
        
        # Save report
        with open(report_file, 'w') as f:
            json.dump(status_report, f, indent=2)
        
        logger.info(f"Status report generated: {report_file}")
        return status_report
        
    except Exception as e:
        logger.error(f"Error generating status report: {e}")
        return None

def recover_from_intermediate_results():
    """Recover and process any pending intermediate results"""
    try:
        pending_results = results_manager.get_pending_updates()
        
        if not pending_results:
            logger.info("No pending intermediate results to recover")
            return 0
        
        logger.info(f"Recovering {len(pending_results)} pending intermediate results...")
        
        # Convert to database update format
        batch_updates = []
        for result in pending_results:
            if 'message_id' in result and 'update_doc' in result:
                batch_updates.append({
                    'message_id': result['message_id'],
                    'update_doc': result['update_doc']
                })
        
        if batch_updates:
            # Process in batches of BATCH_SIZE
            total_recovered = 0
            for i in range(0, len(batch_updates), BATCH_SIZE):
                batch = batch_updates[i:i + BATCH_SIZE]
                saved_count = save_batch_to_database(batch)
                total_recovered += saved_count
                logger.info(f"Recovered batch: {saved_count} records")
            
            logger.info(f"Successfully recovered {total_recovered} records from intermediate results")
            return total_recovered
        
        return 0
        
    except Exception as e:
        logger.error(f"Error recovering intermediate results: {e}")
        return 0

def validate_generated_content():
    """Validate recently generated content quality"""
    try:
        # Get recent generated emails
        recent_emails = list(email_col.find({
            "subject": {"$exists": True, "$ne": "", "$ne": None},
            "message_text": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(20))
        
        validation_results = {
            "total_validated": len(recent_emails),
            "validation_issues": [],
            "quality_metrics": {
                "avg_subject_length": 0,
                "avg_message_length": 0,
                "urgent_emails_count": 0,
                "emails_with_placeholders": 0
            }
        }
        
        subject_lengths = []
        message_lengths = []
        urgent_count = 0
        placeholder_count = 0
        
        for email in recent_emails:
            subject = email.get('subject', '')
            message = email.get('message_text', '')
            
            # Validate subject
            if len(subject) < 20 or len(subject) > 100:
                validation_results["validation_issues"].append({
                    "message_id": email.get('message_id'),
                    "issue": f"Subject length {len(subject)} outside optimal range (20-100)"
                })
            
            # Validate message
            if len(message) < 200 or len(message) > 800:
                validation_results["validation_issues"].append({
                    "message_id": email.get('message_id'),
                    "issue": f"Message length {len(message)} outside optimal range (200-800)"
                })
            
            # Check for required elements
            if 'Best regards,' not in message:
                validation_results["validation_issues"].append({
                    "message_id": email.get('message_id'),
                    "issue": "Missing proper signature"
                })
            
            # Check for placeholders (this should not happen now)
            content_to_check = subject + message
            if '[' in content_to_check or 'placeholder' in content_to_check.lower():
                validation_results["validation_issues"].append({
                    "message_id": email.get('message_id'),
                    "issue": "Contains placeholder text"
                })
                placeholder_count += 1
            
            subject_lengths.append(len(subject))
            message_lengths.append(len(message))
            
            if email.get('is_urgent', False):
                urgent_count += 1
        
        # Calculate metrics
        if subject_lengths:
            validation_results["quality_metrics"]["avg_subject_length"] = sum(subject_lengths) / len(subject_lengths)
        if message_lengths:
            validation_results["quality_metrics"]["avg_message_length"] = sum(message_lengths) / len(message_lengths)
        validation_results["quality_metrics"]["urgent_emails_count"] = urgent_count
        validation_results["quality_metrics"]["emails_with_placeholders"] = placeholder_count
        
        logger.info(f"Content validation complete: {len(validation_results['validation_issues'])} issues found")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating content: {e}")
        return None

def cleanup_old_logs(days_to_keep=7):
    """Clean up old log files"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cleaned_count = 0
        
        for log_file in LOG_DIR.glob("*.log"):
            if log_file.stat().st_mtime < cutoff_date.timestamp():
                log_file.unlink()
                cleaned_count += 1
        
        for json_file in LOG_DIR.glob("*.json"):
            if json_file.stat().st_mtime < cutoff_date.timestamp():
                json_file.unlink()
                cleaned_count += 1
        
        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} old log files")
        
    except Exception as e:
        logger.error(f"Error cleaning up old logs: {e}")

# Main execution function
def main():
    """Main function to initialize and run the email content generator"""
    logger.info("🏦 EU Banking Email Content Generator Starting...")
    logger.info(f"💾 Database: {DB_NAME}")
    logger.info(f"📂 Collection: {EMAIL_COLLECTION}")
    logger.info(f"🤖 Model: {OLLAMA_MODEL}")
    logger.info(f"🔗 Ollama URL: {OLLAMA_BASE_URL}")
    logger.info(f"⚡ Max Workers: {MAX_WORKERS}")
    logger.info(f"📦 Batch Size: {BATCH_SIZE}")
    logger.info(f"📝 Log Directory: {LOG_DIR}")
    
    # Setup signal handlers and cleanup
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    # Clean up old logs
    cleanup_old_logs()
    
    # Initialize database
    if not init_database():
        logger.error("Cannot proceed without database connection")
        return
    
    try:
        # Show current collection stats
        get_collection_stats()
        
        # Try to recover any pending intermediate results first
        recovered_count = recover_from_intermediate_results()
        if recovered_count > 0:
            logger.info(f"Recovered {recovered_count} records from previous session")
        
        # Run the email content generation
        update_emails_with_content_parallel()
        
        # Show final statistics
        get_collection_stats()
        
        # Show sample generated content
        get_sample_generated_emails()
        
        # Validate content quality
        validation_results = validate_generated_content()
        if validation_results and validation_results["validation_issues"]:
            logger.warning(f"Found {len(validation_results['validation_issues'])} content validation issues")
        
        # Generate final status report
        status_report = generate_status_report()
        if status_report:
            logger.info("Final session report:")
            logger.info(f"Success Rate: {status_report['session_stats']['success_rate']:.2f}%")
            logger.info(f"Database Completion: {status_report['database_stats']['completion_percentage']:.2f}%")
        
    except KeyboardInterrupt:
        logger.info("Content generation interrupted by user!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        # Save final intermediate results
        results_manager.save_to_file()
        cleanup_resources()
        
        logger.info("Session complete. Check log files for detailed information:")
        logger.info(f"📋 Main Log: {MAIN_LOG_FILE}")
        logger.info(f"✅ Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"❌ Failure Log: {FAILURE_LOG_FILE}")
        logger.info(f"📊 Progress Log: {PROGRESS_LOG_FILE}")
        logger.info(f"💾 Intermediate Results: {INTERMEDIATE_RESULTS_FILE}")

# Run the content generator
if __name__ == "__main__":
    main()