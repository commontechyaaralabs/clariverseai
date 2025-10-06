# EU Banking Email Thread Generation and Analysis System - Ollama Version
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
from bson import ObjectId
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
EMAIL_COLLECTION = "email_new"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"email_generator_ollama_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"progress_{timestamp}.log"
INTERMEDIATE_RESULTS_FILE = LOG_DIR / f"intermediate_results_{timestamp}.json"

# Configure main logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(MAIN_LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Configure specialized loggers
success_logger = logging.getLogger('success')
success_logger.setLevel(logging.INFO)
success_handler = logging.FileHandler(SUCCESS_LOG_FILE, encoding='utf-8')
success_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
success_logger.addHandler(success_handler)
success_logger.propagate = False

failure_logger = logging.getLogger('failure')
failure_logger.setLevel(logging.ERROR)
failure_handler = logging.FileHandler(FAILURE_LOG_FILE, encoding='utf-8')
failure_handler.setFormatter(logging.Formatter('%(asctime)s - ERROR - %(message)s'))
failure_logger.addHandler(failure_handler)
failure_logger.propagate = False

progress_logger = logging.getLogger('progress')
progress_logger.setLevel(logging.INFO)
progress_handler = logging.FileHandler(PROGRESS_LOG_FILE, encoding='utf-8')
progress_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
progress_logger.addHandler(progress_handler)
progress_logger.propagate = False

logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
client = None
db = None
email_col = None

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Configuration values
OLLAMA_MODEL = "gemma3:27b"
BATCH_SIZE = 3
MAX_WORKERS = CPU_COUNT  # Use all available CPU cores
REQUEST_TIMEOUT = 300  # Increased from 120 to 300 seconds (5 minutes)
MAX_RETRIES = 5
RETRY_DELAY = 3
BATCH_DELAY = 2.0  # Reduced delay for faster processing
API_CALL_DELAY = 0.5  # Reduced API call delay

# Ollama setup
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "7eb2c60fcd3740cea657c8d109ff9016af894d2a2c112954bc3aff033c117736")
OLLAMA_URL = "http://34.147.17.26:16637/api/chat"

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Retry configuration
MAX_RETRY_ATTEMPTS = 3  # Maximum retry attempts for failed records
RETRY_DELAY_SECONDS = 5  # Delay between retry attempts

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
retry_counter = LoggingCounter("RETRY_COUNT")

# Failed records tracking
failed_records = []  # Store failed records for retry

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
    
    def mark_as_saved(self, thread_ids):
        """Mark results as saved to database"""
        with self._lock:
            for result in self.results:
                if result.get('thread_id') in thread_ids:
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
        email_col.create_index("thread.thread_id")
        email_col.create_index("dominant_topic")
        email_col.create_index("urgency")
        email_col.create_index("stages")
        email_col.create_index("category")
        email_col.create_index("priority")
        email_col.create_index("resolution_status")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=600,  # Increased max time for rate limiting and longer processing
    base=RETRY_DELAY,
    on_backoff=lambda details: logger.warning(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_ollama_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call Ollama API with exponential backoff and better error handling"""
    if shutdown_flag.is_set():
        raise KeyboardInterrupt("Shutdown requested")
    
    headers = {
        'Authorization': f'Bearer {OLLAMA_API_KEY}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        "model": OLLAMA_MODEL,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "stream": False,
        "options": {
            "temperature": 0.4,
            "num_predict": 4000
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
        
        if "message" not in result or "content" not in result["message"]:
            logger.error(f"No 'message.content' field. Available fields: {list(result.keys())}")
            raise KeyError("No 'message.content' field in Ollama response")
            
        # Add delay to help with rate limiting
        time.sleep(API_CALL_DELAY)
        return result["message"]["content"]
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out after {timeout} seconds")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - check Ollama endpoint")
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.warning(f"Rate limited (429) - will retry with backoff")
        else:
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

def generate_optimized_email_prompt(email_data):
    """Generate optimized prompt for email content and analysis generation"""
    
    # Extract data from email record - ALL EXISTING FIELDS FROM email_new COLLECTION
    dominant_topic = email_data.get('dominant_topic', 'General Banking')
    subtopics = email_data.get('subtopics', 'General operations')
    messages = email_data.get('messages', [])
    message_count = len(messages) if messages else 2
    
    # EXISTING FIELDS FROM email_new collection - USE THESE EXACT VALUES
    stages = email_data.get('stages', 'Receive')
    category = email_data.get('category', 'External')
    overall_sentiment = email_data.get('overall_sentiment', 1)
    urgency = email_data.get('urgency', False)
    follow_up_required = email_data.get('follow_up_required', 'no')
    action_pending_status = email_data.get('action_pending_status', 'no')
    action_pending_from = email_data.get('action_pending_from', None)
    priority = email_data.get('priority', 'P3-Medium')
    resolution_status = email_data.get('resolution_status', 'open')
    
    # Extract participant details from message headers
    participants = []
    if messages:
        # Collect all unique participants from message headers
        unique_participants = {}
        for message in messages:
            if not message or not isinstance(message, dict):
                continue
                
            headers = message.get('headers', {})
            if not headers or not isinstance(headers, dict):
                continue
            
            # Add from participants
            from_list = headers.get('from', [])
            if from_list and isinstance(from_list, list):
                for from_person in from_list:
                    if not from_person or not isinstance(from_person, dict):
                        continue
                    email = from_person.get('email', '')
                    if email and email not in unique_participants:
                        unique_participants[email] = {
                            'name': from_person.get('name', 'Unknown'),
                            'email': email,
                            'type': 'from'
                        }
            
            # Add to participants
            to_list = headers.get('to', [])
            if to_list and isinstance(to_list, list):
                for to_person in to_list:
                    if not to_person or not isinstance(to_person, dict):
                        continue
                    email = to_person.get('email', '')
                    if email and email not in unique_participants:
                        unique_participants[email] = {
                            'name': to_person.get('name', 'Unknown'),
                            'email': email,
                            'type': 'to'
                        }
        
        participants = list(unique_participants.values())
    
    
    # Extract sender and recipient for reference
    sender = next((p for p in participants if p['type'] == 'from'), participants[0] if participants else {'name': 'Customer', 'email': 'customer@example.com'})
    recipient = next((p for p in participants if p['type'] == 'to'), participants[1] if len(participants) > 1 else {'name': 'Support Team', 'email': 'support@eubank.com'})
    
    # Map category to communication context and direction
    if category == "External":
        category_context = "external customer or third-party communication TO the bank"
        communication_direction = "FROM external parties (customers, vendors, regulatory bodies, other banks) TO EU Bank"
        sender_context = "External party (customer, vendor, regulatory body, or other bank)"
        recipient_context = "EU Bank department or employee"
    else:  # Internal
        category_context = "internal inter-bank communication between departments or employees"
        communication_direction = "WITHIN EU Bank between departments, employees, or internal systems"
        sender_context = "EU Bank department or employee"
        recipient_context = "EU Bank department or employee"
    
    # Determine action pending context based on action_pending_from
    action_pending_context = ""
    if action_pending_status == "yes" and action_pending_from:
        if action_pending_from.lower() == "customer":
            action_pending_context = "The customer needs to take the next action (provide documents, respond to request, complete process, etc.)"
        elif action_pending_from.lower() == "bank":
            action_pending_context = "The bank needs to take the next action (process request, review documents, provide response, etc.)"
        else:
            action_pending_context = f"The {action_pending_from} needs to take the next action"
    elif action_pending_status == "yes":
        action_pending_context = "An action is pending but the responsible party is unclear"
    else:
        action_pending_context = "No action is pending - process is complete or ongoing"
    
    # Format message headers for LLM to understand from/to for each message
    message_headers_info = []
    if messages:
        for i, message in enumerate(messages):
            if not message or not isinstance(message, dict):
                continue
                
            headers = message.get('headers', {})
            if not headers or not isinstance(headers, dict):
                continue
                
            from_list = headers.get('from', [])
            to_list = headers.get('to', [])
            
            from_info = []
            if from_list and isinstance(from_list, list):
                for from_person in from_list:
                    if from_person and isinstance(from_person, dict):
                        from_info.append(f"{from_person.get('name', 'Unknown')} ({from_person.get('email', 'unknown@example.com')})")
            
            to_info = []
            if to_list and isinstance(to_list, list):
                for to_person in to_list:
                    if to_person and isinstance(to_person, dict):
                        to_info.append(f"{to_person.get('name', 'Unknown')} ({to_person.get('email', 'unknown@example.com')})")
            
            if from_info and to_info:
                message_headers_info.append(f"Message {i+1}: FROM {', '.join(from_info)} TO {', '.join(to_info)}")
    
    message_headers_text = "\n".join(message_headers_info) if message_headers_info else "No message headers found"
    
    prompt = f"""Generate EU banking email thread with {message_count} messages.

**METADATA:** Stage:{stages} | Category:{category} | Sentiment:{overall_sentiment}/5 | Urgency:{urgency} | Follow-up:{follow_up_required} | Action:{action_pending_status} | Action From:{action_pending_from} | Priority:{priority} | Resolution:{resolution_status} | Topic:{dominant_topic}

**ACTION PENDING CONTEXT:** {action_pending_context}

**MESSAGE HEADERS:** {message_headers_text}

**RULES:** 
- Sentiment {overall_sentiment}/5: {"Extreme frustration throughout ALL messages" if overall_sentiment == 5 else "Clear anger/frustration" if overall_sentiment == 4 else "Moderate concern/unease" if overall_sentiment == 3 else "Slight irritation/impatience" if overall_sentiment == 2 else "Calm professional baseline" if overall_sentiment == 1 else "Positive satisfied communication"}
- Bank employees: ALWAYS calm, professional, helpful
- Subject: Generate realistic banking subject lines based on category:
  * External (Customer→Bank): Customer queries, complaints, requests
  * Internal (Bank→Customer): Bank notifications, alerts, updates
  - NEVER use dominant topic as subject | {"NO urgency words (urgent, immediate, asap, priority, critical, emergency, attention, required, needed, expedite, rush, hurry, fast, quick, timely, deadline, time-sensitive, pending, overdue)" if not urgency else "Time-sensitive words allowed"}
- Follow-up {follow_up_required}: {"End with open-ended scenarios" if follow_up_required == "yes" else "End with complete resolution"}
- Action {action_pending_status}: {"Show waiting scenarios" if action_pending_status == "yes" else "Show completed processes"}
- Action Pending From {action_pending_from}: {"End with customer needing to respond/take action" if action_pending_from and action_pending_from.lower() == "customer" else "End with bank needing to respond/take action" if action_pending_from and action_pending_from.lower() == "bank" else "End with appropriate party needing to take action" if action_pending_status == "yes" else "End with completed process"}

**STRUCTURE:** "Dear [Name]," → Body (200-300 words) → "Thanks," → Signature (name+title only) | NO "From/To" headers | NO subject in body | NO email addresses in signatures | Dates: 2025-01-01 to 2025-06-30
**IMPORTANT:** Use the FROM/TO addresses from MESSAGE HEADERS for each message - the "to" addresses should be used in "Dear [Name]" and the "from" address should be used in the signature

**BANKING:** Realistic EU accounts (DE89 3704 0044 0532 0130 00) | Specific amounts (€1,250.00) | Transaction IDs (TXN-2025-001234567) | Customer details (DOB: 15/03/1985) | Authentic banking terminology

**OUTPUT:** {{
  "thread_data": {{"subject_norm": "[contextual_banking_subject_with_specific_details]", "first_message_at": "[ISO_timestamp]", "last_message_at": "[ISO_timestamp]"}},
  "messages": [{{"headers": {{"date": "[ISO_timestamp]", "subject": "[contextual_banking_subject_with_specific_details]"}}, "body": {{"text": {{"plain": "[content]"}}}}}}],
  "analysis": {{"email_summary": "[100-150 word summary]", "follow_up_date": {"[ISO_timestamp]" if follow_up_required == "yes" else "null"}, "follow_up_reason": {"[WHY follow-up is needed - the trigger/justification]" if follow_up_required == "yes" else "null"}, "next_action_suggestion": {"[WHAT step to take - the action recommendation]" if follow_up_required == "yes" and action_pending_status == "yes" else "null"}}}
}}

Use EXACT metadata values. Implement concepts through natural scenarios, NOT explicit mentions. Generate authentic banking content with specific details.

**CRITICAL:** 
- SUBJECT: Generate realistic banking subject lines based on category:
  * External: Customer queries, complaints, requests
  * Internal: Bank notifications, alerts, updates
  - NEVER use dominant topic as subject
- Follow-up reason = "WHY" (the trigger/justification for follow-up) - ONLY if follow_up_required="yes", otherwise "null"
- Follow-up date = ISO timestamp - ONLY if follow_up_required="yes", otherwise "null"
- Next-action suggestion = "WHAT" (the step you advise taking) - ONLY generate if follow_up_required="yes" AND action_pending_status="yes":
  * If action_pending_from="Customer": Suggest what the customer needs to do
  * If action_pending_from="Bank": Suggest what the bank needs to do
  * If both follow_up_required="no" AND action_pending_status="no": Set to "null"

Generate now.
""".strip()
    
    return prompt

def generate_email_content(email_data):
    """Generate email content and analysis with Ollama"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    thread_id = email_data.get('thread', {}).get('thread_id', 'unknown')
    
    try:
        prompt = generate_optimized_email_prompt(email_data)
        
        response = call_ollama_with_backoff(prompt)
        
        if not response or not response.strip():
            raise ValueError("Empty response from LLM")
        
        # Clean and parse JSON response
        reply = response.strip()
        
        # Remove markdown formatting if present
        if "```" in reply:
            reply = reply.replace("```json", "").replace("```", "")
        
        # Extract JSON
        json_start = reply.find('{')
        json_end = reply.rfind('}') + 1
        
        if json_start == -1 or json_end <= json_start:
            raise ValueError("No valid JSON found in LLM response")
        
        reply = reply[json_start:json_end]
        
        try:
            result = json.loads(reply)
            # Reduce debug logging for performance
            if thread_id.endswith('0') or thread_id.endswith('5'):  # Log only every 10th thread
                logger.info(f"Thread {thread_id}: JSON parsing successful. Keys: {list(result.keys())}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing failed for thread {thread_id}. Raw response: {reply[:300]}...")
            logger.error(f"Thread {thread_id}: Full LLM response: {response[:500]}...")
            raise ValueError(f"Invalid JSON response from LLM: {json_err}")
        
        # Validate required fields
        required_fields = ['thread_data', 'messages', 'analysis']
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            logger.error(f"Thread {thread_id}: Missing required fields: {missing_fields}")
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Validate thread_data fields
        thread_data_fields = ['subject_norm', 'first_message_at', 'last_message_at']
        for field in thread_data_fields:
            if field not in result['thread_data']:
                logger.error(f"Thread {thread_id}: Missing thread_data field: {field}")
                raise ValueError(f"Missing thread_data field: {field}")
        
        # Validate analysis fields - only new LLM-generated fields
        analysis_fields = [
            'email_summary', 'follow_up_date', 'follow_up_reason', 'next_action_suggestion'
        ]
        for field in analysis_fields:
            if field not in result['analysis']:
                logger.error(f"Thread {thread_id}: Missing analysis field: {field}")
                raise ValueError(f"Missing analysis field: {field}")
        
        # Validate messages count
        message_count = email_data.get('thread', {}).get('message_count', 2)
        if len(result['messages']) != message_count:
            logger.warning(f"Thread {thread_id}: Expected {message_count} messages, got {len(result['messages'])}")
            # Adjust to correct count
            if len(result['messages']) > message_count:
                result['messages'] = result['messages'][:message_count]
        
        # Clean up any subject lines and headers that might appear in body content
        if 'messages' in result:
            for message in result['messages']:
                # Validate message structure before accessing nested elements
                if (isinstance(message, dict) and 
                    'body' in message and isinstance(message['body'], dict) and 
                    'text' in message['body'] and isinstance(message['body']['text'], dict) and 
                    'plain' in message['body']['text']):
                    body_content = message['body']['text']['plain']
                    
                    # Check for various subject line and header patterns and remove them
                    unwanted_patterns = [
                        'Subject:', 'SUBJECT:', 'subject:', 'Re:', 'RE:', 'Fwd:', 'FWD:',
                        'To:', 'TO:', 'to:', 'From:', 'FROM:', 'from:', 'CC:', 'cc:', 'Cc:',
                        'BCC:', 'bcc:', 'Bcc:', 'Date:', 'DATE:', 'date:'
                    ]
                    
                    cleaned_body = body_content
                    for pattern in unwanted_patterns:
                        if pattern in cleaned_body:
                            lines = cleaned_body.split('\n')
                            cleaned_lines = []
                            
                            for line in lines:
                                if any(line.strip().startswith(p) for p in unwanted_patterns):
                                    logger.warning(f"Removed unwanted header '{line.strip()}' from body content for thread {thread_id}")
                                    continue
                                cleaned_lines.append(line)
                            
                            cleaned_body = '\n'.join(cleaned_lines)
                    
                    # Safely access nested message structure
                    if isinstance(message.get('body'), dict) and isinstance(message['body'].get('text'), dict):
                        message['body']['text']['plain'] = cleaned_body
                    else:
                        logger.error(f"Thread {thread_id}: Invalid message structure - body or text is not a dict")
        
        generation_time = time.time() - start_time
        
        # Log success with all preserved fields
        success_info = {
            'thread_id': thread_id,
            'dominant_topic': email_data.get('dominant_topic'),
            'subject': result['thread_data']['subject_norm'],
            'stages': email_data.get('stages'),
            'category': email_data.get('category'),
            'urgency': email_data.get('urgency'),
            'priority': email_data.get('priority'),
            'resolution_status': email_data.get('resolution_status'),
            'generation_time': generation_time
        }
        success_logger.info(json.dumps(success_info))
        
        return result
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'thread_id': thread_id,
            'dominant_topic': email_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_email_update(email_record, retry_attempt=0):
    """Process a single email record to generate content and analysis"""
    if shutdown_flag.is_set():
        return None
        
    # Extract thread_id for logging
    thread_id = email_record.get('thread', {}).get('thread_id', 'unknown')
        
    try:
        # Generate email content based on existing data
        email_content = generate_email_content(email_record)
        
        if not email_content:
            if retry_attempt < MAX_RETRY_ATTEMPTS:
                logger.warning(f"Generation failed for {thread_id}, will retry (attempt {retry_attempt + 1}/{MAX_RETRY_ATTEMPTS})")
                return None  # Will be retried
            else:
                failure_counter.increment()
                logger.error(f"Final failure for {thread_id} after {MAX_RETRY_ATTEMPTS} attempts")
                return None
        
        # Prepare update document with the new structure
        update_doc = {}
        
        # Update thread data
        if 'thread_data' in email_content:
            thread_data = email_content['thread_data']
            if not isinstance(thread_data, dict):
                logger.error(f"Thread {thread_id}: thread_data is not a dict, got {type(thread_data)}: {str(thread_data)[:100]}...")
            else:
                update_doc['thread.subject_norm'] = thread_data.get('subject_norm')
                update_doc['thread.first_message_at'] = thread_data.get('first_message_at')
                update_doc['thread.last_message_at'] = thread_data.get('last_message_at')
        
        # Update messages with generated content
        if 'messages' in email_content:
            messages = email_content['messages']
            if not isinstance(messages, list):
                logger.error(f"Thread {thread_id}: messages is not a list, got {type(messages)}: {str(messages)[:100]}...")
            else:
                for i, message in enumerate(messages):
                    # Validate message is a dictionary, not a string
                    if not isinstance(message, dict):
                        logger.error(f"Thread {thread_id}: Message {i} is not a dict, got {type(message)}: {str(message)[:100]}...")
                        continue
                    
                    # Safely access nested message structure
                    headers = message.get('headers', {})
                    body = message.get('body', {})
                    text = body.get('text', {}) if isinstance(body, dict) else {}
                    
                    update_doc[f'messages.{i}.headers.date'] = headers.get('date') if isinstance(headers, dict) else None
                    update_doc[f'messages.{i}.headers.subject'] = headers.get('subject') if isinstance(headers, dict) else None
                    update_doc[f'messages.{i}.body.text.plain'] = text.get('plain') if isinstance(text, dict) else None
        
        # Update analysis fields from LLM response - only new fields, preserve existing metadata
        if 'analysis' in email_content:
            analysis = email_content['analysis']
            if not isinstance(analysis, dict):
                logger.error(f"Thread {thread_id}: analysis is not a dict, got {type(analysis)}: {str(analysis)[:100]}...")
            else:
                # Only update LLM-generated fields - do NOT overwrite existing metadata fields
                update_doc['email_summary'] = analysis.get('email_summary')
                update_doc['follow_up_date'] = analysis.get('follow_up_date')
                update_doc['follow_up_reason'] = analysis.get('follow_up_reason')
                update_doc['next_action_suggestion'] = analysis.get('next_action_suggestion')
        
        # Add LLM processing tracking
        update_doc['llm_processed'] = True
        update_doc['llm_processed_at'] = datetime.now().isoformat()
        update_doc['llm_model_used'] = OLLAMA_MODEL
        
        success_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'thread_id': email_record['thread']['thread_id'],
            'update_doc': update_doc,
            'original_data': {
                'dominant_topic': email_record.get('dominant_topic'),
                'subtopics': email_record.get('subtopics', '')[:100] + '...' if len(str(email_record.get('subtopics', ''))) > 100 else email_record.get('subtopics', '')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'thread_id': email_record['thread']['thread_id'],
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {email_record.get('thread', {}).get('thread_id', 'unknown')}: {str(e)[:100]}")
        if retry_attempt < MAX_RETRY_ATTEMPTS:
            logger.warning(f"Will retry {email_record.get('thread', {}).get('thread_id', 'unknown')} due to error (attempt {retry_attempt + 1}/{MAX_RETRY_ATTEMPTS})")
            return None  # Will be retried
        else:
            failure_counter.increment()
            logger.error(f"Final failure for {email_record.get('thread', {}).get('thread_id', 'unknown')} after {MAX_RETRY_ATTEMPTS} attempts")
            return None

def retry_failed_records(failed_records_list):
    """Retry processing failed records"""
    if not failed_records_list:
        return []
    
    logger.info(f"Retrying {len(failed_records_list)} failed records...")
    retry_counter.increment()
    
    successful_retries = []
    
    for record_data in failed_records_list:
        if shutdown_flag.is_set():
            break
            
        email_record = record_data['record']
        retry_attempt = record_data.get('retry_attempt', 0) + 1
        
        logger.info(f"Retrying {email_record.get('thread', {}).get('thread_id', 'unknown')} (attempt {retry_attempt}/{MAX_RETRY_ATTEMPTS})")
        
        # Add delay before retry
        time.sleep(RETRY_DELAY_SECONDS)
        
        result = process_single_email_update(email_record, retry_attempt)
        
        if result:
            successful_retries.append(result)
            logger.info(f"Retry successful for {email_record.get('thread', {}).get('thread_id', 'unknown')}")
        else:
            if retry_attempt < MAX_RETRY_ATTEMPTS:
                # Add back to failed records for another retry
                failed_records.append({
                    'record': email_record,
                    'retry_attempt': retry_attempt
                })
            else:
                logger.error(f"Final retry failure for {email_record.get('thread', {}).get('thread_id', 'unknown')}")
    
    return successful_retries

def save_batch_to_database(batch_updates):
    """Save a batch of updates to the database using proper bulk write operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create proper UpdateOne operations
        bulk_operations = []
        thread_ids = []
        
        for update_data in batch_updates:
            # Create UpdateOne operation properly
            operation = UpdateOne(
                filter={"thread.thread_id": update_data['thread_id']},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            thread_ids.append(update_data['thread_id'])
        
        # Execute bulk write with proper error handling
        if bulk_operations:
            try:
                result = email_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Mark intermediate results as saved
                results_manager.mark_as_saved(thread_ids)
                
                # Update counter
                update_counter._value += updated_count
                
                logger.info(f"Successfully saved {updated_count} records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved, total_updates: {update_counter.value}")
                
                # Log some details of what was saved
                if updated_count > 0:
                    sample_update = batch_updates[0]
                    logger.info(f"Sample update - Thread ID: {sample_update['thread_id']}")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Try individual updates as fallback
                logger.info("Attempting individual updates as fallback...")
                individual_success = 0
                
                for update_data in batch_updates:
                    try:
                        result = email_col.update_one(
                            {"thread.thread_id": update_data['thread_id']},
                            {"$set": update_data['update_doc']}
                        )
                        if result.modified_count > 0:
                            individual_success += 1
                    except Exception as individual_error:
                        logger.error(f"Individual update failed for {update_data['thread_id']}: {individual_error}")
                
                if individual_success > 0:
                    results_manager.mark_as_saved([up['thread_id'] for up in batch_updates[:individual_success]])
                    update_counter._value += individual_success
                    logger.info(f"Fallback: {individual_success} records saved individually")
                
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def update_emails_with_content_parallel():
    """Update existing emails with generated content and analysis using optimized batch processing"""
    
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
        # Query for emails that don't have the analysis fields
        query = {
            "$or": [
                {"email_summary": {"$exists": False}},
                {"next_action_suggestion": {"$exists": False}},
                {"follow_up_date": {"$exists": False}},
                {"follow_up_reason": {"$exists": False}}
            ]
        }
        
        email_records = list(email_col.find(query))
        total_emails = len(email_records)
        
        if total_emails == 0:
            logger.info("All emails already have analysis fields!")
            return
            
        logger.info(f"Found {total_emails} emails needing content generation")
        progress_logger.info(f"BATCH_START: total_emails={total_emails}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching email records: {e}")
        return
    
    # Process in batches of BATCH_SIZE (3)
    total_batches = (total_emails + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []  # Accumulate updates for batch saving
    
    logger.info(f"Processing in {total_batches} batches of {BATCH_SIZE} emails each")
    logger.info(f"Using {MAX_WORKERS} workers for parallel processing")
    
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
            
            # Process batch with optimized parallelization
            successful_updates = []
            batch_start_time = time.time()
            
            # Use ThreadPoolExecutor for I/O bound operations (API calls)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks for this batch
                futures = {
                    executor.submit(process_single_email_update, record): record 
                    for record in batch_records
                }
                
                # Collect results with progress tracking - process as they complete
                completed = 0
                batch_failed_records = []  # Track failed records for this batch
                
                try:
                    for future in as_completed(futures, timeout=REQUEST_TIMEOUT * 3):  # Increased timeout multiplier
                        if shutdown_flag.is_set():
                            logger.warning("Cancelling remaining tasks...")
                            for f in futures:
                                f.cancel()
                            break
                            
                        try:
                            result = future.result(timeout=60)  # Increased from 30 to 60 seconds
                            completed += 1
                            
                            if result:
                                successful_updates.append(result)
                                # Save immediately when we have enough for a batch
                                if len(successful_updates) >= BATCH_SIZE:
                                    saved_count = save_batch_to_database(successful_updates)
                                    total_updated += saved_count
                                    successful_updates = []  # Clear after saving
                                    logger.info(f"Immediate save: {saved_count} records")
                            
                            # Progress indicator for each completion
                            progress = (completed / len(batch_records)) * 100
                            logger.info(f"Task completed: {progress:.1f}% ({completed}/{len(batch_records)})")
                                
                        except Exception as e:
                            logger.error(f"Error processing future result: {e}")
                            completed += 1
                            
                except Exception as e:
                    logger.error(f"Error collecting batch results: {e}")
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # Save any remaining updates from this batch
            if successful_updates and not shutdown_flag.is_set():
                saved_count = save_batch_to_database(successful_updates)
                total_updated += saved_count
                logger.info(f"Final batch save: {saved_count} records")
            
            # Retry failed records from this batch
            if failed_records and not shutdown_flag.is_set():
                logger.info(f"Retrying {len(failed_records)} failed records from batch {batch_num}")
                retry_successful = retry_failed_records(failed_records.copy())
                
                if retry_successful:
                    # Save retry results
                    retry_saved_count = save_batch_to_database(retry_successful)
                    total_updated += retry_saved_count
                    logger.info(f"Retry save: {retry_saved_count} records")
                
                # Clear failed records after retry attempt
                failed_records.clear()
            
            logger.info(f"Batch {batch_num} processing complete: {len(successful_updates)}/{len(batch_records)} successful")
            logger.info(f"Batch duration: {batch_duration:.2f}s")
            progress_logger.info(f"BATCH_COMPLETE: batch={batch_num}, successful={len(successful_updates)}, duration={batch_duration:.2f}s")
            
            # Progress summary every 3 batches (more frequent due to smaller batches)
            if batch_num % 3 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_emails) * 100
                logger.info(f"Overall Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value} | Retries: {retry_counter.value} | DB Updates: {total_updated}")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, success={success_counter.value}, failures={failure_counter.value}, retries={retry_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches to help with rate limiting (reduced)
            if not shutdown_flag.is_set() and batch_num < total_batches:
                time.sleep(BATCH_DELAY)
        
        # Final retry phase for any remaining failed records
        if failed_records and not shutdown_flag.is_set():
            logger.info(f"Final retry phase: {len(failed_records)} records remaining")
            final_retry_successful = retry_failed_records(failed_records.copy())
            
            if final_retry_successful:
                final_retry_saved = save_batch_to_database(final_retry_successful)
                total_updated += final_retry_saved
                logger.info(f"Final retry save: {final_retry_saved} records")
            
            failed_records.clear()
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking email content generation complete!")
            
        logger.info(f"Total emails updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        logger.info(f"Retry attempts: {retry_counter.value}")
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
    """Test if Ollama is accessible and model is available"""
    try:
        logger.info(f"Testing connection to Ollama: {OLLAMA_URL}")
        
        headers = {
            'Authorization': f'Bearer {OLLAMA_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Test basic connection with simple generation
        logger.info("Testing simple generation...")
        
        test_payload = {
            "model": OLLAMA_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": "Generate a JSON object with 'test': 'success'"
                }
            ],
            "stream": False,
            "options": {
                "temperature": 0.4,
                "num_predict": 20
            }
        }
        
        test_response = requests.post(
            OLLAMA_URL, 
            json=test_payload,
            headers=headers,
            timeout=60  # Increased from 30 to 60 seconds
        )
        
        logger.info(f"Generation test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from generation endpoint")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "message" in result and "content" in result["message"]:
                logger.info("Ollama connection test successful")
                logger.info(f"Test response: {result['message']['content'][:100]}...")
                return True
            else:
                logger.error(f"No 'message.content' field in test. Fields: {list(result.keys())}")
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
        
        # Count records with and without analysis fields
        with_analysis = email_col.count_documents({
            "email_summary": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_analysis = total_count - with_analysis
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(email_col.aggregate(pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"Total emails: {total_count}")
        logger.info(f"With analysis fields: {with_analysis}")
        logger.info(f"Without analysis fields: {without_analysis}")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"{i}. {topic['_id']}: {topic['count']} emails")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_analysis={with_analysis}, without_analysis={without_analysis}")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_emails(limit=3):
    """Get sample emails with generated content"""
    try:
        samples = list(email_col.find({
            "email_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Email Content:")
        for i, email in enumerate(samples, 1):
            logger.info(f"--- Sample Email {i} ---")
            logger.info(f"Thread ID: {email.get('thread', {}).get('thread_id', 'N/A')}")
            logger.info(f"Subject: {email.get('thread', {}).get('subject_norm', 'N/A')}")
            logger.info(f"Dominant Topic: {email.get('dominant_topic', 'N/A')}")
            logger.info(f"Email Summary: {str(email.get('email_summary', 'N/A'))[:200]}...")
            if 'urgency' in email:
                logger.info(f"Urgent: {email['urgency']}")
            
    except Exception as e:
        logger.error(f"Error getting sample emails: {e}")

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
            if 'thread_id' in result and 'update_doc' in result:
                batch_updates.append({
                    'thread_id': result['thread_id'],
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

# Main execution function
def main():
    """Main function to initialize and run the email content generator"""
    logger.info("EU Banking Email Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {EMAIL_COLLECTION}")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_URL}")
    logger.info(f"Max Workers: {MAX_WORKERS}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    logger.info(f"Log Directory: {LOG_DIR}")
    
    # Setup signal handlers and cleanup
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
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
        
    except KeyboardInterrupt:
        logger.info("Content generation interrupted by user!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        # Save final intermediate results
        results_manager.save_to_file()
        cleanup_resources()
        
        logger.info("Session complete. Check log files for detailed information:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")
        logger.info(f"Progress Log: {PROGRESS_LOG_FILE}")
        logger.info(f"Intermediate Results: {INTERMEDIATE_RESULTS_FILE}")

# Run the content generator
if __name__ == "__main__":
    main()
