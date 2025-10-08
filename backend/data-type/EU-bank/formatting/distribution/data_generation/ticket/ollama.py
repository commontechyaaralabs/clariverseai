# EU Banking Trouble Ticket Content Generator - Ollama Version
import os
import random
import time
import json
import requests
import signal
import sys
import multiprocessing
import logging
import re
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from faker import Faker
import backoff
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import atexit
import psutil
from pathlib import Path
from pymongo import UpdateOne
import traceback

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
TICKET_COLLECTION = "tickets_new"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"ticket_generator_ollama_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"progress_{timestamp}.log"
CHECKPOINT_FILE = LOG_DIR / f"checkpoint_{timestamp}.json"

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

# Configuration for Ollama
OLLAMA_MODEL = "gemma3:27b"
BATCH_SIZE = 1
MAX_WORKERS = 3  # Concurrent workers
REQUEST_TIMEOUT = 300
MAX_RETRIES = 5
RETRY_DELAY = 3
BATCH_DELAY = 2.0
API_CALL_DELAY = 0.5
CHECKPOINT_SAVE_INTERVAL = 5
MAX_RETRY_ATTEMPTS_PER_TICKET = 10

# Ollama setup
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "7eb2c60fcd3740cea657c8d109ff9016af894d2a2c112954bc3aff033c117736")
OLLAMA_URL = "http://34.147.17.26:16637/api/chat"

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
client = None
db = None
ticket_col = None

# Custom JSON encoder to handle ObjectId serialization
class ObjectIdEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

fake = Faker()

# Thread-safe counters
class LoggingCounter:
    def __init__(self, name):
        self._value = 0
        self._lock = threading.Lock()
        self._name = name
    
    def increment(self):
        with self._lock:
            self._value += 1
            if self._value % 10 == 0:
                progress_logger.info(f"{self._name}: {self._value}")
            return self._value
    
    @property
    def value(self):
        with self._lock:
            return self._value

success_counter = LoggingCounter("SUCCESS_COUNT")
failure_counter = LoggingCounter("FAILURE_COUNT")
update_counter = LoggingCounter("UPDATE_COUNT")

# Performance Monitor
class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.tickets_processed = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self._lock = threading.Lock()
    
    def record_success(self, total_tickets=None):
        with self._lock:
            self.successful_requests += 1
            self.tickets_processed += 1
            self.log_progress(total_tickets)
    
    def record_failure(self, total_tickets=None):
        with self._lock:
            self.failed_requests += 1
            self.log_progress(total_tickets)
    
    def log_progress(self, total_tickets=None):
        if self.tickets_processed % 50 == 0 and self.tickets_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.tickets_processed / elapsed if elapsed > 0 else 0
            remaining_tickets = (total_tickets - self.tickets_processed) if total_tickets else 0
            eta = remaining_tickets / rate if rate > 0 and remaining_tickets > 0 else 0
            
            success_rate = self.successful_requests/(self.successful_requests + self.failed_requests)*100 if (self.successful_requests + self.failed_requests) > 0 else 0
            logger.info(f"Performance: {self.tickets_processed}/{total_tickets} tickets, {rate:.1f}/sec ({rate*3600:.0f}/hour), {success_rate:.1f}% success" + (f", ETA: {eta/3600:.1f}h" if eta > 0 else ""))

performance_monitor = PerformanceMonitor()

# Checkpoint Manager
class CheckpointManager:
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file
        self.processed_tickets = set()
        self.failed_tickets = set()
        self.retry_attempts = {}
        self.stats = {
            'start_time': time.time(),
            'processed_count': 0,
            'success_count': 0,
            'failure_count': 0,
            'retry_count': 0
        }
        self._lock = threading.Lock()
        self.load_checkpoint()
    
    def load_checkpoint(self):
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.processed_tickets = set(data.get('processed_tickets', []))
                    self.failed_tickets = set(data.get('failed_tickets', []))
                    self.retry_attempts = data.get('retry_attempts', {})
                    self.stats.update(data.get('stats', {}))
                logger.info(f"Loaded checkpoint: {len(self.processed_tickets)} processed, {len(self.failed_tickets)} failed, {len(self.retry_attempts)} with retry attempts")
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    
    def save_checkpoint(self):
        with self._lock:
            try:
                checkpoint_data = {
                    'processed_tickets': list(self.processed_tickets),
                    'failed_tickets': list(self.failed_tickets),
                    'retry_attempts': self.retry_attempts,
                    'stats': self.stats,
                    'timestamp': datetime.now().isoformat()
                }
                with open(self.checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
            except Exception as e:
                logger.error(f"Could not save checkpoint: {e}")
    
    def is_processed(self, ticket_id):
        return str(ticket_id) in self.processed_tickets
    
    def increment_retry(self, ticket_id):
        with self._lock:
            ticket_id_str = str(ticket_id)
            if ticket_id_str not in self.retry_attempts:
                self.retry_attempts[ticket_id_str] = 0
            self.retry_attempts[ticket_id_str] += 1
            self.stats['retry_count'] += 1
            return self.retry_attempts[ticket_id_str]
    
    def get_retry_count(self, ticket_id):
        return self.retry_attempts.get(str(ticket_id), 0)
    
    def mark_processed(self, ticket_id, success=True):
        with self._lock:
            ticket_id_str = str(ticket_id)
            self.processed_tickets.add(ticket_id_str)
            self.stats['processed_count'] += 1
            
            if success:
                self.stats['success_count'] += 1
                self.failed_tickets.discard(ticket_id_str)
                if ticket_id_str in self.retry_attempts:
                    del self.retry_attempts[ticket_id_str]
            else:
                self.stats['failure_count'] += 1
                self.failed_tickets.add(ticket_id_str)
            
            if self.stats['processed_count'] % CHECKPOINT_SAVE_INTERVAL == 0:
                self.save_checkpoint()

checkpoint_manager = CheckpointManager(CHECKPOINT_FILE)

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
    global client, db, ticket_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        ticket_col = db[TICKET_COLLECTION]
        
        # Create indexes for better performance
        ticket_col.create_index("_id")
        ticket_col.create_index("dominant_topic")
        ticket_col.create_index("priority")
        ticket_col.create_index("urgency")
        ticket_col.create_index("resolution_status")
        ticket_col.create_index("category")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def generate_realistic_banking_details(ticket_record=None):
    """Generate realistic banking details for use in trouble tickets"""
    details = {
        'ticket_number': f"TKT-{random.randint(100000, 999999)}",
        'account_number': f"{random.randint(100000000000, 999999999999)}",
        'customer_id': f"CID{random.randint(100000, 999999)}",
        'system_name': random.choice(['CoreBanking', 'PaymentHub', 'OnlineBanking', 'MobileApp']),
        'amount': f"{random.randint(100, 50000)}.{random.randint(10, 99)}",
        'currency': random.choice(['EUR', 'GBP', 'USD']),
        'error_code': f"ERR_{random.randint(1000, 9999)}",
        'date': fake.date_between(start_date='-7d', end_date='today').strftime('%d/%m/%Y'),
        'time': f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}"
    }
    
    # Get customer name from ticket record if available
    if ticket_record and ticket_record.get('thread', {}).get('participants'):
        for participant in ticket_record['thread']['participants']:
            if participant.get('type') == 'from' and participant.get('name'):
                details['customer_name'] = participant['name']
                break
    
    if 'customer_name' not in details:
        customer_names = ["John Smith", "Sarah Johnson", "Michael Brown", "Emma Wilson", "David Jones"]
        details['customer_name'] = random.choice(customer_names)
    
    return details

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=600,
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

def generate_optimized_ticket_prompt(ticket_data):
    """Generate optimized prompt for ticket content generation"""
    
    # Extract data from ticket record - ALL EXISTING FIELDS FROM tickets_new COLLECTION
    dominant_topic = ticket_data.get('dominant_topic')
    subtopics = ticket_data.get('subtopics')
    messages = ticket_data.get('messages', [])
    message_count = len(messages) if messages else ticket_data.get('thread', {}).get('message_count', 2)
    
    # EXISTING FIELDS FROM tickets_new collection - USE THESE EXACT VALUES
    urgency = ticket_data.get('urgency')
    follow_up_required = ticket_data.get('follow_up_required')
    action_pending_status = ticket_data.get('action_pending_status')
    action_pending_from = ticket_data.get('action_pending_from')
    priority = ticket_data.get('priority')
    resolution_status = ticket_data.get('resolution_status')
    overall_sentiment = ticket_data.get('sentiment')  # Note: field is "sentiment" not "overall_sentiment"
    category = ticket_data.get('category')
    
    banking_details = generate_realistic_banking_details(ticket_data)
    
    urgency_context = "URGENT" if urgency else "NON-URGENT"
    
    # Map category to ticket raising context and direction
    if category == "External":
        category_context = "external customer or third-party ticket raised TO the bank"
        ticket_direction = "FROM external parties (customers, vendors, regulatory bodies, other banks) TO EU Bank"
        raiser_context = "External party (customer, vendor, regulatory body, or other bank)"
        handler_context = "EU Bank department or employee"
    else:  # Internal
        category_context = "internal inter-company inter-bank ticket raised WITHIN the bank"
        ticket_direction = "WITHIN EU Bank between departments, employees, or internal systems"
        raiser_context = "EU Bank department or employee"
        handler_context = "EU Bank department or employee"
    
    # Extract message dates from existing database records
    message_dates = []
    if messages:
        for message in messages:
            if isinstance(message, dict):
                headers = message.get('headers', {})
                if isinstance(headers, dict):
                    date = headers.get('date')
                    if date:
                        message_dates.append(date)
    
    # Get thread-level dates
    thread = ticket_data.get('thread', {})
    first_message_at = thread.get('first_message_at')
    last_message_at = thread.get('last_message_at')
    
    # Determine action pending context
    action_pending_context = ""
    if action_pending_status == "yes" and action_pending_from:
        if action_pending_from.lower() == "customer":
            action_pending_context = "The customer needs to take the next action (provide documents, respond to request, complete process, etc.)"
        elif action_pending_from.lower() == "company" or action_pending_from.lower() == "bank":
            action_pending_context = "The bank needs to take the next action (process request, review documents, provide response, etc.)"
        else:
            action_pending_context = f"The {action_pending_from} needs to take the next action"
    elif action_pending_status == "yes":
        action_pending_context = "An action is pending but the responsible party is unclear"
    else:
        action_pending_context = "No action is pending - process is complete or ongoing"
    
    # Build message generation instructions with actual dates
    message_instructions = []
    for i in range(message_count):
        # Get the actual date for this message
        message_date = message_dates[i] if i < len(message_dates) else (last_message_at if last_message_at else "2025-01-01T12:00:00")
        
        # Check for day shift (different date from previous message)
        day_shift_instruction = ""
        if i > 0:
            prev_message_date = message_dates[i-1] if i-1 < len(message_dates) else first_message_at
            if prev_message_date and message_date:
                try:
                    prev_date = datetime.fromisoformat(prev_message_date.replace('Z', '+00:00')).date() if 'Z' in prev_message_date else datetime.fromisoformat(prev_message_date).date()
                    curr_date = datetime.fromisoformat(message_date.replace('Z', '+00:00')).date() if 'Z' in message_date else datetime.fromisoformat(message_date).date()
                    if prev_date != curr_date:
                        day_shift_instruction = " IMPORTANT: This message is on a different day than the previous message. Start with a natural day shift greeting like 'Good morning!', 'Following up on our discussion yesterday', 'Hi again', or acknowledge the time gap."
                except:
                    pass
        
        if i % 2 == 0:  # Customer message (ticket raiser)
            message_instructions.append(f"Message {i+1} ({raiser_context.upper()} at {message_date}): Message 300-400 words with flexible conversational format, natural flow, proper line breaks (\\n\\n), realistic banking details.{day_shift_instruction}")
        else:  # Company message (ticket handler)
            message_instructions.append(f"Message {i+1} ({handler_context.upper()} at {message_date}): Response 300-400 words starting with 'Ticket Reference: {banking_details['ticket_number']}', flexible professional format, natural flow, proper line breaks (\\n\\n).{day_shift_instruction}")
    
    messages_timeline = "\n".join(message_instructions)
    
    # Build sentiment description
    sentiment_desc = ""
    if overall_sentiment is not None:
        if overall_sentiment == 5:
            sentiment_desc = "Extreme frustration throughout ALL messages"
        elif overall_sentiment == 4:
            sentiment_desc = "Clear anger/frustration"
        elif overall_sentiment == 3:
            sentiment_desc = "Moderate concern/unease"
        elif overall_sentiment == 2:
            sentiment_desc = "Slight irritation/impatience"
        elif overall_sentiment == 1:
            sentiment_desc = "Calm professional baseline"
        else:
            sentiment_desc = "Positive satisfied communication"
    
    # Build follow-up description
    follow_up_desc = "End with open-ended scenarios" if follow_up_required == "yes" else "End with complete resolution"
    
    # Build action description
    action_desc = "Show waiting scenarios" if action_pending_status == "yes" else "Show completed processes"
    
    # Build action pending from description
    action_from_desc = ""
    if action_pending_status == "yes" and action_pending_from:
        if action_pending_from.lower() == "customer":
            action_from_desc = "End with customer needing to respond/take action"
        elif action_pending_from.lower() == "company" or action_pending_from.lower() == "bank":
            action_from_desc = "End with bank needing to respond/take action"
        else:
            action_from_desc = "End with appropriate party needing to take action"
    elif action_pending_status == "yes":
        action_from_desc = "End with appropriate party needing to take action"
    else:
        action_from_desc = "End with completed process"
    
    # Determine ending requirements
    ending_requirements = []
    if action_pending_status == "yes" and action_pending_from:
        if action_pending_from.lower() == "customer":
            ending_requirements.append("Last message should indicate waiting for customer response/action")
        elif action_pending_from.lower() == "company" or action_pending_from.lower() == "bank":
            ending_requirements.append("Last message should indicate bank needs to take action")
        else:
            ending_requirements.append(f"Last message should indicate {action_pending_from} needs to take action")
    
    if follow_up_required == "yes":
        ending_requirements.append("Conversation should end with open-ended scenario requiring follow-up")
    
    ending_str = " | ".join(ending_requirements) if ending_requirements else "Conversation should end with complete resolution"
    
    prompt = f"""Generate EU banking trouble ticket JSON with {message_count} messages.

**METADATA:** Topic:{dominant_topic} | Subtopic:{subtopics} | Urgency:{urgency} | Priority:{priority} | Follow-up:{follow_up_required} | Action:{action_pending_status} | Action From:{action_pending_from} | Resolution:{resolution_status} | Sentiment:{overall_sentiment}/5 | Category:{category}

**CONTEXT:**
Ticket: {banking_details['ticket_number']} | Customer: {banking_details['customer_name']} | Account: {banking_details['account_number']} | System: {banking_details['system_name']}

**TIMELINE INFORMATION:**
- First message date: {first_message_at}
- Last message date: {last_message_at}
- Total message count: {message_count}
- Message timeline:
{messages_timeline}

**PRIORITY LEVEL DEFINITION:**
- P1-Critical: Business stop → must resolve NOW (follow-up within 24-48 hours)
- P2-High: Major issue, limited users impacted, needs fast action (follow-up within 2-7 days)
- P3-Medium: Standard issues/requests, manageable timelines (follow-up within 1-2 weeks)
- P4-Low: Minor issues, no major business impact (follow-up within 2-4 weeks)
- P5-Very Low: Informational, FYI, archival (follow-up within 1-2 months)

**CATEGORY CONTEXT:** {category_context}
- Ticket Direction: {ticket_direction}
- Ticket Raiser: {raiser_context}
- Ticket Handler: {handler_context}

**ACTION PENDING CONTEXT:** {action_pending_context}

**TEAMS AVAILABLE:**
- customerservice@eubank.com (General support)
- financialcrime@eubank.com (Fraud, suspicious activity)
- loansupport@eubank.com (Loans, credit cards)
- kyc@eubank.com (Identity, compliance)
- cardoperations@eubank.com (Card issues)
- digitalbanking@eubank.com (App, online banking)
- internalithelpdesk@eubank.com (Internal IT)

**RULES:**
- Category {category}: {category_context}
  * External: Ticket raised BY {raiser_context}, handled BY {handler_context}
  * Internal: Ticket raised BY {raiser_context}, handled BY {handler_context}
- Sentiment {overall_sentiment}/5: {sentiment_desc}
- Bank employees: ALWAYS calm, professional, helpful
- Title: Professional ticket title 80-120 characters - match {urgency_context} tone
- Priority: Must match urgency={urgency} (P1-Critical/P2-High for urgent, P3-Medium/P4-Low/P5-Very Low for non-urgent)
- Follow-up {follow_up_required}: {follow_up_desc}
- Action {action_pending_status}: {action_desc}
- Action Pending From {action_pending_from}: {action_from_desc}

**DATE-BASED CONVERSATION FLOW:**
- Use the EXACT dates provided for each message
- CRITICAL: If there are time gaps between messages, create natural conversation breaks
- For day shifts (different dates), ALWAYS start with greetings like:
  * "Good morning!" (for morning messages)
  * "Following up on our discussion yesterday" (for next-day follow-ups)
  * "Hi again" (for casual follow-ups)
  * "Quick update on..." (for urgent matters)
- For same-day gaps (hours apart), use phrases like:
  * "Got an update"
  * "Quick follow-up"
  * "Just checking in"
- Consider business hours and urgency when crafting messages
- ALWAYS acknowledge time gaps with appropriate conversation starters
- NEVER continue a conversation across days without acknowledging the time gap

**MESSAGE STRUCTURE:** Customer message 300-400 words → Company response 300-400 words (alternating pattern)
- Customer messages ({raiser_context}): Flexible conversational format with natural flow, proper line breaks (\\n\\n), realistic ticket details
- Company messages ({handler_context}): Professional format with "Ticket Reference: {banking_details['ticket_number']}" at start, natural flow, proper line breaks (\\n\\n)
- Use banking terminology, specific amounts, transaction IDs, account numbers, error codes
- Acknowledge time gaps naturally when messages are on different days
- IMPORTANT: First message MUST be from {raiser_context} raising the ticket, subsequent messages alternate with {handler_context} responses
- CRITICAL: Craft the conversation content to naturally lead to follow_up_reason and next_action_suggestion
  * For External: Include specific customer requests, pending items, missing documents, unresolved issues in conversation
  * For Internal: Include specific inter-department coordination needs, approvals needed, system issues, process steps in conversation

**ENDING REQUIREMENTS:** {ending_str}

**FOLLOW-UP AND NEXT ACTION GUIDELINES:**
- **follow_up_reason** = Simple, direct statement (20-50 words): Just state WHY follow-up is needed from the conversation
  * For External (category={category}): State what's pending from {raiser_context}"
  * For Internal (category={category}): State what's pending between departments"
  * NO extra explanation or context - just the core reason
  
- **next_action_suggestion** = 30-80 words deriving from conversation what {action_pending_from} should do next
  * For External (category={category}): What {raiser_context} or bank needs to do based on conversation
  * For Internal (category={category}): What requesting/handling department needs to do (use department names, NOT "customer")
  * Must be ACTIONABLE and SPECIFIC to conversation context

**OUTPUT:** {{
  "title": "Professional ticket title 80-120 characters matching {urgency_context} tone",
  "priority": "{priority}",
  "assigned_team_email": "exact_email@eubank.com",
  "ticket_summary": "Business summary 150-200 words describing issue, impact, details",
  "action_pending_status": "{action_pending_status}",
  "action_pending_from": {"customer|company|null" if action_pending_status == "yes" else "null"},
  "resolution_status": "{resolution_status}",
  "messages": [
    {{"content": "[Message from {raiser_context} - 300-400 words with flexible format, natural flow, proper line breaks (\\n\\n), realistic banking details. Acknowledge time gaps.]", "sender_type": "customer"}},
    {{"content": "[Response from {handler_context} - 300-400 words starting with 'Ticket Reference: {banking_details['ticket_number']}', flexible professional format, proper line breaks (\\n\\n). Acknowledge time gaps.]", "sender_type": "company"}}{"... continue alternating for " + str(message_count) + " total messages" if message_count > 2 else ""}
  ],
  "analysis": {{
    "ticket_summary": "[150-200 word business summary of the ENTIRE CONVERSATION]",
    "follow_up_reason": {"[Simple direct statement: WHY follow-up is needed. Just state the reason from conversation, no extra explanation. 20-50 words max.]" if follow_up_required == "yes" else "null"},
    "next_action_suggestion": {"[30-80 words: WHAT specific step {action_pending_from} needs to take based on conversation - the action recommendation]" if follow_up_required == "yes" and action_pending_status == "yes" else "null"},
    "follow_up_date": {"[Generate follow-up date after {last_message_at} based on priority={priority}. CRITICAL RULES: P1-Critical=SAME DAY or next business day (24-48 hours MAX), P2-High=2-5 business days (MUST be within 7 days MAX), P3-Medium=1-2 weeks, P4-Low=2-4 weeks, P5-Very Low=1-2 months. Format: YYYY-MM-DDTHH:MM:SS]" if follow_up_required == "yes" else "null"}
  }}
}}

Use EXACT metadata values. Implement concepts through natural scenarios. Generate authentic banking content with specific details.

**CRITICAL:**
- Generate Messages FIRST, then analyze them for follow-up reason and next action
- **follow_up_reason** = Simple direct statement (20-50 words max) stating WHY follow-up is needed - ONLY if follow_up_required="yes", otherwise "null"
  * Just state the reason, NO extra explanation
- **next_action_suggestion** = Detailed action (30-80 words) stating WHAT needs to be done - ONLY if follow_up_required="yes" AND action_pending_status="yes", otherwise "null"
  * For External tickets:
    - If action_pending_from="customer": What the {raiser_context} needs to do based on conversation
    - If action_pending_from="company" or "bank": What the bank/handling team needs to do based on conversation
  * For Internal tickets:
    - If action_pending_from="customer": What the requesting department/employee needs to do based on conversation (DO NOT use word "customer" - use "requesting department" or "employee name")
    - If action_pending_from="company" or "bank": What the handling department/employee needs to do based on conversation (use specific department names)
- **follow_up_date** = Date based on priority - ONLY if follow_up_required="yes", otherwise "null"
  * P1-Critical: SAME DAY or next business day (24-48 hours MAX from {last_message_at})
  * P2-High: 2-5 business days (MUST be within 7 days from {last_message_at})
  * P3-Medium: 1-2 weeks from {last_message_at}
  * P4-Low: 2-4 weeks from {last_message_at}
  * P5-Very Low: 1-2 months from {last_message_at}
- DO NOT generate message dates or thread dates - use EXISTING dates from database
- For category={category}: Use proper terminology ({raiser_context} raising ticket, {handler_context} handling ticket)

Generate now.
""".strip()
    
    return prompt

def generate_ticket_content(ticket_data):
    """Generate ticket content with Ollama"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    ticket_id = str(ticket_data.get('_id', 'unknown'))
    
    try:
        prompt = generate_optimized_ticket_prompt(ticket_data)
        
        response = call_ollama_with_backoff(prompt)
        
        if not response or not response.strip():
            raise ValueError("Empty response from LLM")
        
        # Clean and parse JSON
        reply = response.strip()
        
        # Remove markdown code fences more carefully
        reply = re.sub(r'^```(?:json)?\s*', '', reply)
        reply = re.sub(r'\s*```$', '', reply)
        
        json_start = reply.find('{')
        json_end = reply.rfind('}') + 1
        
        if json_start == -1 or json_end <= json_start:
            raise ValueError("No valid JSON found in LLM response")
        
        reply = reply[json_start:json_end].strip()
        
        try:
            result = json.loads(reply)
            if str(ticket_id).endswith('0') or str(ticket_id).endswith('5'):
                logger.info(f"Ticket {ticket_id}: JSON parsing successful. Keys: {list(result.keys())}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing failed for ticket {ticket_id}. Raw response: {reply[:300]}...")
            raise ValueError(f"Invalid JSON response from LLM: {json_err}")
        
        # Validate required fields
        required_fields = ['title', 'priority', 'assigned_team_email', 'messages', 'analysis']
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            logger.error(f"Ticket {ticket_id}: Missing required fields: {missing_fields}")
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Validate analysis fields
        analysis_fields = ['ticket_summary', 'follow_up_reason', 'next_action_suggestion', 'follow_up_date']
        for field in analysis_fields:
            if field not in result['analysis']:
                logger.error(f"Ticket {ticket_id}: Missing analysis field: {field}")
                raise ValueError(f"Missing analysis field: {field}")
        
        # Validate content-based fields are specific and within word count when required
        if ticket_data.get('follow_up_required') == "yes":
            follow_up_reason = result['analysis'].get('follow_up_reason')
            if follow_up_reason and follow_up_reason != "null" and isinstance(follow_up_reason, str):
                word_count = len(follow_up_reason.strip().split())
                if word_count < 15:
                    logger.warning(f"Ticket {ticket_id}: follow_up_reason too short ({word_count} words, need 20-50): {follow_up_reason[:100]}...")
                elif word_count > 50:
                    logger.warning(f"Ticket {ticket_id}: follow_up_reason too long ({word_count} words, need 20-50): {follow_up_reason[:100]}...")
        
        if ticket_data.get('follow_up_required') == "yes" and ticket_data.get('action_pending_status') == "yes":
            next_action = result['analysis'].get('next_action_suggestion')
            if next_action and next_action != "null" and isinstance(next_action, str):
                word_count = len(next_action.strip().split())
                if word_count < 30:
                    logger.warning(f"Ticket {ticket_id}: next_action_suggestion too short ({word_count} words, need 30-80): {next_action[:100]}...")
                elif word_count > 80:
                    logger.warning(f"Ticket {ticket_id}: next_action_suggestion too long ({word_count} words, need 30-80): {next_action[:100]}...")
        
        # Validate messages count
        message_count = ticket_data.get('thread', {}).get('message_count', 2)
        if len(result['messages']) != message_count:
            logger.warning(f"Ticket {ticket_id}: Expected {message_count} messages, got {len(result['messages'])}")
            if len(result['messages']) > message_count:
                result['messages'] = result['messages'][:message_count]
        
        generation_time = time.time() - start_time
        
        # Log success
        success_info = {
            'ticket_id': ticket_id,
            'dominant_topic': ticket_data.get('dominant_topic'),
            'title': result['title'],
            'priority': result['priority'],
            'urgency': ticket_data.get('urgency'),
            'resolution_status': result.get('resolution_status'),
            'generation_time': generation_time
        }
        success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
        
        return result
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'ticket_id': ticket_id,
            'dominant_topic': ticket_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
        raise

def populate_email_addresses(ticket_record, assigned_team_email, generated_messages):
    """Populate empty email addresses in participants and messages"""
    updates = {}
    
    # Get customer info
    customer_email = None
    customer_name = None
    if ticket_record.get('thread', {}).get('participants'):
        for participant in ticket_record['thread']['participants']:
            if participant.get('type') == 'from' and participant.get('email'):
                customer_email = participant['email']
                customer_name = participant.get('name')
                break
    
    # Update participants
    if ticket_record.get('thread', {}).get('participants'):
        for i, participant in enumerate(ticket_record['thread']['participants']):
            if participant.get('type') == 'to' and not participant.get('email'):
                updates[f'thread.participants.{i}.email'] = assigned_team_email
                updates[f'thread.participants.{i}.name'] = assigned_team_email.split('@')[0].replace('.', ' ').title()
    
    # Update messages with generated content
    if ticket_record.get('messages') and generated_messages:
        for msg_idx, message in enumerate(ticket_record['messages']):
            if msg_idx < len(generated_messages):
                generated_msg = generated_messages[msg_idx]
                
                updates[f'messages.{msg_idx}.body.text.plain'] = generated_msg['content']
                
                if generated_msg['sender_type'] == 'customer':
                    if message.get('headers', {}).get('from') and len(message['headers']['from']) > 0:
                        if not message['headers']['from'][0].get('email') and customer_email:
                            updates[f'messages.{msg_idx}.headers.from.0.email'] = customer_email
                            updates[f'messages.{msg_idx}.headers.from.0.name'] = customer_name
                    
                    if message.get('headers', {}).get('to') and len(message['headers']['to']) > 0:
                        if not message['headers']['to'][0].get('email'):
                            updates[f'messages.{msg_idx}.headers.to.0.email'] = assigned_team_email
                            updates[f'messages.{msg_idx}.headers.to.0.name'] = assigned_team_email.split('@')[0].replace('.', ' ').title()
                
                else:  # company message
                    if message.get('headers', {}).get('from') and len(message['headers']['from']) > 0:
                        if not message['headers']['from'][0].get('email'):
                            updates[f'messages.{msg_idx}.headers.from.0.email'] = assigned_team_email
                            updates[f'messages.{msg_idx}.headers.from.0.name'] = assigned_team_email.split('@')[0].replace('.', ' ').title()
                    
                    if message.get('headers', {}).get('to') and len(message['headers']['to']) > 0:
                        if not message['headers']['to'][0].get('email') and customer_email:
                            updates[f'messages.{msg_idx}.headers.to.0.email'] = customer_email
                            updates[f'messages.{msg_idx}.headers.to.0.name'] = customer_name
    
    return updates

def process_single_ticket(ticket_record, total_tickets=None, retry_attempt=0):
    """Process a single ticket with comprehensive retry logic"""
    if shutdown_flag.is_set():
        return None
    
    ticket_id = str(ticket_record.get('_id', 'unknown'))
    
    for attempt in range(MAX_RETRY_ATTEMPTS_PER_TICKET):
        if shutdown_flag.is_set():
            return None
        
        try:
            if attempt > 0:
                retry_wait = min(120, RETRY_DELAY * (2 ** (attempt - 1)))
                logger.info(f"Ticket {ticket_id}: Retry attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS_PER_TICKET} after {retry_wait}s wait")
                time.sleep(retry_wait)
                checkpoint_manager.increment_retry(ticket_id)
            
            result = _process_single_ticket_internal(ticket_record, total_tickets)
            
            if result:
                if attempt > 0:
                    logger.info(f"Ticket {ticket_id}: SUCCESS after {attempt + 1} attempts!")
                return result
            else:
                logger.warning(f"Ticket {ticket_id}: Attempt {attempt + 1} returned no result, retrying...")
                continue
                
        except Exception as e:
            error_msg = ""
            if e and hasattr(e, '__str__'):
                try:
                    error_msg = str(e).lower()
                except:
                    error_msg = ""
            
            logger.warning(f"Ticket {ticket_id}: Error on attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS_PER_TICKET}: {str(e)[:100] if e else 'unknown error'}")
            
            if error_msg and ("rate limit" in error_msg or "429" in error_msg):
                wait_time = min(120, 30 * (attempt + 1))
                logger.info(f"Ticket {ticket_id}: Rate limit detected, waiting {wait_time}s before retry")
                time.sleep(wait_time)
            
            if attempt < MAX_RETRY_ATTEMPTS_PER_TICKET - 1:
                continue
            else:
                logger.error(f"Ticket {ticket_id}: Failed after {MAX_RETRY_ATTEMPTS_PER_TICKET} attempts")
    
    logger.error(f"Ticket {ticket_id}: FAILED after {MAX_RETRY_ATTEMPTS_PER_TICKET} attempts")
    performance_monitor.record_failure(total_tickets)
    failure_counter.increment()
    checkpoint_manager.mark_processed(ticket_id, success=False)
    return None

def _process_single_ticket_internal(ticket_record, total_tickets=None):
    """Internal ticket processing logic"""
    ticket_id = str(ticket_record.get('_id', 'unknown'))
    
    try:
        # Generate content
        ticket_content = generate_ticket_content(ticket_record)
        
        if not ticket_content:
            performance_monitor.record_failure(total_tickets)
            return None
        
        if str(ticket_id).endswith('0'):
            logger.info(f"Ticket {ticket_id}: Generated content keys: {list(ticket_content.keys()) if isinstance(ticket_content, dict) else 'Not a dict'}")
        
        # Prepare update document
        update_doc = {}
        
        # Update top-level fields from LLM response
        update_doc['title'] = ticket_content.get('title')
        update_doc['priority'] = ticket_content.get('priority')
        update_doc['assigned_team_email'] = ticket_content.get('assigned_team_email')
        update_doc['action_pending_status'] = ticket_content.get('action_pending_status')
        update_doc['action_pending_from'] = ticket_content.get('action_pending_from')
        update_doc['resolution_status'] = ticket_content.get('resolution_status')
        
        # Update messages with generated content
        if 'messages' in ticket_content:
            messages = ticket_content['messages']
            if isinstance(messages, list):
                for i, message in enumerate(messages):
                    if isinstance(message, dict):
                        update_doc[f'messages.{i}.body.text.plain'] = message.get('content')
        
        # Update analysis fields from LLM response
        if 'analysis' in ticket_content:
            analysis = ticket_content['analysis']
            if isinstance(analysis, dict):
                update_doc['ticket_summary'] = analysis.get('ticket_summary')
                
                # Get follow_up_required and action_pending_status from ticket_record
                ticket_follow_up_required = ticket_record.get('follow_up_required')
                ticket_action_pending_status = ticket_record.get('action_pending_status')
                
                # Handle follow_up_reason based on follow_up_required
                if ticket_follow_up_required == "yes":
                    follow_up_reason = analysis.get('follow_up_reason')
                    update_doc['follow_up_reason'] = follow_up_reason
                    
                    # Log content-based follow_up_reason for verification
                    if str(ticket_id).endswith('0') or str(ticket_id).endswith('5'):
                        category = ticket_record.get('category', 'Unknown')
                        word_count = len(str(follow_up_reason).strip().split()) if follow_up_reason else 0
                        logger.info(f"Ticket {ticket_id} ({category}): Follow-up reason ({word_count} words): {str(follow_up_reason)[:150]}...")
                else:
                    update_doc['follow_up_reason'] = None
                
                # Handle follow_up_date based on follow_up_required
                if ticket_follow_up_required == "yes":
                    follow_up_date = analysis.get('follow_up_date')
                    update_doc['follow_up_date'] = follow_up_date
                else:
                    update_doc['follow_up_date'] = None
                
                # Handle next_action_suggestion
                if ticket_follow_up_required == "yes" and ticket_action_pending_status == "yes":
                    next_action = analysis.get('next_action_suggestion')
                    update_doc['next_action_suggestion'] = next_action
                    
                    # Log content-based next_action_suggestion for verification
                    if str(ticket_id).endswith('0') or str(ticket_id).endswith('5'):
                        category = ticket_record.get('category', 'Unknown')
                        action_from = ticket_record.get('action_pending_from', 'Unknown')
                        word_count = len(str(next_action).strip().split()) if next_action else 0
                        logger.info(f"Ticket {ticket_id} ({category}): Next action for {action_from} ({word_count} words): {str(next_action)[:150]}...")
                else:
                    update_doc['next_action_suggestion'] = None
        
        # Add email and message updates (only content, not dates)
        email_updates = populate_email_addresses(ticket_record, ticket_content['assigned_team_email'], ticket_content['messages'])
        update_doc.update(email_updates)
        
        # Set titles programmatically
        main_title = ticket_content['title']
        update_doc['thread.ticket_title'] = main_title
        
        # Update message titles (not dates - they already exist)
        if ticket_content['messages']:
            for i, message in enumerate(ticket_content['messages']):
                if message['sender_type'] == 'customer':
                    update_doc[f'messages.{i}.title'] = main_title
                    update_doc[f'messages.{i}.headers.ticket_title'] = main_title
                else:
                    update_doc[f'messages.{i}.title'] = f"RE: {main_title}"
                    update_doc[f'messages.{i}.headers.ticket_title'] = f"RE: {main_title}"
        
        # Add LLM processing tracking
        update_doc['llm_processed'] = True
        update_doc['llm_processed_at'] = datetime.now().isoformat()
        update_doc['llm_model_used'] = OLLAMA_MODEL
        
        performance_monitor.record_success(total_tickets)
        success_counter.increment()
        
        return {
            'ticket_id': str(ticket_record['_id']),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Ticket {ticket_id} internal processing failed: {str(e)[:100]}")
        raise

def save_batch_to_database(batch_updates):
    """Save batch updates to database with optimized bulk operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        if len(batch_updates) > 5:
            logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        bulk_operations = []
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"_id": ObjectId(update_data['ticket_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
        
        if bulk_operations:
            try:
                result = ticket_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.matched_count
                
                update_counter._value += updated_count
                
                if len(batch_updates) > 5:
                    logger.info(f"Successfully saved {updated_count} records to database")
                    progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Fallback to smaller chunks
                chunk_size = 10
                individual_success = 0
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i:i + chunk_size]
                    try:
                        chunk_operations = []
                        for update_data in chunk:
                            operation = UpdateOne(
                                filter={"_id": ObjectId(update_data['ticket_id'])},
                                update={"$set": update_data['update_doc']}
                            )
                            chunk_operations.append(operation)
                        
                        chunk_result = ticket_col.bulk_write(chunk_operations, ordered=False)
                        individual_success += chunk_result.matched_count
                    except Exception as chunk_error:
                        logger.error(f"Chunk update failed: {chunk_error}")
                
                logger.info(f"Fallback: {individual_success} records saved in chunks")
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def process_tickets_optimized():
    """Main optimized processing function for ticket generation"""
    logger.info("Starting Optimized EU Banking Trouble Ticket Content Generation...")
    logger.info("Focus: Processing records with NULL/empty message body content")
    logger.info(f"Collection: {TICKET_COLLECTION}")
    logger.info(f"Optimized Configuration:")
    logger.info(f"  Max Workers: {MAX_WORKERS}")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info(f"  API Delay: {API_CALL_DELAY}s")
    logger.info(f"  Request Timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"  Model: {OLLAMA_MODEL}")
    
    # Test connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        return
    
    # Get tickets to process
    try:
        query = {
            "$and": [
                {"_id": {"$exists": True}},
                {"thread": {"$exists": True}},
                {"messages": {"$exists": True, "$ne": None, "$ne": []}},
                {
                    "$or": [
                        {"messages.body.text.plain": {"$eq": None}},
                        {"messages.body.text.plain": {"$eq": ""}},
                        {"messages.body.text.plain": {"$exists": False}},
                        {"messages.body.text": {"$exists": False}},
                        {"messages.body": {"$exists": False}},
                        {"messages": {"$size": 0}}
                    ]
                }
            ]
        }
        
        if checkpoint_manager.processed_tickets:
            processed_ids = [ObjectId(tid) for tid in checkpoint_manager.processed_tickets if ObjectId.is_valid(tid)]
            query["_id"] = {"$nin": processed_ids}
        
        # Check ticket status
        total_tickets_in_db = ticket_col.count_documents({})
        tickets_processed_by_llm = ticket_col.count_documents({"llm_processed": True})
        tickets_with_basic_fields = ticket_col.count_documents({
            "$and": [
                {"_id": {"$exists": True}},
                {"thread": {"$exists": True}},
                {"messages": {"$exists": True, "$ne": None, "$ne": []}}
            ]
        })
        
        tickets_with_null_body_content = ticket_col.count_documents({
            "$and": [
                {"messages": {"$exists": True, "$ne": None, "$ne": []}},
                {
                    "$or": [
                        {"messages.body.text.plain": {"$eq": None}},
                        {"messages.body.text.plain": {"$eq": ""}},
                        {"messages.body.text.plain": {"$exists": False}}
                    ]
                }
            ]
        })
        
        tickets_needing_processing = ticket_col.count_documents(query)
        
        completion_percentage = (tickets_processed_by_llm / tickets_with_basic_fields * 100) if tickets_with_basic_fields > 0 else 0
        pending_percentage = (tickets_with_null_body_content / tickets_with_basic_fields * 100) if tickets_with_basic_fields > 0 else 0
        
        logger.info(f"Database Status:")
        logger.info(f"  Total tickets in DB: {total_tickets_in_db}")
        logger.info(f"  Tickets with required basic fields: {tickets_with_basic_fields}")
        logger.info(f"  Tickets with NULL/empty message body content: {tickets_with_null_body_content}")
        logger.info(f"  Tickets processed by LLM: {tickets_processed_by_llm}")
        logger.info(f"  Tickets needing processing: {tickets_needing_processing}")
        logger.info(f"  Overall Progress: {completion_percentage:.1f}% completed, {pending_percentage:.1f}% pending")
        
        ticket_records = list(ticket_col.find(query).batch_size(100))
        total_tickets = len(ticket_records)
        
        if total_tickets == 0:
            logger.info("No tickets found that need processing!")
            logger.info("All tickets appear to have been processed by LLM already.")
            return
        
        logger.info(f"Found {total_tickets} tickets that need LLM processing")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_tickets)} tickets")
        
        progress_logger.info(f"SESSION_START: total_tickets={total_tickets}, completed={tickets_processed_by_llm}, pending={tickets_with_null_body_content}, completion_rate={completion_percentage:.1f}%")
        
    except Exception as e:
        logger.error(f"Error fetching ticket records: {e}")
        return
    
    # Process tickets
    total_updated = 0
    batch_updates = []
    
    try:
        # Process with ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {}
            processed_count = 0
            
            for ticket in ticket_records:
                if shutdown_flag.is_set():
                    logger.info("Shutdown requested, stopping processing")
                    break
                
                ticket_id = str(ticket.get('_id'))
                
                if not checkpoint_manager.is_processed(ticket_id):
                    future = executor.submit(process_single_ticket, ticket, total_tickets)
                    futures[future] = ticket_id
                else:
                    logger.info(f"Skipping already processed ticket: {ticket_id}")
                    processed_count += 1
            
            logger.info(f"Created {len(futures)} tasks for processing")
            
            # Collect results
            for future in as_completed(futures):
                if shutdown_flag.is_set():
                    logger.info("Shutdown requested, stopping collection")
                    break
                
                ticket_id = futures[future]
                processed_count += 1
                
                try:
                    result = future.result()
                    
                    if result:
                        batch_updates.append(result)
                        checkpoint_manager.mark_processed(result['ticket_id'], success=True)
                        
                        # Save batch when we have enough
                        if len(batch_updates) >= BATCH_SIZE:
                            saved_count = save_batch_to_database(batch_updates)
                            total_updated += saved_count
                            batch_updates = []
                    
                    # Progress update
                    if processed_count % 10 == 0:
                        progress_pct = (processed_count / total_tickets) * 100
                        remaining_tickets = total_tickets - processed_count
                        total_completed = tickets_processed_by_llm + total_updated
                        overall_completion = (total_completed / tickets_with_basic_fields * 100) if tickets_with_basic_fields > 0 else 0
                        
                        logger.info(f"Session Progress: {progress_pct:.1f}% ({processed_count}/{total_tickets}) - {remaining_tickets} remaining")
                        logger.info(f"Overall Progress: {overall_completion:.1f}% completed ({total_completed}/{tickets_with_basic_fields} total)")
                        
                        progress_logger.info(f"PROGRESS_UPDATE: session={progress_pct:.1f}%, overall={overall_completion:.1f}%, processed_this_session={total_updated}, remaining={remaining_tickets}")
                    
                    performance_monitor.log_progress(total_tickets)
                    
                except Exception as e:
                    logger.error(f"Error processing ticket {ticket_id}: {e}")
                    checkpoint_manager.mark_processed(ticket_id, success=False)
                
                # Brief pause between tickets
                if not shutdown_flag.is_set():
                    time.sleep(BATCH_DELAY)
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
        
        checkpoint_manager.save_checkpoint()
        
        if shutdown_flag.is_set():
            logger.info("Processing interrupted gracefully!")
        else:
            logger.info("Optimized ticket content generation complete!")
        
        final_total_completed = tickets_processed_by_llm + total_updated
        final_completion_percentage = (final_total_completed / tickets_with_basic_fields * 100) if tickets_with_basic_fields > 0 else 0
        final_pending = tickets_with_basic_fields - final_total_completed
        
        logger.info(f"Final Results:")
        logger.info(f"  Total tickets updated this session: {total_updated}")
        logger.info(f"  Total tickets completed (all time): {final_total_completed}")
        logger.info(f"  Total tickets pending: {final_pending}")
        logger.info(f"  Overall completion rate: {final_completion_percentage:.1f}%")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        logger.info(f"  Total retry attempts: {checkpoint_manager.stats.get('retry_count', 0)}")
        
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_ticket = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per ticket: {avg_time_per_ticket:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} tickets/hour" if total_time > 0 else "Processing rate: N/A")
        
        progress_logger.info(f"FINAL_SUMMARY: session_updated={total_updated}, total_completed={final_total_completed}, pending={final_pending}, completion_rate={final_completion_percentage:.1f}%, success={success_counter.value}, failures={failure_counter.value}, retries={checkpoint_manager.stats.get('retry_count', 0)}, total_time={total_time/3600:.2f}h, rate={success_counter.value/(total_time/3600):.0f}/h" if total_time > 0 else f"FINAL_SUMMARY: session_updated={total_updated}, total_completed={final_total_completed}, pending={final_pending}")
        
    except Exception as e:
        logger.error(f"Unexpected error in main processing: {e}")
        logger.error(traceback.format_exc())
    finally:
        checkpoint_manager.save_checkpoint()

def test_ollama_connection():
    """Test Ollama connection"""
    try:
        logger.info(f"Testing connection to Ollama: {OLLAMA_URL}")
        
        headers = {
            'Authorization': f'Bearer {OLLAMA_API_KEY}',
            'Content-Type': 'application/json'
        }
        
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
            timeout=60
        )
        
        logger.info(f"Test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from Ollama")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "message" in result and "content" in result["message"]:
                logger.info("Ollama connection test successful")
                logger.info(f"Test response: {result['message']['content'][:100]}...")
                return True
            else:
                logger.error(f"No 'message.content' field. Fields: {list(result.keys())}")
                return False
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON response: {e}")
            return False
        
    except Exception as e:
        logger.error(f"Ollama connection test failed: {e}")
        return False

def get_collection_stats():
    """Get collection statistics"""
    try:
        total_count = ticket_col.count_documents({})
        
        with_complete_fields = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Stats by priority
        pipeline_priority = [
            {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        priority_dist = list(ticket_col.aggregate(pipeline_priority))
        
        # Stats by resolution status
        pipeline_resolution = [
            {"$group": {"_id": "$resolution_status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        resolution_dist = list(ticket_col.aggregate(pipeline_resolution))
        
        urgent_tickets = ticket_col.count_documents({"urgency": True})
        without_complete_fields = total_count - with_complete_fields
        
        logger.info("Collection Statistics:")
        logger.info(f"  Total tickets: {total_count}")
        logger.info(f"  With complete fields: {with_complete_fields}")
        logger.info(f"  Without complete fields: {without_complete_fields}")
        logger.info(f"  Urgent tickets: {urgent_tickets} ({(urgent_tickets/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent tickets: 0")
        logger.info(f"  Completion rate: {(with_complete_fields/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
        logger.info("Priority Distribution:")
        for item in priority_dist:
            logger.info(f"  {item['_id']}: {item['count']} tickets")
        
        logger.info("Resolution Status Distribution:")
        for item in resolution_dist:
            logger.info(f"  {item['_id']}: {item['count']} tickets")
        
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_tickets(limit=3):
    """Get sample tickets with generated analysis"""
    try:
        samples = list(ticket_col.find({
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Ticket Analysis:")
        for i, ticket in enumerate(samples, 1):
            logger.info(f"--- Sample Ticket {i} ---")
            logger.info(f"Ticket ID: {ticket.get('_id', 'N/A')}")
            logger.info(f"Title: {ticket.get('title', 'N/A')}")
            logger.info(f"Dominant Topic: {ticket.get('dominant_topic', 'N/A')}")
            logger.info(f"Priority: {ticket.get('priority', 'N/A')}")
            logger.info(f"Resolution Status: {ticket.get('resolution_status', 'N/A')}")
            logger.info(f"Ticket Summary: {str(ticket.get('ticket_summary', 'N/A'))[:150]}...")
            if 'urgency' in ticket:
                logger.info(f"Urgent: {ticket['urgency']}")
            
    except Exception as e:
        logger.error(f"Error getting sample tickets: {e}")

def main():
    """Main function"""
    logger.info("Optimized EU Banking Trouble Ticket Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{TICKET_COLLECTION}")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_URL}")
    logger.info(f"Configuration: {MAX_WORKERS} workers, {BATCH_SIZE} batch size")
    
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    if not init_database():
        logger.error("Cannot proceed without database connection")
        return
    
    try:
        get_collection_stats()
        process_tickets_optimized()
        
        logger.info("="*60)
        logger.info("FINAL STATISTICS")
        logger.info("="*60)
        get_collection_stats()
        get_sample_generated_tickets(3)
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        logger.error(traceback.format_exc())
    finally:
        cleanup_resources()
        logger.info("Session complete. Check log files for details:")
        logger.info(f"  Main: {MAIN_LOG_FILE}")
        logger.info(f"  Success: {SUCCESS_LOG_FILE}")
        logger.info(f"  Failures: {FAILURE_LOG_FILE}")
        logger.info(f"  Progress: {PROGRESS_LOG_FILE}")
        logger.info(f"  Checkpoint: {CHECKPOINT_FILE}")

if __name__ == "__main__":
    main()
