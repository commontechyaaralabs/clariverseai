# EU Banking Email Thread Generation and Analysis System - OpenRouter Migration (Modified for email_new)
import os
import random
import time
import json
import asyncio
import aiohttp
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
from asyncio import Semaphore
import traceback

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
EMAIL_COLLECTION = "email_new"  # Changed to email_new

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"email_generator_openrouter_{timestamp}.log"
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

# Optimized configuration for faster processing
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"
BATCH_SIZE = 6  # Moderate batch size to balance speed and reliability
MAX_CONCURRENT = 2  # Conservative concurrency to avoid OpenRouter timeouts
REQUEST_TIMEOUT = 180  # Reduced timeout for faster failure detection
MAX_RETRIES = 8  # Fewer retries for faster failure handling
RETRY_DELAY = 30  # Shorter retry delay
BATCH_DELAY = 5.0  # Minimal batch delay
API_CALL_DELAY = 2.0  # Much shorter API delay between calls
CHECKPOINT_SAVE_INTERVAL = 50  # Much less frequent checkpoints
RATE_LIMIT_BACKOFF_MULTIPLIER = 2  # Gentler backoff
MAX_RATE_LIMIT_WAIT = 300  # Shorter wait time for rate limits

# OpenRouter setup
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Global variables for graceful shutdown
shutdown_flag = asyncio.Event()
client = None
db = None
email_col = None

# Custom JSON encoder to handle ObjectId serialization
class ObjectIdEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        return super().default(obj)

fake = Faker()

# Thread-safe counters
class AtomicCounter:
    def __init__(self, name):
        self._value = 0
        self._lock = asyncio.Lock()
        self._name = name
    
    async def increment(self):
        async with self._lock:
            self._value += 1
            # Reduce logging frequency for performance
            if self._value % 10 == 0:
                progress_logger.info(f"{self._name}: {self._value}")
            return self._value
    
    @property
    def value(self):
        return self._value

success_counter = AtomicCounter("SUCCESS_COUNT")
failure_counter = AtomicCounter("FAILURE_COUNT")
update_counter = AtomicCounter("UPDATE_COUNT")

# Performance Monitor
class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.emails_processed = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self._lock = asyncio.Lock()
    
    async def record_success(self, total_emails=None):
        async with self._lock:
            self.successful_requests += 1
            self.emails_processed += 1
            await self.log_progress(total_emails)
    
    async def record_failure(self, total_emails=None):
        async with self._lock:
            self.failed_requests += 1
            await self.log_progress(total_emails)
    
    async def log_progress(self, total_emails=None):
        if self.emails_processed % 50 == 0 and self.emails_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.emails_processed / elapsed if elapsed > 0 else 0
            remaining_emails = (total_emails - self.emails_processed) if total_emails else 0
            eta = remaining_emails / rate if rate > 0 and remaining_emails > 0 else 0
            
            # Concise logging for performance
            success_rate = self.successful_requests/(self.successful_requests + self.failed_requests)*100 if (self.successful_requests + self.failed_requests) > 0 else 0
            logger.info(f"Performance: {self.emails_processed}/{total_emails} emails, {rate:.1f}/sec ({rate*3600:.0f}/hour), {success_rate:.1f}% success" + (f", ETA: {eta/3600:.1f}h" if eta > 0 else ""))

performance_monitor = PerformanceMonitor()

# Circuit Breaker for handling rate limits and timeouts
class CircuitBreaker:
    def __init__(self, failure_threshold=8, recovery_timeout=120):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'  # CLOSED, OPEN, HALF_OPEN
        self._lock = asyncio.Lock()
    
    async def call(self, func, *args, **kwargs):
        async with self._lock:
            if self.state == 'OPEN':
                if time.time() - self.last_failure_time > self.recovery_timeout:
                    self.state = 'HALF_OPEN'
                    logger.info("Circuit breaker moving to HALF_OPEN state")
                else:
                    raise Exception("Circuit breaker is OPEN - too many failures")
        
        try:
            result = await func(*args, **kwargs)
            await self.on_success()
            return result
        except Exception as e:
            await self.on_failure()
            raise
    
    async def on_success(self):
        async with self._lock:
            if self.state == 'HALF_OPEN':
                logger.info("Circuit breaker moving to CLOSED state")
            self.failure_count = 0
            self.state = 'CLOSED'
    
    async def on_failure(self):
        async with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = 'OPEN'
                logger.warning(f"Circuit breaker OPEN after {self.failure_count} failures")

circuit_breaker = CircuitBreaker()

# Checkpoint Manager for resuming from failures
class CheckpointManager:
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file
        self.processed_emails = set()
        self.failed_emails = set()
        self.stats = {
            'start_time': time.time(),
            'processed_count': 0,
            'success_count': 0,
            'failure_count': 0
        }
        self._lock = asyncio.Lock()
        self.load_checkpoint()
    
    def load_checkpoint(self):
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.processed_emails = set(data.get('processed_emails', []))
                    self.failed_emails = set(data.get('failed_emails', []))
                    self.stats.update(data.get('stats', {}))
                logger.info(f"Loaded checkpoint: {len(self.processed_emails)} processed, {len(self.failed_emails)} failed")
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    
    async def save_checkpoint(self):
        async with self._lock:
            try:
                checkpoint_data = {
                    'processed_emails': list(self.processed_emails),
                    'failed_emails': list(self.failed_emails),
                    'stats': self.stats,
                    'timestamp': datetime.now().isoformat()
                }
                with open(self.checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
            except Exception as e:
                logger.error(f"Could not save checkpoint: {e}")
    
    def is_processed(self, thread_id):
        return str(thread_id) in self.processed_emails
    
    async def mark_processed(self, thread_id, success=True):
        async with self._lock:
            thread_id_str = str(thread_id)
            self.processed_emails.add(thread_id_str)
            self.stats['processed_count'] += 1
            
            if success:
                self.stats['success_count'] += 1
                self.failed_emails.discard(thread_id_str)
            else:
                self.stats['failure_count'] += 1
                self.failed_emails.add(thread_id_str)
            
            # Auto-save much less frequently to reduce I/O overhead
            if self.stats['processed_count'] % CHECKPOINT_SAVE_INTERVAL == 0:
                # Use create_task to avoid blocking
                asyncio.create_task(self.save_checkpoint())

checkpoint_manager = CheckpointManager(CHECKPOINT_FILE)

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        asyncio.create_task(shutdown_flag.set())
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

# Rate Limited Processor
class RateLimitedProcessor:
    def __init__(self, max_concurrent=MAX_CONCURRENT):
        self.semaphore = Semaphore(max_concurrent)
        self.last_request_time = 0
        self._lock = asyncio.Lock()
    
    async def call_openrouter_async(self, session, prompt, max_retries=MAX_RETRIES):
        """Async OpenRouter API call with rate limiting and retries"""
        async with self.semaphore:
            # Rate limiting - ensure minimum delay between requests
            async with self._lock:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < API_CALL_DELAY:
                    await asyncio.sleep(API_CALL_DELAY - time_since_last)
                self.last_request_time = time.time()
            
            # Minimal delay for OpenRouter free tier to prevent timeouts
            await asyncio.sleep(0.2)
            
            headers = {
                'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                'Content-Type': 'application/json',
                'HTTP-Referer': 'http://localhost:3000',
                'X-Title': 'EU Banking Email Generator'
            }
            
            payload = {
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4000,
                "temperature": 0.4
            }
            
            for attempt in range(max_retries):
                try:
                    # Progressive timeout - increase timeout with each retry
                    progressive_timeout = REQUEST_TIMEOUT + (attempt * 30)  # Add 30s per retry
                    timeout = aiohttp.ClientTimeout(total=progressive_timeout)
                    async with session.post(OPENROUTER_URL, json=payload, headers=headers, timeout=timeout) as response:
                        
                        if response.status == 429:  # Rate limited
                            wait_time = min(MAX_RATE_LIMIT_WAIT, RETRY_DELAY * (RATE_LIMIT_BACKOFF_MULTIPLIER ** attempt))
                            logger.warning(f"Rate limited (429), waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                            logger.info(f"Rate limit detected - this is normal for free tier. Waiting {wait_time} seconds...")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        if response.status == 502 or response.status == 503:  # Bad Gateway or Service Unavailable
                            wait_time = min(60, RETRY_DELAY * (attempt + 1))
                            logger.warning(f"Server error ({response.status}), waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        response.raise_for_status()
                        result = await response.json()
                        
                        if "choices" not in result or not result["choices"]:
                            raise ValueError("No 'choices' field in OpenRouter response")
                        
                        return result["choices"][0]["message"]["content"]
                        
                except asyncio.TimeoutError:
                    logger.warning(f"Request timeout on attempt {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        logger.info(f"Waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                
                except aiohttp.ClientResponseError as e:
                    if e.status == 429:  # Rate limit - already handled above
                        continue
                    if e.status in [502, 503]:  # Server errors - already handled above
                        continue
                    logger.error(f"HTTP error {e.status} on attempt {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                
                except Exception as e:
                    logger.warning(f"Request failed on attempt {attempt+1}/{max_retries}: {e}")
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        await asyncio.sleep(wait_time)
                        continue
                    raise
            
            raise Exception(f"All {max_retries} attempts failed")

processor = RateLimitedProcessor()

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

async def generate_email_content(email_data):
    """Generate email content and analysis with OpenRouter"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    thread_id = email_data.get('thread', {}).get('thread_id', 'unknown')
    
    try:
        prompt = generate_optimized_email_prompt(email_data)
        
        # Create session for this request
        connector = aiohttp.TCPConnector(limit=10, force_close=True, enable_cleanup_closed=True)
        timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 10)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            response = await circuit_breaker.call(
                processor.call_openrouter_async, 
                session, 
                prompt
            )
        
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
        
        # Sentiment analysis removed - no longer needed
        
        # Clean up any subject lines and headers that might appear in body content
        if 'messages' in result:
            for message in result['messages']:
                if 'body' in message and 'text' in message['body'] and 'plain' in message['body']['text']:
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
                    
                    message['body']['text']['plain'] = cleaned_body
        
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
        success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
        
        return result
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'thread_id': thread_id,
            'dominant_topic': email_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
        raise

async def process_single_email(email_record, total_emails=None):
    """Process a single email record with all optimizations"""
    if shutdown_flag.is_set():
        return None
    
    thread_id = email_record.get('thread', {}).get('thread_id', 'unknown')
    
    try:
        return await _process_single_email_internal(email_record, total_emails)
    except Exception as e:
        logger.error(f"Thread {thread_id} processing failed with error: {str(e)[:100]}")
        await performance_monitor.record_failure(total_emails)
        await failure_counter.increment()
        await checkpoint_manager.mark_processed(thread_id, success=False)
        return None

async def _process_single_email_internal(email_record, total_emails=None):
    """Internal email processing logic"""
    thread_id = email_record.get('thread', {}).get('thread_id', 'unknown')
    
    try:
        # Generate content
        email_content = await generate_email_content(email_record)
        
        if not email_content:
            await performance_monitor.record_failure(total_emails)
            return None
        
        # Debug: Log the generated content structure (reduced frequency)
        if thread_id.endswith('0'):  # Log only every 10th thread
            logger.info(f"Thread {thread_id}: Generated content keys: {list(email_content.keys()) if isinstance(email_content, dict) else 'Not a dict'}")
        
        # Prepare update document
        update_doc = {}
        
        # Update thread data
        if 'thread_data' in email_content:
            thread_data = email_content['thread_data']
            update_doc['thread.subject_norm'] = thread_data.get('subject_norm')
            update_doc['thread.first_message_at'] = thread_data.get('first_message_at')
            update_doc['thread.last_message_at'] = thread_data.get('last_message_at')
        
        # Update messages with generated content
        if 'messages' in email_content:
            messages = email_content['messages']
            for i, message in enumerate(messages):
                if i < len(email_record.get('messages', [])):
                    update_doc[f'messages.{i}.headers.date'] = message.get('headers', {}).get('date')
                    update_doc[f'messages.{i}.headers.subject'] = message.get('headers', {}).get('subject')
                    update_doc[f'messages.{i}.body.text.plain'] = message.get('body', {}).get('text', {}).get('plain')
        
        # Update analysis fields from LLM response - only new fields, preserve existing metadata
        if 'analysis' in email_content:
            analysis = email_content['analysis']
            # Only update LLM-generated fields - do NOT overwrite existing metadata fields
            update_doc['email_summary'] = analysis.get('email_summary')
            update_doc['follow_up_date'] = analysis.get('follow_up_date')
            update_doc['follow_up_reason'] = analysis.get('follow_up_reason')
            update_doc['next_action_suggestion'] = analysis.get('next_action_suggestion')
        
        # Add LLM processing tracking
        update_doc['llm_processed'] = True
        update_doc['llm_processed_at'] = datetime.now().isoformat()
        update_doc['llm_model_used'] = OPENROUTER_MODEL
        
        await performance_monitor.record_success(total_emails)
        await success_counter.increment()
        
        return {
            'thread_id': thread_id,
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Thread {thread_id} internal processing failed: {str(e)[:100]}")
        raise  # Re-raise to be caught by the outer handler

async def save_batch_to_database(batch_updates):
    """Save batch updates to database with optimized bulk operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        # Reduced logging for performance
        if len(batch_updates) > 5:  # Only log for larger batches
            logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create bulk operations efficiently
        bulk_operations = []
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"thread.thread_id": update_data['thread_id']},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
        
        if bulk_operations:
            try:
                # Use ordered=False for better performance
                result = email_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.matched_count
                
                # Update counter asynchronously
                asyncio.create_task(update_counter.increment())
                
                if len(batch_updates) > 5:
                    logger.info(f"Successfully saved {updated_count} records to database")
                    progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Fallback to smaller chunks if bulk fails
                chunk_size = 10
                individual_success = 0
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i:i + chunk_size]
                    try:
                        chunk_operations = []
                        for update_data in chunk:
                            operation = UpdateOne(
                                filter={"thread.thread_id": update_data['thread_id']},
                                update={"$set": update_data['update_doc']}
                            )
                            chunk_operations.append(operation)
                        
                        chunk_result = email_col.bulk_write(chunk_operations, ordered=False)
                        individual_success += chunk_result.matched_count
                    except Exception as chunk_error:
                        logger.error(f"Chunk update failed: {chunk_error}")
                
                logger.info(f"Fallback: {individual_success} records saved in chunks")
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

async def process_emails_optimized():
    """Main optimized processing function for email generation"""
    logger.info("Starting Optimized EU Banking Email Content Generation...")
    logger.info(f"Collection: {EMAIL_COLLECTION}")
    logger.info(f"Optimized Configuration:")
    logger.info(f"  Max Concurrent: {MAX_CONCURRENT}")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info(f"  API Delay: {API_CALL_DELAY}s")
    logger.info(f"  Request Timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"  Model: {OPENROUTER_MODEL}")
    
    # Test connection
    if not await test_openrouter_connection():
        logger.error("Cannot proceed without OpenRouter connection")
        return
    
    # Get emails to process - only those that have NEVER been processed by LLM
    try:
        # Query for emails that have basic fields but are missing LLM-generated content
        query = {
            "$and": [
                # Must have basic email structure
                {"thread.thread_id": {"$exists": True}},
                {"thread": {"$exists": True}},
                # Must have the basic fields from email_new collection
                {"stages": {"$exists": True, "$ne": None, "$ne": ""}},
                {"category": {"$exists": True, "$ne": None, "$ne": ""}},
                {"urgency": {"$exists": True}},
                {"follow_up_required": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"priority": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}},
                # Exclude already processed records
                {"llm_processed": {"$ne": True}},
                # Must be missing LLM-generated content fields
                {
                    "$or": [
                        {"email_summary": {"$exists": False}},
                        {"email_summary": {"$eq": None}},
                        {"email_summary": {"$eq": ""}},
                        {"next_action_suggestion": {"$exists": False}},
                        {"next_action_suggestion": {"$eq": None}},
                        {"next_action_suggestion": {"$eq": ""}},
                        {"sentiment": {"$exists": False}},
                        {"sentiment": {"$eq": None}}
                    ]
                }
            ]
        }
        
        # Exclude already processed emails
        if checkpoint_manager.processed_emails:
            processed_ids = list(checkpoint_manager.processed_emails)
            query["thread.thread_id"] = {"$nin": processed_ids}
        
        # Check email status
        total_emails_in_db = email_col.count_documents({})
        emails_processed_by_llm = email_col.count_documents({"llm_processed": True})
        emails_with_basic_fields = email_col.count_documents({
            "$and": [
                {"stages": {"$exists": True, "$ne": None, "$ne": ""}},
                {"category": {"$exists": True, "$ne": None, "$ne": ""}},
                {"urgency": {"$exists": True}},
                {"follow_up_required": {"$exists": True, "$ne": None, "$ne": ""}},
                {"priority": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        emails_with_llm_fields = email_col.count_documents({
            "$and": [
                {"email_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"sentiment": {"$exists": True, "$ne": None}}
            ]
        })
        
        # Calculate actual emails needing processing
        emails_needing_processing = email_col.count_documents(query)
        
        # Calculate pending emails (those with basic fields but not processed)
        emails_pending_processing = emails_with_basic_fields - emails_processed_by_llm
        
        # Debug: Let's also check what fields actually exist
        logger.info("Debug - Checking field distribution in email_new collection:")
        for field in ["stages", "category", "overall_sentiment", "urgency", "follow_up_required", 
                      "action_pending_status", "priority", "resolution_status", "email_summary", 
                      "next_action_suggestion", "sentiment"]:
            count = email_col.count_documents({field: {"$exists": True, "$ne": None, "$ne": ""}})
            logger.info(f"  {field}: {count} emails have this field")
        
        # Calculate completion percentages
        completion_percentage = (emails_processed_by_llm / emails_with_basic_fields * 100) if emails_with_basic_fields > 0 else 0
        pending_percentage = (emails_pending_processing / emails_with_basic_fields * 100) if emails_with_basic_fields > 0 else 0
        
        logger.info(f"Database Status:")
        logger.info(f"  Total emails in DB: {total_emails_in_db}")
        logger.info(f"  Emails with required basic fields: {emails_with_basic_fields}")
        logger.info(f"  Emails with LLM-generated fields: {emails_with_llm_fields}")
        logger.info(f"  Emails processed by LLM (llm_processed=True): {emails_processed_by_llm}")
        logger.info(f"  Emails pending processing: {emails_pending_processing}")
        logger.info(f"  Emails needing processing (this session): {emails_needing_processing}")
        logger.info(f"  Overall Progress: {completion_percentage:.1f}% completed, {pending_percentage:.1f}% pending")
        
        # Use cursor instead of loading all into memory at once
        email_records = email_col.find(query).batch_size(100)
        total_emails = email_col.count_documents(query)
        
        if total_emails == 0:
            logger.info("No emails found that need processing!")
            logger.info("All emails appear to have been processed by LLM already.")
            return
        
        logger.info(f"Found {total_emails} emails that need LLM processing")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_emails)} emails")
        
        # Log session progress
        progress_logger.info(f"SESSION_START: total_emails={total_emails}, completed={emails_processed_by_llm}, pending={emails_pending_processing}, completion_rate={completion_percentage:.1f}%")
        progress_logger.info(f"BATCH_START: total_emails={total_emails}")
        
    except Exception as e:
        logger.error(f"Error fetching email records: {e}")
        return
    
    # Process emails in optimized batches
    total_updated = 0
    batch_updates = []
    
    try:
        # Process emails in concurrent batches using cursor
        batch_num = 0
        processed_count = 0
        
        while processed_count < total_emails:
            if shutdown_flag.is_set():
                logger.info("Shutdown requested, stopping processing")
                break
            
            batch_num += 1
            total_batches = (total_emails + BATCH_SIZE - 1)//BATCH_SIZE
            
            # Collect batch from cursor
            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    email = next(email_records)
                    batch.append(email)
                    processed_count += 1
                except StopIteration:
                    break
            
            if not batch:
                break
                
            logger.info(f"Processing batch {batch_num}/{total_batches} (emails {processed_count-len(batch)+1}-{processed_count})")
            
            # Process batch concurrently
            batch_tasks = []
            for email in batch:
                thread_id = email.get('thread', {}).get('thread_id')
                
                # Check checkpoint to prevent duplicates (database check is already done in query)
                if not checkpoint_manager.is_processed(thread_id):
                    task = process_single_email(email, total_emails)
                    batch_tasks.append(task)
                else:
                    logger.info(f"Skipping already processed email (checkpoint): {thread_id}")
            
            logger.info(f"Created {len(batch_tasks)} tasks for batch {batch_num}")
            
            if batch_tasks:
                # Process tasks with controlled concurrency to avoid OpenRouter timeouts
                logger.info(f"Processing {len(batch_tasks)} tasks with controlled concurrency for batch {batch_num}")
                
                batch_start_time = time.time()
                successful_results = []
                failed_count = 0
                
                # Process tasks with staggered execution to avoid overwhelming OpenRouter
                try:
                    # Use asyncio.as_completed with controlled concurrency
                    semaphore = asyncio.Semaphore(MAX_CONCURRENT)
                    
                    async def controlled_task(task):
                        async with semaphore:
                            # Minimal delay between concurrent requests to OpenRouter
                            await asyncio.sleep(0.1)
                            return await task
                    
                    # Create controlled tasks
                    controlled_tasks = [controlled_task(task) for task in batch_tasks]
                    
                    # Process with individual timeouts to prevent one slow request from blocking others
                    completed_results = []
                    for i, task in enumerate(controlled_tasks):
                        try:
                            task_timeout = REQUEST_TIMEOUT * 4  # 12 minutes (720s) per task
                            logger.info(f"Starting task {i+1}/{len(controlled_tasks)} with {task_timeout}s timeout")
                            
                            result = await asyncio.wait_for(task, timeout=task_timeout)
                            completed_results.append((i, result))
                            
                            if result:
                                successful_results.append(result)
                                # Mark as processed (non-blocking)
                                asyncio.create_task(
                                    checkpoint_manager.mark_processed(result['thread_id'], success=True)
                                )
                                logger.info(f"Task {i+1}/{len(controlled_tasks)} completed successfully")
                            else:
                                failed_count += 1
                                logger.warning(f"Task {i+1}/{len(controlled_tasks)} returned no result")
                                
                        except asyncio.TimeoutError:
                            failed_count += 1
                            logger.error(f"Task {i+1}/{len(controlled_tasks)} timed out after {task_timeout}s")
                        except Exception as e:
                            failed_count += 1
                            logger.error(f"Task {i+1}/{len(controlled_tasks)} failed with error: {e}")
                    
                    if successful_results:
                        batch_updates.extend(successful_results)
                    
                    batch_elapsed = time.time() - batch_start_time
                    logger.info(f"Batch {batch_num} completed in {batch_elapsed:.1f}s: {len(successful_results)}/{len(batch_tasks)} successful, {failed_count} failed")
                    
                except Exception as batch_error:
                    batch_elapsed = time.time() - batch_start_time
                    logger.error(f"Batch {batch_num} failed with error: {batch_error}")
                    failed_count = len(batch_tasks)
            
            # Save to database when we have enough updates
            if len(batch_updates) >= BATCH_SIZE:
                saved_count = await save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear batch
            
            # Progress update
            progress_pct = (processed_count / total_emails) * 100
            remaining_emails = total_emails - processed_count
            
            # Calculate overall completion including previously processed
            total_completed = emails_processed_by_llm + total_updated
            overall_completion = (total_completed / emails_with_basic_fields * 100) if emails_with_basic_fields > 0 else 0
            
            logger.info(f"Session Progress: {progress_pct:.1f}% ({processed_count}/{total_emails}) - {remaining_emails} remaining")
            logger.info(f"Overall Progress: {overall_completion:.1f}% completed ({total_completed}/{emails_with_basic_fields} total emails)")
            
            # Log detailed progress
            progress_logger.info(f"PROGRESS_UPDATE: session={progress_pct:.1f}%, overall={overall_completion:.1f}%, processed_this_session={total_updated}, remaining={remaining_emails}")
            
            # Update performance monitor with actual total
            await performance_monitor.log_progress(total_emails)
            
            # Brief delay between batches to manage rate limits
            if processed_count < total_emails and not shutdown_flag.is_set():
                await asyncio.sleep(BATCH_DELAY)
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = await save_batch_to_database(batch_updates)
            total_updated += saved_count
        
        # Final checkpoint save
        await checkpoint_manager.save_checkpoint()
        
        if shutdown_flag.is_set():
            logger.info("Processing interrupted gracefully!")
        else:
            logger.info("Optimized email content generation complete!")
        
        # Final statistics
        final_total_completed = emails_processed_by_llm + total_updated
        final_completion_percentage = (final_total_completed / emails_with_basic_fields * 100) if emails_with_basic_fields > 0 else 0
        final_pending = emails_with_basic_fields - final_total_completed
        
        logger.info(f"Final Results:")
        logger.info(f"  Total emails updated this session: {total_updated}")
        logger.info(f"  Total emails completed (all time): {final_total_completed}")
        logger.info(f"  Total emails pending: {final_pending}")
        logger.info(f"  Overall completion rate: {final_completion_percentage:.1f}%")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_email = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per email: {avg_time_per_email:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} emails/hour" if total_time > 0 else "Processing rate: N/A")
        
        progress_logger.info(f"FINAL_SUMMARY: session_updated={total_updated}, total_completed={final_total_completed}, pending={final_pending}, completion_rate={final_completion_percentage:.1f}%, success={success_counter.value}, failures={failure_counter.value}, total_time={total_time/3600:.2f}h, rate={success_counter.value/(total_time/3600):.0f}/h" if total_time > 0 else f"FINAL_SUMMARY: session_updated={total_updated}, total_completed={final_total_completed}, pending={final_pending}, completion_rate={final_completion_percentage:.1f}%, success={success_counter.value}, failures={failure_counter.value}")
        
    except Exception as e:
        logger.error(f"Unexpected error in main processing: {e}")
        logger.error(traceback.format_exc())
    finally:
        await checkpoint_manager.save_checkpoint()

async def test_openrouter_connection():
    """Test OpenRouter connection with async client"""
    try:
        logger.info("Testing OpenRouter connection...")
        
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'http://localhost:3000',
            'X-Title': 'EU Banking Email Generator'
        }
        
        test_payload = {
            "model": OPENROUTER_MODEL,
            "messages": [{"role": "user", "content": 'Generate JSON: {"test": "success"}'}],
            "max_tokens": 50
        }
        
        connector = aiohttp.TCPConnector(force_close=True, enable_cleanup_closed=True)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            async with session.post(OPENROUTER_URL, json=test_payload, headers=headers) as response:
                response.raise_for_status()
                result = await response.json()
                
                if "choices" in result and result["choices"]:
                    logger.info("OpenRouter connection test successful")
                    logger.info(f"Test response: {result['choices'][0]['message']['content'][:100]}...")
                    return True
                else:
                    logger.error("Invalid response structure from OpenRouter")
                    return False
        
    except Exception as e:
        logger.error(f"OpenRouter connection test failed: {e}")
        return False

def get_collection_stats():
    """Get collection statistics for emails"""
    try:
        total_count = email_col.count_documents({})
        
        with_complete_analysis = email_col.count_documents({
            "email_summary": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Stats by category
        external_count = email_col.count_documents({"category": "External"})
        internal_count = email_col.count_documents({"category": "Internal"})
        
        # Stats by priority
        pipeline_priority = [
            {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        priority_dist = list(email_col.aggregate(pipeline_priority))
        
        # Stats by resolution status
        pipeline_resolution = [
            {"$group": {"_id": "$resolution_status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        resolution_dist = list(email_col.aggregate(pipeline_resolution))
        
        urgent_emails = email_col.count_documents({"urgency": True})
        without_complete_analysis = total_count - with_complete_analysis
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(email_col.aggregate(pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"  Total emails: {total_count}")
        logger.info(f"  With complete LLM analysis: {with_complete_analysis}")
        logger.info(f"  Without complete analysis: {without_complete_analysis}")
        logger.info(f"  External emails: {external_count} ({(external_count/total_count)*100:.1f}%)" if total_count > 0 else "  External emails: 0")
        logger.info(f"  Internal emails: {internal_count} ({(internal_count/total_count)*100:.1f}%)" if total_count > 0 else "  Internal emails: 0")
        logger.info(f"  Urgent emails: {urgent_emails} ({(urgent_emails/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent emails: 0")
        logger.info(f"  Completion rate: {(with_complete_analysis/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
        logger.info("Priority Distribution:")
        for item in priority_dist:
            logger.info(f"  {item['_id']}: {item['count']} emails")
        
        logger.info("Resolution Status Distribution:")
        for item in resolution_dist:
            logger.info(f"  {item['_id']}: {item['count']} emails")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"  {i}. {topic['_id']}: {topic['count']} emails")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_emails(limit=3):
    """Get sample emails with generated analysis"""
    try:
        samples = list(email_col.find({
            "email_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Email Analysis:")
        for i, email in enumerate(samples, 1):
            logger.info(f"--- Sample Email {i} ---")
            logger.info(f"Thread ID: {email.get('thread', {}).get('thread_id', 'N/A')}")
            logger.info(f"Subject: {email.get('thread', {}).get('subject_norm', 'N/A')}")
            logger.info(f"Dominant Topic: {email.get('dominant_topic', 'N/A')}")
            logger.info(f"Category: {email.get('category', 'N/A')}")
            logger.info(f"Stages: {email.get('stages', 'N/A')}")
            logger.info(f"Priority: {email.get('priority', 'N/A')}")
            logger.info(f"Resolution Status: {email.get('resolution_status', 'N/A')}")
            logger.info(f"Email Summary: {str(email.get('email_summary', 'N/A'))[:150]}...")
            if 'urgency' in email:
                logger.info(f"Urgent: {email['urgency']}")
            if 'overall_sentiment' in email:
                logger.info(f"Overall Sentiment: {email['overall_sentiment']}")
            
    except Exception as e:
        logger.error(f"Error getting sample emails: {e}")

async def main():
    """Main async function"""
    logger.info("Optimized EU Banking Email Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{EMAIL_COLLECTION}")
    logger.info(f"Model: {OPENROUTER_MODEL}")
    logger.info(f"Configuration: {MAX_CONCURRENT} concurrent, {BATCH_SIZE} batch size")
    
    # Setup signal handlers
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    # Initialize database
    if not init_database():
        logger.error("Cannot proceed without database connection")
        return
    
    try:
        # Show initial stats
        get_collection_stats()
        
        # Run optimized processing
        await process_emails_optimized()
        
        # Show final stats
        logger.info("="*60)
        logger.info("FINAL STATISTICS")
        logger.info("="*60)
        get_collection_stats()
        get_sample_generated_emails(3)
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        await shutdown_flag.set()
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

# Run the optimized generator
if __name__ == "__main__":
    asyncio.run(main())