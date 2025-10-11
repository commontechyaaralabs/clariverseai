# EU Banking Chat Content Generator - External Category Only (OpenRouter)
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
import re
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
CHAT_COLLECTION = "chat_new"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"chat_external_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"external_successful_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"external_failed_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"external_progress_{timestamp}.log"
CHECKPOINT_FILE = LOG_DIR / f"external_checkpoint_{timestamp}.json"

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

# Ultra-conservative configuration for OpenRouter
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"
BATCH_SIZE = 1  # Process only 1 chat at a time
MAX_CONCURRENT = 1  # Single concurrent call to avoid rate limits
REQUEST_TIMEOUT = 300  # 5 minute timeout
MAX_RETRIES = 5
RETRY_DELAY = 10
BATCH_DELAY = 5.0
API_CALL_DELAY = 3.0
BASE_REQUEST_DELAY = 40.0
CHECKPOINT_SAVE_INTERVAL = 5
RATE_LIMIT_BACKOFF_MULTIPLIER = 2
MAX_RATE_LIMIT_WAIT = 120
MAX_RETRY_ATTEMPTS_PER_CHAT = 10

# OpenRouter setup
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Global variables for graceful shutdown
shutdown_flag = asyncio.Event()
client = None
db = None
chat_col = None

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
        self.chats_processed = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self._lock = asyncio.Lock()
    
    async def record_success(self, total_chats=None):
        async with self._lock:
            self.successful_requests += 1
            self.chats_processed += 1
            await self.log_progress(total_chats)
    
    async def record_failure(self, total_chats=None):
        async with self._lock:
            self.failed_requests += 1
            await self.log_progress(total_chats)
    
    async def log_progress(self, total_chats=None):
        if self.chats_processed % 50 == 0 and self.chats_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.chats_processed / elapsed if elapsed > 0 else 0
            remaining_chats = (total_chats - self.chats_processed) if total_chats else 0
            eta = remaining_chats / rate if rate > 0 and remaining_chats > 0 else 0
            
            success_rate = self.successful_requests/(self.successful_requests + self.failed_requests)*100 if (self.successful_requests + self.failed_requests) > 0 else 0
            logger.info(f"Performance: {self.chats_processed}/{total_chats} chats, {rate:.1f}/sec ({rate*3600:.0f}/hour), {success_rate:.1f}% success" + (f", ETA: {eta/3600:.1f}h" if eta > 0 else ""))

performance_monitor = PerformanceMonitor()

# Circuit Breaker for handling rate limits and timeouts
class CircuitBreaker:
    def __init__(self, failure_threshold=3, recovery_timeout=60):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = 'CLOSED'
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
        self.processed_chats = set()
        self.failed_chats = set()
        self.retry_attempts = {}
        self.stats = {
            'start_time': time.time(),
            'processed_count': 0,
            'success_count': 0,
            'failure_count': 0,
            'retry_count': 0
        }
        self._lock = asyncio.Lock()
        self.load_checkpoint()
    
    def load_checkpoint(self):
        try:
            if os.path.exists(self.checkpoint_file):
                with open(self.checkpoint_file, 'r') as f:
                    data = json.load(f)
                    self.processed_chats = set(data.get('processed_chats', []))
                    self.failed_chats = set(data.get('failed_chats', []))
                    self.retry_attempts = data.get('retry_attempts', {})
                    self.stats.update(data.get('stats', {}))
                logger.info(f"Loaded checkpoint: {len(self.processed_chats)} processed, {len(self.failed_chats)} failed")
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    
    async def save_checkpoint(self):
        async with self._lock:
            try:
                checkpoint_data = {
                    'processed_chats': list(self.processed_chats),
                    'failed_chats': list(self.failed_chats),
                    'retry_attempts': self.retry_attempts,
                    'stats': self.stats,
                    'timestamp': datetime.now().isoformat()
                }
                with open(self.checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
            except Exception as e:
                logger.error(f"Could not save checkpoint: {e}")
    
    def is_processed(self, chat_id):
        return str(chat_id) in self.processed_chats
    
    async def increment_retry(self, chat_id):
        async with self._lock:
            chat_id_str = str(chat_id)
            if chat_id_str not in self.retry_attempts:
                self.retry_attempts[chat_id_str] = 0
            self.retry_attempts[chat_id_str] += 1
            self.stats['retry_count'] += 1
            return self.retry_attempts[chat_id_str]
    
    def get_retry_count(self, chat_id):
        return self.retry_attempts.get(str(chat_id), 0)
    
    async def mark_processed(self, chat_id, success=True):
        async with self._lock:
            chat_id_str = str(chat_id)
            self.processed_chats.add(chat_id_str)
            self.stats['processed_count'] += 1
            
            if success:
                self.stats['success_count'] += 1
                self.failed_chats.discard(chat_id_str)
                if chat_id_str in self.retry_attempts:
                    del self.retry_attempts[chat_id_str]
            else:
                self.stats['failure_count'] += 1
                self.failed_chats.add(chat_id_str)
            
            if self.stats['processed_count'] % CHECKPOINT_SAVE_INTERVAL == 0:
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
    global client, db, chat_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        chat_col = db[CHAT_COLLECTION]
        
        # Create indexes for better performance
        chat_col.create_index("_id")
        chat_col.create_index("category")
        chat_col.create_index("dominant_topic")
        chat_col.create_index("urgency")
        chat_col.create_index("resolution_status")
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
        self.rate_limit_count = 0
        self.last_rate_limit_time = 0
        self._lock = asyncio.Lock()
    
    async def call_openrouter_async(self, session, prompt, max_retries=MAX_RETRIES):
        """Async OpenRouter API call with smart adaptive rate limiting and retries"""
        async with self.semaphore:
            # Intelligent rate limiting
            async with self._lock:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                
                if self.rate_limit_count > 0 and current_time - self.last_rate_limit_time > 600:
                    self.rate_limit_count = 0
                    logger.info("Rate limit counter decayed, resetting to normal delays")
                
                if self.rate_limit_count > 0:
                    base_delay = BASE_REQUEST_DELAY + (self.rate_limit_count * 20)
                    delay = max(base_delay, 45)
                    logger.info(f"Rate limit history detected ({self.rate_limit_count} recent), using {delay}s delay")
                else:
                    delay = BASE_REQUEST_DELAY
                
                if time_since_last < delay:
                    await asyncio.sleep(delay - time_since_last)
                self.last_request_time = time.time()
            
            headers = {
                'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                'Content-Type': 'application/json',
                'HTTP-Referer': 'http://localhost:3000',
                'X-Title': 'EU Banking Chat Generator - External'
            }
            
            payload = {
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4000,
                "temperature": 0.4
            }
            
            for attempt in range(max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    async with session.post(OPENROUTER_URL, json=payload, headers=headers, timeout=timeout) as response:
                        
                        if response.status == 429:
                            async with self._lock:
                                self.rate_limit_count += 1
                                self.last_rate_limit_time = time.time()
                            
                            wait_time = min(MAX_RATE_LIMIT_WAIT, 30 * (attempt + 1))
                            logger.warning(f"Rate limited (429) on attempt {attempt+1}/{max_retries}, waiting {wait_time}s")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        if response.status == 502 or response.status == 503:
                            wait_time = min(30, RETRY_DELAY * (attempt + 1))
                            logger.warning(f"Server error ({response.status}), waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        response.raise_for_status()
                        result = await response.json()
                        
                        if "choices" not in result or not result["choices"]:
                            raise ValueError("No 'choices' field in OpenRouter response")
                        
                        async with self._lock:
                            if self.rate_limit_count > 0:
                                logger.info(f"Successful request after rate limits, resetting counter")
                                self.rate_limit_count = 0
                        
                        return result["choices"][0]["message"]["content"]
                        
                except asyncio.TimeoutError:
                    logger.warning(f"Request timeout on attempt {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error(f"Request timed out after {max_retries} attempts")
                    raise
                
                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        continue
                    if e.status in [502, 503]:
                        continue
                    logger.error(f"HTTP error {e.status} on attempt {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        await asyncio.sleep(wait_time)
                        continue
                    raise
                
                except Exception as e:
                    try:
                        error_type = type(e).__name__
                        error_str = str(e) if e else "None"
                        logger.error(f"Exception type: {error_type}, Error: {error_str}")
                        
                        error_msg = ""
                        if e and hasattr(e, '__str__'):
                            try:
                                error_msg = str(e).lower()
                            except:
                                error_msg = ""
                        
                        logger.warning(f"Request failed on attempt {attempt+1}/{max_retries}: {error_msg if error_msg else 'unknown error'}")
                        
                        if error_msg and ("rate limit" in error_msg or "429" in error_msg):
                            async with self._lock:
                                self.rate_limit_count += 1
                                self.last_rate_limit_time = time.time()
                            wait_time = min(MAX_RATE_LIMIT_WAIT, 30 * (attempt + 1))
                            logger.warning(f"Rate limit detected in exception, pausing for {wait_time}s...")
                            await asyncio.sleep(wait_time)
                    except Exception as debug_e:
                        logger.error(f"Error in error handling: {debug_e}")
                        logger.error(f"Original error: {e}")
                    
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        await asyncio.sleep(wait_time)
                        continue
                    raise
            
            raise Exception(f"All {max_retries} attempts failed")

processor = RateLimitedProcessor()

def generate_external_chat_prompt(chat_data):
    """Generate optimized prompt for EXTERNAL chat content and analysis generation"""
    
    # Extract data from chat record
    dominant_topic = chat_data.get('dominant_topic')
    subtopics = chat_data.get('subtopics')
    messages = chat_data.get('messages', [])
    message_count = len(messages) if messages else 2
    
    # EXISTING FIELDS FROM chat_new collection
    urgency = chat_data.get('urgency')
    follow_up_required = chat_data.get('follow_up_required')
    action_pending_status = chat_data.get('action_pending_status')
    action_pending_from = chat_data.get('action_pending_from')
    priority = chat_data.get('priority')
    resolution_status = chat_data.get('resolution_status')
    overall_sentiment = chat_data.get('overall_sentiment')
    category = chat_data.get('category', 'External')  # Should always be External for this script
    
    # Extract participant names
    participant_names = []
    message_dates = []
    if messages:
        for message in messages:
            if isinstance(message, dict):
                from_user = message.get('from', {})
                if isinstance(from_user, dict):
                    user_info = from_user.get('user', {})
                    if isinstance(user_info, dict):
                        display_name = user_info.get('displayName')
                        if display_name and display_name not in participant_names:
                            participant_names.append(display_name)
                
                created_date = message.get('createdDateTime')
                if created_date:
                    message_dates.append(created_date)
    
    # Generate meaningful names for External conversation (customer and bank employee)
    if len(participant_names) < 2:
        participant_names = ['Customer', 'Bank_Employee']
    
    # Get chat-level dates
    chat_created = chat_data.get('chat', {}).get('createdDateTime') if chat_data.get('chat') else None
    chat_last_updated = chat_data.get('chat', {}).get('lastUpdatedDateTime') if chat_data.get('chat') else None
    
    # Use actual message dates if available
    if message_dates:
        first_message_date = message_dates[0]
        last_message_date = message_dates[-1]
    else:
        first_message_date = chat_created
        last_message_date = chat_last_updated
    
    # Determine action pending context
    action_pending_context = ""
    if action_pending_status == "yes" and action_pending_from:
        if action_pending_from and action_pending_from.lower() == "customer":
            action_pending_context = "The customer needs to take the next action (provide documents, respond to request, complete process, etc.)"
        elif action_pending_from and action_pending_from.lower() == "bank":
            action_pending_context = "The bank needs to take the next action (process request, review documents, provide response, etc.)"
        else:
            action_pending_context = f"The {action_pending_from} needs to take the next action"
    elif action_pending_status == "yes":
        action_pending_context = "An action is pending but the responsible party is unclear"
    else:
        action_pending_context = "No action is pending - process is complete or ongoing"
    
    # Build dynamic prompt metadata
    metadata_parts = []
    if dominant_topic is not None:
        metadata_parts.append(f"Topic:{dominant_topic}")
    if subtopics is not None:
        metadata_parts.append(f"Subtopic:{subtopics}")
    if overall_sentiment is not None:
        metadata_parts.append(f"Sentiment:{overall_sentiment}/5")
    if urgency is not None:
        metadata_parts.append(f"Urgency:{'URGENT' if urgency else 'NON-URGENT'}")
    if follow_up_required is not None:
        metadata_parts.append(f"Follow-up:{follow_up_required}")
    if action_pending_status is not None:
        metadata_parts.append(f"Action:{action_pending_status}")
    if action_pending_from is not None:
        metadata_parts.append(f"Action From:{action_pending_from}")
    if priority is not None:
        metadata_parts.append(f"Priority:{priority}")
    if resolution_status is not None:
        metadata_parts.append(f"Resolution:{resolution_status}")
    
    metadata_str = " | ".join(metadata_parts) if metadata_parts else "No metadata available"
    
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
    
    # Build participant string
    participant_str = " and ".join(participant_names) if len(participant_names) >= 2 else "Customer and Bank_Employee"
    
    # Build message generation instructions based on dates
    message_instructions = []
    for i in range(message_count):
        if i < len(participant_names):
            user_name = participant_names[i]
        else:
            user_name = f"User_{i+1}"
        
        message_date = message_dates[i] if i < len(message_dates) else first_message_date
        
        # Check for day shift
        day_shift_instruction = ""
        if i > 0:
            prev_message_date = message_dates[i-1] if i-1 < len(message_dates) else first_message_date
            if prev_message_date and message_date:
                try:
                    from datetime import datetime
                    prev_date = datetime.fromisoformat(prev_message_date.replace('Z', '+00:00')).date()
                    curr_date = datetime.fromisoformat(message_date.replace('Z', '+00:00')).date()
                    if prev_date != curr_date:
                        day_shift_instruction = " IMPORTANT: This message is on a different day. Start with a natural greeting acknowledging the time gap."
                except:
                    pass
        
        # CRITICAL: External means CUSTOMER speaks first!
        if i == 0:
            message_instructions.append(f'{{"content": "CUSTOMER message to BANK (10-100 words). Customer reaching out with banking question/issue/complaint. NOT an employee! Use first person (I, my, me). Sound like real customer: \'Hi, I need help with...\', \'Hello, I\'m having an issue...\', \'Can someone help me?\'. Topic: {dominant_topic if dominant_topic else "banking issue"}. Natural language, contractions, informal. Use emojis occasionally for emotion (😅 frustration, 🤔 confusion, 😊 thanks, ❓ questions). Include realistic details. NEVER mention other customer names.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
        else:
            message_instructions.append(f'{{"content": "BANK EMPLOYEE responding to CUSTOMER (10-100 words). Professional helpful response. Use phrases like \'I can help you with that\', \'Let me check\', \'I\'ll look into this\'. Address customer directly. Topic: {dominant_topic if dominant_topic else "banking business"}. Natural language, contractions, friendly tone. Use emojis sparingly (👍 confirmation, ✅ completed, 📧 email). Include solutions, next steps. When mentioning OTHER clients in conversation, use names like John Smith, Maria Garcia.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
    
    messages_json = ",\n  ".join(message_instructions)
    
    # Determine ending requirements
    ending_requirements = []
    if action_pending_status == "yes" and action_pending_from:
        if action_pending_from and action_pending_from.lower() == "customer":
            ending_requirements.append("Last message should indicate waiting for customer response/action")
        elif action_pending_from and action_pending_from.lower() == "bank":
            ending_requirements.append("Last message should indicate bank needs to take action")
        else:
            ending_requirements.append(f"Last message should indicate {action_pending_from} needs to take action")
    
    if follow_up_required == "yes":
        ending_requirements.append("Conversation should end with open-ended scenario requiring follow-up")
    
    ending_str = " | ".join(ending_requirements) if ending_requirements else "Conversation should end with complete resolution"

    prompt = f"""Generate EU banking EXTERNAL chat conversation with {message_count} messages (Customer ↔ Bank).

**METADATA:** {metadata_str}

**CONVERSATION TYPE:** EXTERNAL (Customer ↔ Bank Employee)
- This is external customer communication TO the bank
- First message is ALWAYS from CUSTOMER (not employee!)
- Customer has a problem/question/complaint
- Bank employee responds professionally and helpfully

**TIMELINE INFORMATION:**
- First message date: {first_message_date}
- Last message date: {last_message_date}
- Total message count: {message_count}

**RESOLUTION STATUS:** {resolution_status}
- Open: Issue is still being worked on, not resolved yet
- Pending: Waiting for action from customer or bank
- Resolved: Issue has been completely resolved
- Closed: Conversation ended, may or may not be resolved

**PRIORITY LEVEL DEFINITION:**
- P1-Critical: Business stop → must resolve NOW (follow-up within 24-48 hours)
- P2-High: Major issue, needs fast action (follow-up within 2-7 days)
- P3-Medium: Standard issues/requests (follow-up within 1-2 weeks)
- P4-Low: Minor issues, no major impact (follow-up within 2-4 weeks)
- P5-Very Low: Informational, FYI (follow-up within 1-2 months)

**ACTION PENDING CONTEXT:** {action_pending_context}

**PARTICIPANTS:** {participant_str}

**RULES:** 
- Category: EXTERNAL (Customer → Bank)
- Sentiment {overall_sentiment}/5: {sentiment_desc}
- Customer sentiment: {sentiment_desc}
- Bank employees: ALWAYS calm, professional, helpful (regardless of customer sentiment)
- Follow-up {follow_up_required}: {"End with open-ended scenarios" if follow_up_required == "yes" else "End with complete resolution"}
- Action {action_pending_status}: {"Show waiting scenarios" if action_pending_status == "yes" else "Show completed processes"}
- Action Pending From {action_pending_from}: {"End with customer needing to respond/take action" if action_pending_from and action_pending_from.lower() == "customer" else "End with bank needing to respond/take action" if action_pending_from and action_pending_from.lower() == "bank" else "End with completed process"}

**CONVERSATION STRUCTURE - CRITICAL FOR EXTERNAL:**
- **CUSTOMER writes FIRST message** - customer has a problem/question
- Bank employee responds helpfully
- Customer uses first person (I, my, me)
- Example flow:
  * Customer: "Hi, I'm having trouble with my account..." 
  * Bank: "I can help you with that! Let me check..."
  * Customer: "Thank you, I really need this fixed..."
  * Bank: "I've resolved it for you. You should see..."
- Realistic chat messages 10-100 words each
- Natural human conversation flow
- Use contractions, informal tone, emojis
- Use EXACT dates provided for message timestamps
- Consider time gaps for natural flow

**DATE-BASED CONVERSATION FLOW:**
- Use EXACT dates provided for each message
- CRITICAL: If there are time gaps, create natural conversation breaks
- For day shifts (different dates), start with greetings:
  * "Good morning!" / "Hey, got an update" / "Hi again" / "Following up on our discussion yesterday"
- For same-day gaps (hours apart):
  * "Got an update" / "Quick follow-up" / "Just checking in" / "Hey, just heard back"
- ALWAYS acknowledge time gaps appropriately
- NEVER continue across days without acknowledging the gap

**BANKING DETAILS:** 
- Realistic EU accounts (DE89 3704 0044 0532 0130 00)
- Specific amounts (€1,250.00)
- Transaction IDs (TXN-2025-001234567)
- Customer details (DOB: 15/03/1985)
- Authentic banking terminology

**CONVERSATION STYLE:** 
- Sound like real people chatting, not formal emails
- Use contractions (I'm, we're, can't, won't)
- Natural reactions (oh no!, really?, that's interesting)
- **Use emojis occasionally (NOT every message)** - when expressing emotion:
  * Customer emotions: 😅 (frustration), 🤔 (confusion), 😊 (thanks), ❓ (questions), 😞 (disappointed)
  * Bank responses: 👍 (confirmation), ✅ (completed), 📧 (email), 💳 (card), 📱 (phone)
  * Use 1-2 emojis per message maximum, only where natural
- Ask follow-up questions naturally
- Show emotions and personality
- Include realistic banking scenarios
- **CRITICAL: NO MOCK DATA OR PLACEHOLDERS** - Never use format like "[example: ...]" or "[secure link - example: ...]"
- Write realistic content directly without placeholder text in square brackets
- If mentioning links/IDs/references, write them naturally: "I've sent you a secure upload link" NOT "[secure upload link - example: https://...]"

**ENDING REQUIREMENTS:** {ending_str}

**FOLLOW-UP vs NEXT ACTION DISTINCTION:**
- **follow_up_reason** = "WHY" (the trigger/justification for follow-up)
  * Focus on REASON/CAUSE that necessitates follow-up
  * Examples: "Customer requested status update", "Documentation incomplete", "Compliance deadline approaching"
- **next_action_suggestion** = "WHAT" (the specific step to take)
  * Focus on CONCRETE ACTION to be performed
  * Examples: "Contact client to request missing documents", "Escalate to senior management", "Send follow-up email to customer"

**EXAMPLES OF GOOD EXTERNAL CHAT MESSAGES:**
- Customer: "Hi, I'm having trouble with my online banking. Can someone help me check my account balance?" (straightforward question)
- Bank: "Of course! I can help you with that 👍 Let me look up your account details right away." (emoji for confirmation)
- Customer: "Hello, I need help accessing my statements. My login isn't working 😅" (emoji for frustration)
- Bank: "Thanks for your patience! I've reset your login ✅ You should be able to access it now." (emoji for completion)
- Bank: "I've generated a secure upload link and sent it to your email address. Please use that to upload your documents." (realistic, no placeholders)
- Day shift: "Good morning! Following up on our discussion about the Smith application..."
- Same day gap: "Quick follow-up - did you hear back from the client?"

**AVOID THESE (BAD EXAMPLES WITH MOCK DATA):**
- ❌ "Here's your link: [secure upload link - example: https://secure.bankeu.com/upload/123]"
- ❌ "Your transaction ID is [TXN-ID-example]"
- ❌ "Contact us at [example@bank.com]"
- ✅ INSTEAD: "I've sent you a secure upload link via email" or "Your transaction has been processed successfully"

**FINAL CHECK:**
- Conversation Type = EXTERNAL (Customer ↔ Bank)
- First message MUST be from CUSTOMER (not employee!)
- Customer has problem/question - use first person (I, my, me)
- Bank responds professionally and helpfully
- Priority = {priority} → follow_up_date timing based on priority level (ONLY if follow_up_required="yes")
- Resolution Status = {resolution_status} → conversation should reflect this status
- Use emojis occasionally (1-2 per message max), NOT in every message!

**OUTPUT:** {{
  "messages": [
    {messages_json}
  ],
  "analysis": {{
    "chat_summary": "Business summary 150-200 words describing discussion topic, participants, key points, and context",
    "follow_up_reason": {"[WHY follow-up is needed - the trigger/justification. Focus on REASON/CAUSE. Examples: 'Customer requested status update', 'Documentation incomplete', 'Issue unresolved'. Be specific.]" if follow_up_required == "yes" else "null"},
    "next_action_suggestion": {"[WHAT specific step to take - actionable recommendation. Focus on CONCRETE ACTION. Examples: 'Contact client to request missing documents', 'Send follow-up email to customer', 'Escalate to senior management'. Be specific.]" if follow_up_required == "yes" and action_pending_status == "yes" else "null"},
    "follow_up_date": {"[Generate follow-up date after {last_message_date} based on priority={priority}. CRITICAL: P1=24-48h MAX, P2=7 days MAX, P3=1-2 weeks, P4=2-4 weeks, P5=1-2 months. Format: YYYY-MM-DDTHH:MM:SSZ]" if follow_up_required == "yes" else "null"}
  }}
}}

Use EXACT metadata values and EXACT dates provided. Implement concepts through natural scenarios, NOT explicit mentions. Generate authentic banking content with specific details.

**CRITICAL VALIDATION:**
- Follow-up reason = "WHY" - ONLY if follow_up_required="yes", otherwise "null"
- Next-action suggestion = "WHAT" - ONLY if follow_up_required="yes" AND action_pending_status="yes":
  * If action_pending_from="Customer": Suggest what customer needs to do
  * If action_pending_from="Bank": Suggest what bank needs to do
  * Otherwise: Set to "null"
- Follow-up date = "WHEN" - ONLY if follow_up_required="yes", otherwise "null"
- Chat summary should reflect EXTERNAL conversation (customer issue → bank resolution)
- All analysis fields should be based on actual conversation content
- Use EXACT dates provided for message timestamps

Generate now.
""".strip()
    
    return prompt

async def generate_chat_content(chat_data):
    """Generate chat content and analysis with OpenRouter"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    chat_id = chat_data.get('_id', 'unknown')
    
    try:
        logger.info(f"Chat {chat_id}: Starting prompt generation...")
        prompt = generate_external_chat_prompt(chat_data)
        logger.info(f"Chat {chat_id}: Prompt generated successfully, length: {len(prompt)}")
        
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
        
        # Remove markdown code fences
        reply = re.sub(r'^```(?:json)?\s*', '', reply)
        reply = re.sub(r'\s*```$', '', reply)
        
        # Extract JSON
        json_start = reply.find('{')
        json_end = reply.rfind('}') + 1
        
        if json_start == -1 or json_end <= json_start:
            raise ValueError("No valid JSON found in LLM response")
        
        reply = reply[json_start:json_end].strip()
        
        try:
            result = json.loads(reply)
            if str(chat_id).endswith('0') or str(chat_id).endswith('5'):
                logger.info(f"Chat {chat_id}: JSON parsing successful. Keys: {list(result.keys())}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing failed for chat {chat_id}. Raw response: {reply[:300]}...")
            raise ValueError(f"Invalid JSON response from LLM: {json_err}")
        
        # Validate required fields
        required_fields = ['messages', 'analysis']
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            logger.error(f"Chat {chat_id}: Missing required fields: {missing_fields}")
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Validate messages count
        message_count = len(chat_data.get('messages', [])) if chat_data.get('messages') else 2
        if len(result['messages']) != message_count:
            logger.warning(f"Chat {chat_id}: Expected {message_count} messages, got {len(result['messages'])}")
            if len(result['messages']) > message_count:
                result['messages'] = result['messages'][:message_count]
        
        # Validate required analysis fields
        required_analysis_fields = ['chat_summary']
        for field in required_analysis_fields:
            if field not in result['analysis']:
                logger.error(f"Chat {chat_id}: Missing required analysis field: {field}")
                raise ValueError(f"Missing required analysis field: {field}")
        
        generation_time = time.time() - start_time
        
        # Log success
        success_info = {
            'chat_id': str(chat_id),
            'category': 'External',
            'dominant_topic': chat_data.get('dominant_topic'),
            'urgency': chat_data.get('urgency'),
            'priority': chat_data.get('priority'),
            'resolution_status': chat_data.get('resolution_status'),
            'generation_time': generation_time
        }
        success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
        
        return result
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'chat_id': str(chat_id),
            'category': 'External',
            'dominant_topic': chat_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
        raise

async def process_single_chat(chat_record, total_chats=None):
    """Process a single chat record with comprehensive retry logic"""
    if shutdown_flag.is_set():
        return None
    
    chat_id = str(chat_record.get('_id', 'unknown'))
    retry_count = checkpoint_manager.get_retry_count(chat_id)
    
    for attempt in range(MAX_RETRY_ATTEMPTS_PER_CHAT):
        if shutdown_flag.is_set():
            return None
        
        try:
            if attempt > 0:
                retry_wait = min(120, RETRY_DELAY * (2 ** (attempt - 1)))
                logger.info(f"Chat {chat_id}: Retry attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS_PER_CHAT} after {retry_wait}s wait")
                await asyncio.sleep(retry_wait)
                await checkpoint_manager.increment_retry(chat_id)
            
            result = await _process_single_chat_internal(chat_record, total_chats)
            
            if result:
                if attempt > 0:
                    logger.info(f"Chat {chat_id}: SUCCESS after {attempt + 1} attempts!")
                return result
            else:
                logger.warning(f"Chat {chat_id}: Attempt {attempt + 1} returned no result, retrying...")
                continue
                
        except asyncio.TimeoutError:
            logger.warning(f"Chat {chat_id}: Timeout on attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS_PER_CHAT}")
            if attempt < MAX_RETRY_ATTEMPTS_PER_CHAT - 1:
                continue
            else:
                logger.error(f"Chat {chat_id}: Failed after {MAX_RETRY_ATTEMPTS_PER_CHAT} timeout attempts")
        
        except Exception as e:
            error_msg = ""
            if e and hasattr(e, '__str__'):
                try:
                    error_msg = str(e).lower()
                except:
                    error_msg = ""
            
            logger.warning(f"Chat {chat_id}: Error on attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS_PER_CHAT}: {str(e)[:100] if e else 'unknown error'}")
            
            if error_msg and ("rate limit" in error_msg or "429" in error_msg):
                wait_time = min(120, 30 * (attempt + 1))
                logger.info(f"Chat {chat_id}: Rate limit detected, waiting {wait_time}s before retry")
                await asyncio.sleep(wait_time)
            
            if attempt < MAX_RETRY_ATTEMPTS_PER_CHAT - 1:
                continue
            else:
                logger.error(f"Chat {chat_id}: Failed after {MAX_RETRY_ATTEMPTS_PER_CHAT} attempts: {str(e)[:100] if e else 'unknown error'}")
    
    logger.error(f"Chat {chat_id}: FAILED after {MAX_RETRY_ATTEMPTS_PER_CHAT} attempts - marking for manual retry")
    await performance_monitor.record_failure(total_chats)
    await failure_counter.increment()
    await checkpoint_manager.mark_processed(chat_id, success=False)
    return None

async def _process_single_chat_internal(chat_record, total_chats=None):
    """Internal chat processing logic"""
    chat_id = str(chat_record.get('_id', 'unknown'))
    
    try:
        # Generate content
        chat_content = await generate_chat_content(chat_record)
        
        if not chat_content:
            await performance_monitor.record_failure(total_chats)
            return None
        
        if str(chat_id).endswith('0'):
            logger.info(f"Chat {chat_id}: Generated content keys: {list(chat_content.keys()) if isinstance(chat_content, dict) else 'Not a dict'}")
        
        # Prepare update document
        update_doc = {}
        
        # Update messages with generated content
        if 'messages' in chat_content:
            messages = chat_content['messages']
            if isinstance(messages, list):
                for i, message in enumerate(messages):
                    if isinstance(message, dict):
                        update_doc[f'messages.{i}.body.content'] = message.get('content')
                        update_doc[f'messages.{i}.createdDateTime'] = message.get('timestamp')
        
        # Update analysis fields from LLM response
        if 'analysis' in chat_content:
            analysis = chat_content['analysis']
            if isinstance(analysis, dict):
                update_doc['chat_summary'] = analysis.get('chat_summary')
                
                chat_follow_up_required = chat_record.get('follow_up_required')
                chat_action_pending_status = chat_record.get('action_pending_status')
                
                if chat_follow_up_required == "yes":
                    update_doc['follow_up_reason'] = analysis.get('follow_up_reason')
                else:
                    update_doc['follow_up_reason'] = None
                
                if chat_follow_up_required == "yes":
                    follow_up_date = analysis.get('follow_up_date')
                    update_doc['follow_up_date'] = follow_up_date
                    if follow_up_date:
                        logger.info(f"Chat {chat_id}: Generated follow_up_date: {follow_up_date}")
                    else:
                        logger.warning(f"Chat {chat_id}: follow_up_date is None despite follow_up_required=yes")
                else:
                    update_doc['follow_up_date'] = None
                
                if chat_follow_up_required == "yes" and chat_action_pending_status == "yes":
                    update_doc['next_action_suggestion'] = analysis.get('next_action_suggestion')
                else:
                    update_doc['next_action_suggestion'] = None
        
        if 'follow_up_date' not in update_doc and 'follow_up_date' not in chat_record:
            update_doc['follow_up_date'] = None
        
        # Add LLM processing tracking
        update_doc['llm_processed'] = True
        update_doc['llm_processed_at'] = datetime.now().isoformat()
        update_doc['llm_model_used'] = OPENROUTER_MODEL
        update_doc['llm_category_processed'] = 'External'
        
        logger.info(f"Chat {chat_id}: Update document keys: {list(update_doc.keys())}")
        
        await performance_monitor.record_success(total_chats)
        await success_counter.increment()
        
        return {
            'chat_id': chat_id,
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Chat {chat_id} internal processing failed: {str(e)[:100]}")
        raise

async def save_batch_to_database(batch_updates):
    """Save batch updates to database with optimized bulk operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        if len(batch_updates) > 5:
            logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        bulk_operations = []
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"_id": ObjectId(update_data['chat_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
        
        if bulk_operations:
            try:
                result = chat_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.matched_count
                
                asyncio.create_task(update_counter.increment())
                
                if len(batch_updates) > 5:
                    logger.info(f"Successfully saved {updated_count} records to database")
                    progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                chunk_size = 10
                individual_success = 0
                for i in range(0, len(batch_updates), chunk_size):
                    chunk = batch_updates[i:i + chunk_size]
                    try:
                        chunk_operations = []
                        for update_data in chunk:
                            operation = UpdateOne(
                                filter={"_id": ObjectId(update_data['chat_id'])},
                                update={"$set": update_data['update_doc']}
                            )
                            chunk_operations.append(operation)
                        
                        chunk_result = chat_col.bulk_write(chunk_operations, ordered=False)
                        individual_success += chunk_result.matched_count
                    except Exception as chunk_error:
                        logger.error(f"Chunk update failed: {chunk_error}")
                
                logger.info(f"Fallback: {individual_success} records saved in chunks")
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

async def process_external_chats():
    """Main processing function for EXTERNAL category chat generation"""
    logger.info("Starting EU Banking EXTERNAL Chat Content Generation...")
    logger.info("Focus: Processing EXTERNAL category records ONLY")
    logger.info(f"Collection: {CHAT_COLLECTION}")
    logger.info(f"Configuration:")
    logger.info(f"  Max Concurrent: {MAX_CONCURRENT}")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info(f"  Model: {OPENROUTER_MODEL}")
    
    # Test connection
    if not await test_openrouter_connection():
        logger.error("Cannot proceed without OpenRouter connection")
        return
    
    # Get EXTERNAL chats to process
    try:
        query = {
            "$and": [
                # Must be External category
                {"category": {"$regex": "^External$", "$options": "i"}},
                # Must have basic structure
                {"_id": {"$exists": True}},
                {"messages": {"$exists": True, "$ne": None, "$ne": []}},
                # Must have null/empty message content
                {
                    "$or": [
                        {"messages.body.content": {"$eq": None}},
                        {"messages.body.content": {"$eq": ""}},
                        {"messages.body.content": {"$exists": False}},
                        {"messages.body": {"$exists": False}},
                        {"messages": {"$size": 0}}
                    ]
                }
            ]
        }
        
        # Exclude already processed chats
        if checkpoint_manager.processed_chats:
            processed_ids = [ObjectId(cid) for cid in checkpoint_manager.processed_chats if ObjectId.is_valid(cid)]
            query["_id"] = {"$nin": processed_ids}
        
        # Get statistics
        total_chats_in_db = chat_col.count_documents({})
        external_chats_total = chat_col.count_documents({"category": {"$regex": "^External$", "$options": "i"}})
        external_chats_processed = chat_col.count_documents({
            "category": {"$regex": "^External$", "$options": "i"},
            "llm_processed": True
        })
        
        chats_needing_processing = chat_col.count_documents(query)
        
        logger.info(f"Database Status:")
        logger.info(f"  Total chats in DB: {total_chats_in_db}")
        logger.info(f"  Total EXTERNAL chats: {external_chats_total}")
        logger.info(f"  EXTERNAL chats processed: {external_chats_processed}")
        logger.info(f"  EXTERNAL chats needing processing: {chats_needing_processing}")
        
        chat_records = chat_col.find(query).batch_size(100)
        total_chats = chat_col.count_documents(query)
        
        if total_chats == 0:
            logger.info("No EXTERNAL chats found that need processing!")
            return
        
        logger.info(f"Found {total_chats} EXTERNAL chats that need LLM processing")
        
        progress_logger.info(f"SESSION_START: total_external_chats={total_chats}, completed={external_chats_processed}")
        
    except Exception as e:
        logger.error(f"Error fetching chat records: {e}")
        return
    
    # Process chats
    total_updated = 0
    batch_updates = []
    
    try:
        batch_num = 0
        processed_count = 0
        
        while processed_count < total_chats:
            if shutdown_flag.is_set():
                logger.info("Shutdown requested, stopping processing")
                break
            
            batch_num += 1
            total_batches = (total_chats + BATCH_SIZE - 1)//BATCH_SIZE
            
            batch = []
            for _ in range(BATCH_SIZE):
                try:
                    chat = next(chat_records)
                    batch.append(chat)
                    processed_count += 1
                except StopIteration:
                    break
            
            if not batch:
                break
                
            logger.info(f"Processing batch {batch_num}/{total_batches} (chats {processed_count-len(batch)+1}-{processed_count})")
            
            batch_tasks = []
            for chat in batch:
                chat_id = str(chat.get('_id'))
                
                if not checkpoint_manager.is_processed(chat_id):
                    task = process_single_chat(chat, total_chats)
                    batch_tasks.append(task)
                else:
                    logger.info(f"Skipping already processed chat: {chat_id}")
            
            if batch_tasks:
                batch_start_time = time.time()
                successful_results = []
                failed_count = 0
                
                try:
                    task = batch_tasks[0]
                    result = await task
                    
                    if result:
                        successful_results.append(result)
                        asyncio.create_task(
                            checkpoint_manager.mark_processed(result['chat_id'], success=True)
                        )
                    else:
                        failed_count += 1
                        
                except Exception as e:
                    failed_count += 1
                    error_msg = ""
                    if e and hasattr(e, '__str__'):
                        try:
                            error_msg = str(e).lower()
                        except:
                            error_msg = ""
                    
                    if error_msg and ("rate limit" in error_msg or "429" in error_msg):
                        wait_time = 30
                        logger.warning(f"Rate limit detected at batch level, pausing for {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    logger.error(f"Single task failed with error: {e}")
                
                if successful_results:
                    batch_updates.extend(successful_results)
                
                batch_elapsed = time.time() - batch_start_time
                logger.info(f"Batch {batch_num} completed in {batch_elapsed:.1f}s: {len(successful_results)}/1 successful")
            
            if len(batch_updates) >= BATCH_SIZE:
                saved_count = await save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []
            
            progress_pct = (processed_count / total_chats) * 100
            logger.info(f"Progress: {progress_pct:.1f}% ({processed_count}/{total_chats})")
            
            if processed_count < total_chats and not shutdown_flag.is_set():
                await asyncio.sleep(BATCH_DELAY)
        
        if batch_updates and not shutdown_flag.is_set():
            saved_count = await save_batch_to_database(batch_updates)
            total_updated += saved_count
        
        # Final checkpoint save
        await checkpoint_manager.save_checkpoint()
        
        if shutdown_flag.is_set():
            logger.info("Processing interrupted gracefully!")
        else:
            logger.info("EXTERNAL chat content generation complete!")
        
        logger.info(f"Final Results:")
        logger.info(f"  Total EXTERNAL chats updated: {total_updated}")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        
        progress_logger.info(f"FINAL_SUMMARY: updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}")
        
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
            'X-Title': 'EU Banking External Chat Generator'
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
                    return True
                else:
                    logger.error("Invalid response structure from OpenRouter")
                    return False
        
    except Exception as e:
        logger.error(f"OpenRouter connection test failed: {e}")
        return False

async def main():
    """Main async function"""
    logger.info("EU Banking EXTERNAL Chat Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{CHAT_COLLECTION}")
    logger.info(f"Model: {OPENROUTER_MODEL}")
    logger.info(f"Category Filter: EXTERNAL ONLY")
    
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    if not init_database():
        logger.error("Cannot proceed without database connection")
        return
    
    try:
        await process_external_chats()
        
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

# Run the generator
if __name__ == "__main__":
    asyncio.run(main())


