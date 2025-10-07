# EU Banking Chat Content Generator - OpenRouter Migration (Modified for chat_new)
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
CHAT_COLLECTION = "chat_new"  # Changed to chat_new

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"chat_generator_openrouter_{timestamp}.log"
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

# Ultra-conservative configuration for OpenRouter (for organization API with strict rate limits)
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"
BATCH_SIZE = 1  # Process only 1 chat at a time
MAX_CONCURRENT = 1  # Single concurrent call to avoid rate limits
REQUEST_TIMEOUT = 300  # 5 minute timeout (free tier is SLOW - needs time to respond)
MAX_RETRIES = 5  # More retries to ensure no records are missed
RETRY_DELAY = 10  # 10 second initial retry delay
BATCH_DELAY = 5.0  # 5 second delay between batches (increased to avoid rate limits)
API_CALL_DELAY = 3.0  # 3 second base delay between API calls
BASE_REQUEST_DELAY = 40.0  # 40 second base delay per request (INCREASED for org API with strict limits)
CHECKPOINT_SAVE_INTERVAL = 5  # Very frequent checkpoints
RATE_LIMIT_BACKOFF_MULTIPLIER = 2  # Moderate backoff
MAX_RATE_LIMIT_WAIT = 120  # 2 minute max wait for rate limits
MAX_RETRY_ATTEMPTS_PER_CHAT = 10  # Maximum attempts per chat before final retry queue

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
            
            # Concise logging for performance
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
        self.processed_chats = set()
        self.failed_chats = set()
        self.retry_attempts = {}  # Track retry attempts per chat_id
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
                logger.info(f"Loaded checkpoint: {len(self.processed_chats)} processed, {len(self.failed_chats)} failed, {len(self.retry_attempts)} with retry attempts")
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
        """Track retry attempts for a chat"""
        async with self._lock:
            chat_id_str = str(chat_id)
            if chat_id_str not in self.retry_attempts:
                self.retry_attempts[chat_id_str] = 0
            self.retry_attempts[chat_id_str] += 1
            self.stats['retry_count'] += 1
            return self.retry_attempts[chat_id_str]
    
    def get_retry_count(self, chat_id):
        """Get number of retry attempts for a chat"""
        return self.retry_attempts.get(str(chat_id), 0)
    
    async def mark_processed(self, chat_id, success=True):
        async with self._lock:
            chat_id_str = str(chat_id)
            self.processed_chats.add(chat_id_str)
            self.stats['processed_count'] += 1
            
            if success:
                self.stats['success_count'] += 1
                self.failed_chats.discard(chat_id_str)
                # Clear retry attempts on success
                if chat_id_str in self.retry_attempts:
                    del self.retry_attempts[chat_id_str]
            else:
                self.stats['failure_count'] += 1
                self.failed_chats.add(chat_id_str)
            
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
    global client, db, chat_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        chat_col = db[CHAT_COLLECTION]
        
        # Create indexes for better performance
        chat_col.create_index("_id")
        chat_col.create_index("dominant_topic")
        chat_col.create_index("urgency")
        chat_col.create_index("follow_up_required")
        chat_col.create_index("action_pending_status")
        chat_col.create_index("priority")
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
            # Intelligent rate limiting based on recent rate limit hits
            async with self._lock:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                
                # Decay rate limit counter over time (reset after 10 minutes)
                if self.rate_limit_count > 0 and current_time - self.last_rate_limit_time > 600:
                    self.rate_limit_count = 0
                    logger.info("Rate limit counter decayed, resetting to normal delays")
                
                # Adaptive delay based on rate limit history
                if self.rate_limit_count > 0:
                    # Aggressive delay increase after rate limits
                    base_delay = BASE_REQUEST_DELAY + (self.rate_limit_count * 20)  # Add 20s per recent rate limit
                    delay = max(base_delay, 45)  # Minimum 45 seconds when rate limited
                    logger.info(f"Rate limit history detected ({self.rate_limit_count} recent), using {delay}s delay")
                else:
                    # Normal operation - use base delay
                    delay = BASE_REQUEST_DELAY
                
                if time_since_last < delay:
                    await asyncio.sleep(delay - time_since_last)
                self.last_request_time = time.time()
            
            headers = {
                'Authorization': f'Bearer {OPENROUTER_API_KEY}',
                'Content-Type': 'application/json',
                'HTTP-Referer': 'http://localhost:3000',
                'X-Title': 'EU Banking Chat Generator'
            }
            
            payload = {
                "model": OPENROUTER_MODEL,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 4000,
                "temperature": 0.4
            }
            
            for attempt in range(max_retries):
                try:
                    # Use fixed timeout to avoid long waits
                    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    async with session.post(OPENROUTER_URL, json=payload, headers=headers, timeout=timeout) as response:
                        
                        if response.status == 429:  # Rate limited
                            # Track rate limit hits
                            async with self._lock:
                                self.rate_limit_count += 1
                                self.last_rate_limit_time = time.time()
                            
                            # Very aggressive backoff for heavily rate limited free tier: 30s, 60s, 90s, 120s, 120s
                            wait_time = min(MAX_RATE_LIMIT_WAIT, 30 * (attempt + 1))
                            logger.warning(f"Rate limited (429) on attempt {attempt+1}/{max_retries}, waiting {wait_time}s")
                            logger.info(f"Rate limit detected - adapting delays for subsequent requests")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        if response.status == 502 or response.status == 503:  # Bad Gateway or Service Unavailable
                            wait_time = min(30, RETRY_DELAY * (attempt + 1))  # Reduced max wait to 30s
                            logger.warning(f"Server error ({response.status}), waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                            await asyncio.sleep(wait_time)
                            continue
                        
                        response.raise_for_status()
                        result = await response.json()
                        
                        if "choices" not in result or not result["choices"]:
                            raise ValueError("No 'choices' field in OpenRouter response")
                        
                        # Reset rate limit counter on successful request
                        async with self._lock:
                            if self.rate_limit_count > 0:
                                logger.info(f"Successful request after rate limits, resetting counter")
                                self.rate_limit_count = 0
                        
                        return result["choices"][0]["message"]["content"]
                        
                except asyncio.TimeoutError:
                    logger.warning(f"Request timeout on attempt {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        logger.info(f"Timeout detected - waiting {wait_time}s before retry...")
                        await asyncio.sleep(wait_time)
                        continue
                    logger.error(f"Request timed out after {max_retries} attempts")
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
                    try:
                        # More detailed error logging
                        error_type = type(e).__name__
                        error_str = str(e) if e else "None"
                        logger.error(f"Exception type: {error_type}, Error: {error_str}")
                        logger.error(f"Exception args: {e.args if hasattr(e, 'args') else 'No args'}")
                        
                        # Safe error message extraction
                        error_msg = ""
                        if e and hasattr(e, '__str__'):
                            try:
                                error_msg = str(e).lower()
                            except:
                                error_msg = ""
                        
                        logger.warning(f"Request failed on attempt {attempt+1}/{max_retries}: {error_msg if error_msg else 'unknown error'}")
                        
                        # Check for rate limit in error message safely
                        if error_msg and ("rate limit" in error_msg or "429" in error_msg):
                            # Track rate limit and use very aggressive backoff
                            async with self._lock:
                                self.rate_limit_count += 1
                                self.last_rate_limit_time = time.time()
                            # Very aggressive backoff: 30s, 60s, 90s, 120s, 120s
                            wait_time = min(MAX_RATE_LIMIT_WAIT, 30 * (attempt + 1))
                            logger.warning(f"Rate limit detected in exception, pausing for {wait_time}s...")
                            await asyncio.sleep(wait_time)
                    except Exception as debug_e:
                        logger.error(f"Error in error handling: {debug_e}")
                        logger.error(f"Original error: {e}")
                        logger.warning(f"Request failed on attempt {attempt+1}/{max_retries}: {e}")
                    
                    if attempt < max_retries - 1:
                        wait_time = RETRY_DELAY * (attempt + 1)
                        await asyncio.sleep(wait_time)
                        continue
                    raise
            
            raise Exception(f"All {max_retries} attempts failed")

processor = RateLimitedProcessor()

def generate_optimized_chat_prompt(chat_data):
    """Generate optimized prompt for chat content and analysis generation"""
    
    # Extract data from chat record - ALL EXISTING FIELDS FROM chat_new COLLECTION
    dominant_topic = chat_data.get('dominant_topic')
    subtopics = chat_data.get('subtopics')
    messages = chat_data.get('messages', [])
    message_count = len(messages) if messages else 2
    
    # EXISTING FIELDS FROM chat_new collection - USE THESE EXACT VALUES (NO DEFAULTS)
    urgency = chat_data.get('urgency')
    follow_up_required = chat_data.get('follow_up_required')
    action_pending_status = chat_data.get('action_pending_status')
    action_pending_from = chat_data.get('action_pending_from')
    priority = chat_data.get('priority')
    resolution_status = chat_data.get('resolution_status')
    overall_sentiment = chat_data.get('overall_sentiment')
    category = chat_data.get('category')  # external or internal
    
    # Extract participant names from messages - these are for conversation participants only
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
                
                # Extract message dates
                created_date = message.get('createdDateTime')
                if created_date:
                    message_dates.append(created_date)
    
    # Generate meaningful names for conversation participants (not client names)
    if len(participant_names) < 2:
        if category == 'external':
            # External: customer and bank employee
            participant_names = ['Customer', 'Bank_Employee']
        else:
            # Internal: bank employees only
            participant_names = ['Employee_1', 'Employee_2']
    
    # Get chat-level dates
    chat_created = chat_data.get('chat', {}).get('createdDateTime') if chat_data.get('chat') else None
    chat_last_updated = chat_data.get('chat', {}).get('lastUpdatedDateTime') if chat_data.get('chat') else None
    
    # Use actual message dates if available, otherwise use chat dates
    if message_dates:
        first_message_date = message_dates[0]
        last_message_date = message_dates[-1]
    else:
        first_message_date = chat_created
        last_message_date = chat_last_updated
    
    
    # Determine action pending context based on action_pending_from
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
    
    # Map urgency to conversation context
    urgency_context = "URGENT" if urgency else "NON-URGENT"
    
    # Build dynamic prompt based on actual data (no defaults)
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
    if category is not None:
        metadata_parts.append(f"Category:{category}")
    
    metadata_str = " | ".join(metadata_parts) if metadata_parts else "No metadata available"
    
    # Build sentiment description dynamically
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
    follow_up_desc = ""
    if follow_up_required == "yes":
        follow_up_desc = "End with open-ended scenarios"
    else:
        follow_up_desc = "End with complete resolution"
    
    # Build action description
    action_desc = ""
    if action_pending_status == "yes":
        action_desc = "Show waiting scenarios"
    else:
        action_desc = "Show completed processes"
    
    # Build action pending from description
    action_from_desc = ""
    if action_pending_status == "yes":
        if action_pending_from and action_pending_from.lower() == "customer":
            action_from_desc = "End with customer needing to respond/take action"
        elif action_pending_from and action_pending_from.lower() == "bank":
            action_from_desc = "End with bank needing to respond/take action"
        else:
            action_from_desc = "End with appropriate party needing to take action"
    else:
        action_from_desc = "End with completed process"
    
    # Build action pending from description for prompt
    action_pending_from_desc = ""
    if action_pending_status == "yes" and action_pending_from:
        if action_pending_from and action_pending_from.lower() == "customer":
            action_pending_from_desc = "End with customer needing to respond/take action"
        elif action_pending_from and action_pending_from.lower() == "bank":
            action_pending_from_desc = "End with bank needing to respond/take action"
        else:
            action_pending_from_desc = "End with appropriate party needing to take action"
    elif action_pending_status == "yes":
        action_pending_from_desc = "End with appropriate party needing to take action"
    else:
        action_pending_from_desc = "End with completed process"
    
    # Build participant string
    participant_str = " and ".join(participant_names) if len(participant_names) >= 2 else f"{participant_names[0]} and User_2" if participant_names else "User_1 and User_2"
    
    # Build message generation instructions based on category with actual dates
    message_instructions = []
    for i in range(message_count):
        if i < len(participant_names):
            user_name = participant_names[i]
        else:
            user_name = f"User_{i+1}"
        
        # Get the actual date for this message
        message_date = message_dates[i] if i < len(message_dates) else first_message_date
        
        # Check for day shift (different date from previous message)
        day_shift_instruction = ""
        if i > 0:
            prev_message_date = message_dates[i-1] if i-1 < len(message_dates) else first_message_date
            if prev_message_date and message_date:
                # Parse dates to compare
                try:
                    from datetime import datetime
                    prev_date = datetime.fromisoformat(prev_message_date.replace('Z', '+00:00')).date()
                    curr_date = datetime.fromisoformat(message_date.replace('Z', '+00:00')).date()
                    if prev_date != curr_date:
                        day_shift_instruction = " IMPORTANT: This message is on a different day than the previous message. Start with a natural day shift greeting like 'Good morning!', 'Hey, got an update', 'Following up on our discussion yesterday', or 'Hi again' to acknowledge the time gap."
                except:
                    pass  # If date parsing fails, continue without day shift instruction
        
        # Determine conversation context based on category - CRITICAL: Follow category exactly!
        # Handle both "external"/"External" and "internal"/"Internal"
        if category and category.lower() == 'external':
            if i == 0:
                # CRITICAL: External means CUSTOMER speaks first (not employee!)
                message_instructions.append(f'{{"content": "CUSTOMER message to BANK (10-100 words). Customer reaching out with banking question/issue/complaint. NOT an employee! Use first person (I, my, me). Sound like real customer: \'Hi, I need help with...\', \'Hello, I\'m having an issue...\', \'Can someone help me?\'. Topic: {dominant_topic if dominant_topic else "banking issue"}. Natural language, contractions, informal. Use emojis occasionally when expressing emotion (😅 for frustration, 🤔 for confusion, 😊 for thanks, ❓ for questions). Include realistic details. NEVER mention other customer names.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
            else:
                # Bank employee responding to customer
                message_instructions.append(f'{{"content": "BANK EMPLOYEE responding to CUSTOMER (10-100 words). Professional helpful response. Use phrases like \'I can help you with that\', \'Let me check\', \'I\'ll look into this\'. Address customer directly. Topic: {dominant_topic if dominant_topic else "banking business"}. Natural language, contractions, friendly tone. Use emojis sparingly when appropriate (👍 for confirmation, ✅ for completed, 📧 for email references). Include solutions, next steps. When mentioning OTHER clients in conversation, use names like John Smith, Maria Garcia.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
        else:
            # CRITICAL: Internal means EMPLOYEE to EMPLOYEE conversation (colleagues discussing work)
            if i == 0:
                message_instructions.append(f'{{"content": "BANK EMPLOYEE to COLLEAGUE (10-100 words). Internal work discussion between staff. Use phrases like \'Hey [name], just got a request from...\', \'Can you help me with...\', \'Did you process the...\'. Discussing CUSTOMERS\' issues, not their own. Topic: {dominant_topic if dominant_topic else "banking operations"}. Informal colleague chat. Use emojis occasionally for reactions (😅 for stress, 🤔 for thinking, 👍 for acknowledgment, 📁 for files). Mention customer names like Maria Garcia, John Smith when discussing their cases.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
            else:
                message_instructions.append(f'{{"content": "BANK EMPLOYEE replying to COLLEAGUE (10-100 words). Continue work discussion between staff. Use phrases like \'Let me check on that\', \'I\'ll look into it\', \'Yeah, I saw that request\'. Colleagues helping each other with customers\' cases. Topic: {dominant_topic if dominant_topic else "banking operations"}. Natural colleague conversation. Use emojis occasionally for emphasis (👍 for agreement, ✅ for done, 🔍 for searching, 😅 for challenges). Reference customer names like Maria Garcia, John Smith.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
    
    messages_json = ",\n  ".join(message_instructions)
    
    # No sentiment generation needed - use existing overall_sentiment
    
    # Determine conversation type and ending requirements (case-insensitive check)
    conversation_type = "EXTERNAL (Customer ↔ Bank)" if (category and category.lower() == 'external') else "INTERNAL (Bank Employee ↔ Bank Employee)"
    
    # Determine ending requirements based on action pending and follow-up
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

    prompt = f"""Generate EU banking chat conversation with {message_count} messages.

**METADATA:** {metadata_str}

**CONVERSATION TYPE:** {conversation_type}

**TIMELINE INFORMATION:**
- First message date: {first_message_date}
- Last message date: {last_message_date}
- Total message count: {message_count}

**PRIORITY LEVEL DEFINITION:**
- P1-Critical: Business stop → must resolve NOW (follow-up within 24-48 hours)
- P2-High: Major issue, limited users impacted, needs fast action (follow-up within 2-7 days)
- P3-Medium: Standard issues/requests, manageable timelines (follow-up within 1-2 weeks)
- P4-Low: Minor issues, no major business impact (follow-up within 2-4 weeks)
- P5-Very Low: Informational, FYI, archival (follow-up within 1-2 months)

**ACTION PENDING CONTEXT:** {action_pending_context}

**PARTICIPANTS:** {participant_str}

**RULES:** 
- Sentiment {overall_sentiment}/5: {"Extreme frustration throughout ALL messages" if overall_sentiment == 5 else "Clear anger/frustration" if overall_sentiment == 4 else "Moderate concern/unease" if overall_sentiment == 3 else "Slight irritation/impatience" if overall_sentiment == 2 else "Calm professional baseline" if overall_sentiment == 1 else "Positive satisfied communication"}
- Bank employees: ALWAYS calm, professional, helpful
- Follow-up {follow_up_required}: {"End with open-ended scenarios" if follow_up_required == "yes" else "End with complete resolution"}
- Action {action_pending_status}: {"Show waiting scenarios" if action_pending_status == "yes" else "Show completed processes"}
- Action Pending From {action_pending_from}: {action_pending_from_desc}

**CONVERSATION STRUCTURE - CRITICAL:**
- **External ({conversation_type})**: CUSTOMER writes FIRST message to BANK. Customer has problem/question. Bank employee responds helpfully. Customer uses first person (I, my, me). Example: "Hi, I'm having trouble with my account..." → "I can help you with that!"
- **Internal ({conversation_type})**: BANK EMPLOYEES discussing work with each other. Colleagues chatting about customers' cases. Use names like Sarah, Megan. Example: "Hey Sarah, Maria Garcia called about..." → "Oh yeah, let me check that..."
- **NEVER mix these up!** External = customer speaking, Internal = employees chatting
- Realistic chat messages 10-100 words each
- Natural human conversation flow
- Use contractions, informal tone, emojis
- Use EXACT dates provided for each message timestamp
- Consider time gaps between messages for natural conversation flow

**DATE-BASED CONVERSATION FLOW:**
- Use the EXACT dates provided for each message
- CRITICAL: If there are time gaps between messages, create natural conversation breaks
- For day shifts (different dates), ALWAYS start with greetings like:
  * "Good morning!" (for morning messages)
  * "Hey, got an update" (for afternoon/evening messages)
  * "Hi again" (for casual follow-ups)
  * "Following up on our discussion yesterday" (for next-day follow-ups)
  * "Quick update on..." (for urgent matters)
- For same-day gaps (hours apart), use phrases like:
  * "Got an update"
  * "Quick follow-up"
  * "Just checking in"
  * "Hey, just heard back"
- Consider business hours and urgency when crafting messages
- Make conversations feel natural based on the actual timeline
- ALWAYS acknowledge time gaps with appropriate conversation starters
- NEVER continue a conversation across days without acknowledging the time gap

**BANKING:** Realistic EU accounts | Specific amounts | Transaction IDs | Customer details | Authentic banking terminology

**CONVERSATION STYLE:** 
- Sound like real people chatting, not formal business emails
- Use contractions (I'm, we're, can't, won't, etc.)
- Include natural reactions (oh no!, really?, that's interesting, etc.)
- **Use emojis occasionally (NOT every message)** - add emojis when expressing emotion or emphasis:
  * Customer emotions: 😅 (frustration), 🤔 (confusion), 😊 (thanks), ❓ (questions), 😞 (disappointed)
  * Bank responses: 👍 (confirmation), ✅ (completed), 📧 (email), 💳 (card), 📱 (phone)
  * Employee chat: 😅 (stress), 🤔 (thinking), 👍 (acknowledgment), 📁 (files), 🔍 (searching), ✅ (done)
  * Use 1-2 emojis per message maximum, only where natural
- Ask follow-up questions naturally
- Show emotions and personality
- Use informal language while staying professional
- Include realistic banking scenarios and problems

**NAMING RULES:**
- Participant names are ONLY for conversation participants (Customer, Bank_Employee, Employee_1, etc.)
- When mentioning clients, customers, or issues in conversation content, use realistic names like John Smith, Maria Garcia, etc.
- NEVER use participant names when referring to clients or customers in the conversation content

**ENDING REQUIREMENTS:** {ending_str}

**FOLLOW-UP vs NEXT ACTION DISTINCTION:**
- **follow_up_reason** = "WHY" (the trigger/justification for follow-up)
  * Focus on the REASON/CAUSE that necessitates follow-up
  * Examples: "Customer requested status update", "Documentation incomplete", "Compliance deadline approaching", "Issue unresolved", "Waiting for external approval", "System error occurred"
- **next_action_suggestion** = "WHAT" (the specific step to take)
  * Focus on the CONCRETE ACTION to be performed
  * Examples: "Contact client to request missing documents", "Schedule compliance review meeting", "Escalate to senior management", "Update system with new information", "Send follow-up email to customer", "Review and approve pending application"

**EXAMPLE SCENARIO:**
- follow_up_reason: "Customer requested status update on loan application"
- next_action_suggestion: "Call customer to provide current application status and next steps"

**EXAMPLES OF GOOD CHAT MESSAGES (with appropriate emoji usage):**
- **External (Customer → Bank):** "Hi, I'm having trouble with my online banking. Can someone help me check my account balance?" ← CUSTOMER speaking (no emoji - straightforward question)
- **External (Bank → Customer):** "Of course! I can help you with that 👍 Let me look up your account details right away." ← BANK employee (emoji for confirmation)
- **External (Customer → Bank):** "Hello, I need help accessing my statements. My login isn't working 😅" ← CUSTOMER issue (emoji for frustration)
- **External (Bank → Customer):** "Thanks for your patience! I've reset your login ✅ You should be able to access it now." ← BANK (emoji for completion)
- **Internal (Employee → Employee):** "Hey Sarah, just got a call from Maria Garcia about her loan application. She's asking about the status 😅" ← EMPLOYEES discussing customer's case (emoji for stress)
- **Internal (Employee → Employee):** "Oh really? What's the issue with Maria's application? I thought we processed it yesterday" ← COLLEAGUES chatting (no emoji - straightforward)
- **Internal (Employee → Employee):** "Can you help me with the Johnson account? They're asking about wire transfer limits 🤔" ← STAFF helping each other (emoji for uncertainty)
- **Internal (Employee → Employee):** "Found it! I'll send you the file now 📁" ← COLLEAGUES (emoji for file reference)
- Day shift: "Good morning! Following up on our discussion about the Smith application..."
- Same day gap: "Quick follow-up - did you hear back from the client?"

**FINAL CHECK BEFORE GENERATING:**
- Conversation Type = {conversation_type}
- If External: First message MUST be from CUSTOMER (not employee!) - customer has a problem/question
- If Internal: All messages are EMPLOYEES chatting with each other about work/customers
- Double-check you're following the correct category!
- Priority = {priority} → {"follow_up_date MUST be within 24-48 hours of {last_message_date}" if priority and priority.startswith("P1") else "follow_up_date MUST be within 2-7 days of {last_message_date}" if priority and priority.startswith("P2") else "follow_up_date can be 1-2 weeks from {last_message_date}" if priority and priority.startswith("P3") else "follow_up_date can be 2-4 weeks from {last_message_date}" if priority and priority.startswith("P4") else "follow_up_date can be 1-2 months from {last_message_date}"} (ONLY if follow_up_required="yes")
- Use emojis occasionally (1-2 per message max) where they add emotion or emphasis, NOT in every message!

**OUTPUT:** {{
  "messages": [
    {messages_json}
  ],
  "analysis": {{
    "chat_summary": "Business summary 150-200 words describing discussion topic, participants, key points, and context",
    "follow_up_reason": {"[WHY follow-up is needed - the trigger/justification. Focus on the REASON/CAUSE that necessitates follow-up. Examples: 'Customer requested status update', 'Documentation incomplete', 'Compliance deadline approaching', 'Issue unresolved', 'Waiting for external approval', 'System error occurred', 'Client response required', 'Regulatory requirement pending'. Be specific about what triggered the need for follow-up.]" if follow_up_required == "yes" else "null"},
    "next_action_suggestion": {"[WHAT specific step to take - the actionable recommendation. Focus on the CONCRETE ACTION to be performed. Examples: 'Contact client to request missing documents', 'Schedule compliance review meeting', 'Escalate to senior management', 'Update system with new information', 'Send follow-up email to customer', 'Review and approve pending application', 'Coordinate with IT team for resolution', 'Prepare documentation for audit'. Be specific about what needs to be done.]" if follow_up_required == "yes" and action_pending_status == "yes" else "null"},
    "follow_up_date": {"[Generate follow-up date after {last_message_date} based on priority={priority}. CRITICAL RULES: P1-Critical=SAME DAY or next business day (24-48 hours MAX), P2-High=2-5 business days (MUST be within same week, 7 days MAX), P3-Medium=1-2 weeks, P4-Low=2-4 weeks, P5-Very Low=1-2 months. NEVER generate dates months away for P1/P2! Format: YYYY-MM-DDTHH:MM:SSZ]" if follow_up_required == "yes" else "null"}
  }}
}}

Use EXACT metadata values and EXACT dates provided. Implement concepts through natural scenarios, NOT explicit mentions. Generate authentic banking content with specific details.

**CRITICAL:** 
- Follow-up reason = "WHY" (the trigger/justification for follow-up) - ONLY if follow_up_required="yes", otherwise "null"
- Next-action suggestion = "WHAT" (the step you advise taking) - ONLY generate if follow_up_required="yes" AND action_pending_status="yes":
  * If action_pending_from="Customer": Suggest what the customer needs to do
  * If action_pending_from="Bank": Suggest what the bank needs to do
  * If both follow_up_required="no" AND action_pending_status="no": Set to "null"
- Follow-up date = "WHEN" - ONLY if follow_up_required="yes", otherwise "null"
  * **PRIORITY-BASED TIMING (STRICT):**
    - P1-Critical: SAME DAY or next business day (24-48 hours MAXIMUM from {last_message_date})
    - P2-High: 2-5 business days (MUST be within 7 days from {last_message_date})
    - P3-Medium: 1-2 weeks from {last_message_date}
    - P4-Low: 2-4 weeks from {last_message_date}
    - P5-Very Low: 1-2 months from {last_message_date}
  * **NEVER generate dates months away for P1-Critical or P2-High priorities!**
  * Current priority for this chat: {priority}
- Chat summary should reflect the conversation type (external vs internal) and include all relevant context
- All analysis fields should be meaningful and based on the actual conversation content
- Use the EXACT dates provided for message timestamps - do not generate new dates

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
        prompt = generate_optimized_chat_prompt(chat_data)
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
        
        # Remove markdown code fences more carefully using regex
        # Remove leading ```json or ``` and trailing ```
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
            # Reduce debug logging for performance
            if str(chat_id).endswith('0') or str(chat_id).endswith('5'):  # Log only every 10th chat
                logger.info(f"Chat {chat_id}: JSON parsing successful. Keys: {list(result.keys())}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing failed for chat {chat_id}. Raw response: {reply[:300]}...")
            logger.error(f"Chat {chat_id}: Full LLM response: {response[:500]}...")
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
            # Adjust to correct count
            if len(result['messages']) > message_count:
                result['messages'] = result['messages'][:message_count]
        
        # Validate required analysis fields
        required_analysis_fields = ['chat_summary']
        for field in required_analysis_fields:
            if field not in result['analysis']:
                logger.error(f"Chat {chat_id}: Missing required analysis field: {field}")
                raise ValueError(f"Missing required analysis field: {field}")
        
        # Get follow_up_required and action_pending_status from chat_data for validation
        chat_follow_up_required = chat_data.get('follow_up_required')
        chat_action_pending_status = chat_data.get('action_pending_status')
        
        # Validate conditional analysis fields based on follow_up_required
        if chat_follow_up_required == "yes":
            conditional_fields = ['follow_up_reason', 'follow_up_date']
            for field in conditional_fields:
                if field not in result['analysis']:
                    logger.error(f"Chat {chat_id}: Missing conditional analysis field: {field} (follow_up_required=yes)")
                    raise ValueError(f"Missing conditional analysis field: {field}")
        
        # Validate next_action_suggestion based on both follow_up_required and action_pending_status
        if chat_follow_up_required == "yes" and chat_action_pending_status == "yes":
            if 'next_action_suggestion' not in result['analysis']:
                logger.error(f"Chat {chat_id}: Missing next_action_suggestion (follow_up_required=yes, action_pending_status=yes)")
                raise ValueError("Missing next_action_suggestion field")
        
        # Validate message word counts for realism
        for i, message in enumerate(result.get('messages', [])):
            if isinstance(message, dict) and 'content' in message:
                content = message['content']
                word_count = len(content.split())
                if word_count < 10:
                    logger.warning(f"Chat {chat_id}: Message {i} too short ({word_count} words), expanding...")
                    # Add some context to make it more realistic
                    message['content'] = f"{content} Let me give you more details about this."
                elif word_count > 100:
                    logger.warning(f"Chat {chat_id}: Message {i} too long ({word_count} words), truncating...")
                    # Truncate to 100 words
                    words = content.split()
                    message['content'] = ' '.join(words[:100])
        
        # No need to validate follow_up_required - using existing value from database
        
        # No need to validate action_pending_from or dates - using existing values from database
        
        generation_time = time.time() - start_time
        
        # Log success with all preserved fields
        success_info = {
            'chat_id': str(chat_id),
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
            'dominant_topic': chat_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
        raise

async def process_single_chat(chat_record, total_chats=None):
    """Process a single chat record with comprehensive retry logic - NEVER GIVE UP"""
    if shutdown_flag.is_set():
        return None
    
    chat_id = str(chat_record.get('_id', 'unknown'))
    retry_count = checkpoint_manager.get_retry_count(chat_id)
    
    # Try up to MAX_RETRY_ATTEMPTS_PER_CHAT times
    for attempt in range(MAX_RETRY_ATTEMPTS_PER_CHAT):
        if shutdown_flag.is_set():
            return None
        
        try:
            if attempt > 0:
                # Exponential backoff for retries: 10s, 20s, 40s, 80s, 120s, 120s...
                retry_wait = min(120, RETRY_DELAY * (2 ** (attempt - 1)))  # Cap at 2 minutes
                logger.info(f"Chat {chat_id}: Retry attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS_PER_CHAT} after {retry_wait}s wait")
                await asyncio.sleep(retry_wait)
                await checkpoint_manager.increment_retry(chat_id)
            
            result = await _process_single_chat_internal(chat_record, total_chats)
            
            if result:
                # Success!
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
            # Safe error message extraction
            error_msg = ""
            if e and hasattr(e, '__str__'):
                try:
                    error_msg = str(e).lower()
                except:
                    error_msg = ""
            
            logger.warning(f"Chat {chat_id}: Error on attempt {attempt + 1}/{MAX_RETRY_ATTEMPTS_PER_CHAT}: {str(e)[:100] if e else 'unknown error'}")
            
            # Special handling for rate limits - very aggressive backoff
            if error_msg and ("rate limit" in error_msg or "429" in error_msg):
                wait_time = min(120, 30 * (attempt + 1))  # 30s, 60s, 90s, 120s, 120s...
                logger.info(f"Chat {chat_id}: Rate limit detected, waiting {wait_time}s before retry")
                await asyncio.sleep(wait_time)
            
            if attempt < MAX_RETRY_ATTEMPTS_PER_CHAT - 1:
                continue
            else:
                logger.error(f"Chat {chat_id}: Failed after {MAX_RETRY_ATTEMPTS_PER_CHAT} attempts: {str(e)[:100] if e else 'unknown error'}")
    
    # If we get here, all attempts failed
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
        
        # Debug: Log the generated content structure (reduced frequency)
        if str(chat_id).endswith('0'):  # Log only every 10th chat
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
        
        # Update analysis fields from LLM response - regenerate ALL analysis fields
        if 'analysis' in chat_content:
            analysis = chat_content['analysis']
            if isinstance(analysis, dict):
                # Regenerate ALL LLM-generated analysis fields (overwrite existing ones)
                update_doc['chat_summary'] = analysis.get('chat_summary')
                
                # Get follow_up_required and action_pending_status from chat_record for updates
                chat_follow_up_required = chat_record.get('follow_up_required')
                chat_action_pending_status = chat_record.get('action_pending_status')
                
                # Handle follow_up_reason based on follow_up_required status
                if chat_follow_up_required == "yes":
                    update_doc['follow_up_reason'] = analysis.get('follow_up_reason')
                else:
                    update_doc['follow_up_reason'] = None
                
                # Handle follow_up_date based on follow_up_required status
                if chat_follow_up_required == "yes":
                    follow_up_date = analysis.get('follow_up_date')
                    update_doc['follow_up_date'] = follow_up_date
                    # Debug logging for follow_up_date
                    if follow_up_date:
                        logger.info(f"Chat {chat_id}: Generated follow_up_date: {follow_up_date}")
                        logger.info(f"Chat {chat_id}: Adding follow_up_date to update_doc: {follow_up_date}")
                    else:
                        logger.warning(f"Chat {chat_id}: follow_up_date is None despite follow_up_required=yes")
                else:
                    update_doc['follow_up_date'] = None
                    logger.info(f"Chat {chat_id}: Setting follow_up_date to None (follow_up_required=no)")
                
                # Handle next_action_suggestion based on follow_up_required and action_pending_status
                if chat_follow_up_required == "yes" and chat_action_pending_status == "yes":
                    update_doc['next_action_suggestion'] = analysis.get('next_action_suggestion')
                else:
                    update_doc['next_action_suggestion'] = None
        
        # Ensure follow_up_date field exists in database (create with null if not exists)
        # Only set to None if we haven't already set it above
        if 'follow_up_date' not in update_doc and 'follow_up_date' not in chat_record:
            update_doc['follow_up_date'] = None
        
        # Add LLM processing tracking
        update_doc['llm_processed'] = True
        update_doc['llm_processed_at'] = datetime.now().isoformat()
        update_doc['llm_model_used'] = OPENROUTER_MODEL
        
        # Debug: Log what's being saved to database
        logger.info(f"Chat {chat_id}: Update document keys: {list(update_doc.keys())}")
        if 'follow_up_date' in update_doc:
            logger.info(f"Chat {chat_id}: follow_up_date in update_doc: {update_doc['follow_up_date']}")
        else:
            logger.warning(f"Chat {chat_id}: follow_up_date NOT in update_doc")
        
        await performance_monitor.record_success(total_chats)
        await success_counter.increment()
        
        return {
            'chat_id': chat_id,
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Chat {chat_id} internal processing failed: {str(e)[:100]}")
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
                filter={"_id": ObjectId(update_data['chat_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
        
        if bulk_operations:
            try:
                # Use ordered=False for better performance
                result = chat_col.bulk_write(bulk_operations, ordered=False)
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

async def process_chats_optimized():
    """Main optimized processing function for chat generation - regenerates BOTH message content AND analysis fields for records with null/empty message content"""
    logger.info("Starting Optimized EU Banking Chat Content Generation...")
    logger.info("Focus: Processing records with NULL/empty message content")
    logger.info("Action: Will regenerate BOTH message content AND analysis fields")
    logger.info(f"Collection: {CHAT_COLLECTION}")
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
    
    # Get chats to process - only those that have NEVER been processed by LLM
    try:
        # Query for chats that have null/empty message content - will regenerate BOTH body content AND analysis fields
        query = {
            "$and": [
                # Must have basic chat structure
                {"_id": {"$exists": True}},
                # Must have messages array
                {"messages": {"$exists": True, "$ne": None, "$ne": []}},
                # Must have at least one message with null/empty body content
                {
                    "$or": [
                        # Check for null content in any message
                        {"messages.body.content": {"$eq": None}},
                        {"messages.body.content": {"$eq": ""}},
                        {"messages.body.content": {"$exists": False}},
                        # Check for messages with missing body structure
                        {"messages.body": {"$exists": False}},
                        # Check for empty messages array
                        {"messages": {"$size": 0}}
                    ]
                }
            ]
        }
        
        # Exclude already processed chats
        if checkpoint_manager.processed_chats:
            processed_ids = [ObjectId(cid) for cid in checkpoint_manager.processed_chats if ObjectId.is_valid(cid)]
            query["_id"] = {"$nin": processed_ids}
        
        # Check chat status
        total_chats_in_db = chat_col.count_documents({})
        chats_processed_by_llm = chat_col.count_documents({"llm_processed": True})
        chats_with_basic_fields = chat_col.count_documents({
            "$and": [
                {"_id": {"$exists": True}},
                {"messages": {"$exists": True, "$ne": None, "$ne": []}}
            ]
        })
        chats_with_llm_fields = chat_col.count_documents({
            "$and": [
                {"chat_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"follow_up_reason": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        
        # Calculate chats with null/empty message content
        chats_with_null_content = chat_col.count_documents({
            "$and": [
                {"_id": {"$exists": True}},
                {"messages": {"$exists": True, "$ne": None, "$ne": []}},
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
        })
        
        # Calculate actual chats needing processing
        chats_needing_processing = chat_col.count_documents(query)
        
        # Calculate pending chats (those with null content)
        chats_pending_processing = chats_with_null_content
        
        # Debug: Let's also check what fields actually exist
        logger.info("Debug - Checking field distribution in chat_new collection:")
        for field in ["dominant_topic", "urgency", "follow_up_required", 
                      "action_pending_status", "priority", "resolution_status", 
                      "chat_summary", "next_action_suggestion", "follow_up_reason",
                      "follow_up_date", "overall_sentiment", "subtopics", "category"]:
            count = chat_col.count_documents({field: {"$exists": True, "$ne": None, "$ne": ""}})
            logger.info(f"  {field}: {count} chats have this field")
        
        # Calculate completion percentages
        completion_percentage = (chats_processed_by_llm / chats_with_basic_fields * 100) if chats_with_basic_fields > 0 else 0
        pending_percentage = (chats_pending_processing / chats_with_basic_fields * 100) if chats_with_basic_fields > 0 else 0
        
        logger.info(f"Database Status:")
        logger.info(f"  Total chats in DB: {total_chats_in_db}")
        logger.info(f"  Chats with required basic fields: {chats_with_basic_fields}")
        logger.info(f"  Chats with LLM-generated fields: {chats_with_llm_fields}")
        logger.info(f"  Chats with NULL/empty message content: {chats_with_null_content}")
        logger.info(f"  Chats processed by LLM (llm_processed=True): {chats_processed_by_llm}")
        logger.info(f"  Chats pending processing (null content): {chats_pending_processing}")
        logger.info(f"  Chats needing processing (this session): {chats_needing_processing}")
        logger.info(f"  Action: Will regenerate BOTH message content AND analysis fields")
        logger.info(f"  Overall Progress: {completion_percentage:.1f}% completed, {pending_percentage:.1f}% pending")
        
        # Use cursor instead of loading all into memory at once
        chat_records = chat_col.find(query).batch_size(100)
        total_chats = chat_col.count_documents(query)
        
        if total_chats == 0:
            logger.info("No chats found that need processing!")
            logger.info("All chats appear to have been processed by LLM already.")
            return
        
        logger.info(f"Found {total_chats} chats that need LLM processing")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_chats)} chats")
        
        # Log session progress
        progress_logger.info(f"SESSION_START: total_chats={total_chats}, completed={chats_processed_by_llm}, pending={chats_pending_processing}, completion_rate={completion_percentage:.1f}%")
        progress_logger.info(f"BATCH_START: total_chats={total_chats}")
        
    except Exception as e:
        logger.error(f"Error fetching chat records: {e}")
        return
    
    # Process chats in optimized batches
    total_updated = 0
    batch_updates = []
    
    try:
        # Process chats in concurrent batches using cursor
        batch_num = 0
        processed_count = 0
        
        while processed_count < total_chats:
            if shutdown_flag.is_set():
                logger.info("Shutdown requested, stopping processing")
                break
            
            batch_num += 1
            total_batches = (total_chats + BATCH_SIZE - 1)//BATCH_SIZE
            
            # Collect batch from cursor - process only 1 chat at a time
            batch = []
            for _ in range(BATCH_SIZE):  # BATCH_SIZE is now 1
                try:
                    chat = next(chat_records)
                    batch.append(chat)
                    processed_count += 1
                except StopIteration:
                    break
            
            if not batch:
                break
                
            logger.info(f"Processing batch {batch_num}/{total_batches} (chats {processed_count-len(batch)+1}-{processed_count})")
            
            # Process batch concurrently
            batch_tasks = []
            for chat in batch:
                chat_id = str(chat.get('_id'))
                
                # Check checkpoint to prevent duplicates (database check is already done in query)
                if not checkpoint_manager.is_processed(chat_id):
                    task = process_single_chat(chat, total_chats)
                    batch_tasks.append(task)
                else:
                    logger.info(f"Skipping already processed chat (checkpoint): {chat_id}")
            
            logger.info(f"Created {len(batch_tasks)} tasks for batch {batch_num}")
            
            if batch_tasks:
                # Process single task with ultra-conservative approach
                logger.info(f"Processing 1 task for batch {batch_num}")
                
                batch_start_time = time.time()
                successful_results = []
                failed_count = 0
                
                # Process single task WITHOUT outer timeout (process_single_chat has its own retry logic)
                try:
                    task = batch_tasks[0]  # Only one task
                    logger.info(f"Starting single task (with internal retry logic)")
                    
                    # No outer timeout - let the internal retry logic handle everything
                    result = await task
                    
                    if result:
                        successful_results.append(result)
                        # Mark as processed (non-blocking)
                        asyncio.create_task(
                            checkpoint_manager.mark_processed(result['chat_id'], success=True)
                        )
                        logger.info(f"Single task completed successfully")
                    else:
                        failed_count += 1
                        logger.warning(f"Single task returned no result after all retries")
                        
                except Exception as e:
                    failed_count += 1
                    # Safe error message extraction
                    error_msg = ""
                    if e and hasattr(e, '__str__'):
                        try:
                            error_msg = str(e).lower()
                        except:
                            error_msg = ""
                    
                    if error_msg and ("rate limit" in error_msg or "429" in error_msg):
                        wait_time = 30  # 30 second pause for rate limits at batch level
                        logger.warning(f"Rate limit detected at batch level, pausing for {wait_time}s...")
                        await asyncio.sleep(wait_time)
                    logger.error(f"Single task failed with error: {e}")
                
                if successful_results:
                    batch_updates.extend(successful_results)
                
                batch_elapsed = time.time() - batch_start_time
                logger.info(f"Batch {batch_num} completed in {batch_elapsed:.1f}s: {len(successful_results)}/1 successful, {failed_count} failed")
            
            # Save to database when we have enough updates
            if len(batch_updates) >= BATCH_SIZE:
                saved_count = await save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear batch
            
            # Progress update
            progress_pct = (processed_count / total_chats) * 100
            remaining_chats = total_chats - processed_count
            
            # Calculate overall completion including previously processed
            total_completed = chats_processed_by_llm + total_updated
            overall_completion = (total_completed / chats_with_basic_fields * 100) if chats_with_basic_fields > 0 else 0
            
            logger.info(f"Session Progress: {progress_pct:.1f}% ({processed_count}/{total_chats}) - {remaining_chats} remaining")
            logger.info(f"Overall Progress: {overall_completion:.1f}% completed ({total_completed}/{chats_with_basic_fields} total chats)")
            
            # Log detailed progress
            progress_logger.info(f"PROGRESS_UPDATE: session={progress_pct:.1f}%, overall={overall_completion:.1f}%, processed_this_session={total_updated}, remaining={remaining_chats}")
            
            # Update performance monitor with actual total
            await performance_monitor.log_progress(total_chats)
            
            # Short delay between batches (rate limiting is already handled in API call)
            if processed_count < total_chats and not shutdown_flag.is_set():
                await asyncio.sleep(BATCH_DELAY)
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = await save_batch_to_database(batch_updates)
            total_updated += saved_count
        
        # FINAL RETRY PASS: Retry all failed chats
        if checkpoint_manager.failed_chats and not shutdown_flag.is_set():
            failed_chat_ids = list(checkpoint_manager.failed_chats)
            logger.info(f"="*60)
            logger.info(f"FINAL RETRY PASS: Retrying {len(failed_chat_ids)} failed chats")
            logger.info(f"="*60)
            
            retry_success = 0
            retry_failed = 0
            
            for failed_chat_id in failed_chat_ids:
                if shutdown_flag.is_set():
                    break
                
                try:
                    # Fetch chat record from database
                    chat_record = chat_col.find_one({"_id": ObjectId(failed_chat_id)})
                    
                    if not chat_record:
                        logger.warning(f"Failed chat {failed_chat_id} not found in database")
                        continue
                    
                    logger.info(f"Final retry pass: Processing failed chat {failed_chat_id}")
                    
                    # Process with full retry logic
                    result = await process_single_chat(chat_record, total_chats)
                    
                    if result:
                        # Save successful result
                        saved_count = await save_batch_to_database([result])
                        if saved_count > 0:
                            retry_success += 1
                            total_updated += 1
                            logger.info(f"Final retry pass: SUCCESS for chat {failed_chat_id}")
                    else:
                        retry_failed += 1
                        logger.warning(f"Final retry pass: FAILED for chat {failed_chat_id}")
                    
                    # Short delay between retries
                    await asyncio.sleep(BATCH_DELAY)
                    
                except Exception as e:
                    retry_failed += 1
                    logger.error(f"Final retry pass error for chat {failed_chat_id}: {e}")
            
            logger.info(f"Final retry pass complete: {retry_success} succeeded, {retry_failed} failed")
            logger.info(f"="*60)
        
        # Final checkpoint save
        await checkpoint_manager.save_checkpoint()
        
        if shutdown_flag.is_set():
            logger.info("Processing interrupted gracefully!")
        else:
            logger.info("Optimized chat content generation complete!")
        
        # Final statistics
        final_total_completed = chats_processed_by_llm + total_updated
        final_completion_percentage = (final_total_completed / chats_with_basic_fields * 100) if chats_with_basic_fields > 0 else 0
        final_pending = chats_with_basic_fields - final_total_completed
        
        logger.info(f"Final Results:")
        logger.info(f"  Total chats updated this session: {total_updated}")
        logger.info(f"  Total chats completed (all time): {final_total_completed}")
        logger.info(f"  Total chats pending: {final_pending}")
        logger.info(f"  Overall completion rate: {final_completion_percentage:.1f}%")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        logger.info(f"  Total retry attempts: {checkpoint_manager.stats.get('retry_count', 0)}")
        logger.info(f"  Chats still in failed state: {len(checkpoint_manager.failed_chats)}")
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_chat = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per chat: {avg_time_per_chat:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} chats/hour" if total_time > 0 else "Processing rate: N/A")
        
        progress_logger.info(f"FINAL_SUMMARY: session_updated={total_updated}, total_completed={final_total_completed}, pending={final_pending}, completion_rate={final_completion_percentage:.1f}%, success={success_counter.value}, failures={failure_counter.value}, retries={checkpoint_manager.stats.get('retry_count', 0)}, total_time={total_time/3600:.2f}h, rate={success_counter.value/(total_time/3600):.0f}/h" if total_time > 0 else f"FINAL_SUMMARY: session_updated={total_updated}, total_completed={final_total_completed}, pending={final_pending}, completion_rate={final_completion_percentage:.1f}%, success={success_counter.value}, failures={failure_counter.value}, retries={checkpoint_manager.stats.get('retry_count', 0)}")
        
        # Save list of permanently failed chats to a file for manual review
        if checkpoint_manager.failed_chats:
            failed_chats_file = LOG_DIR / f"permanently_failed_chats_{timestamp}.json"
            try:
                failed_chats_details = []
                for failed_id in checkpoint_manager.failed_chats:
                    failed_chats_details.append({
                        'chat_id': failed_id,
                        'retry_attempts': checkpoint_manager.get_retry_count(failed_id)
                    })
                
                with open(failed_chats_file, 'w') as f:
                    json.dump(failed_chats_details, f, indent=2)
                
                logger.warning(f"ATTENTION: {len(checkpoint_manager.failed_chats)} chats could not be processed after multiple retries")
                logger.warning(f"Failed chat IDs saved to: {failed_chats_file}")
                logger.warning(f"Please review and retry these chats manually or re-run the script")
            except Exception as e:
                logger.error(f"Could not save failed chats list: {e}")
        
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
            'X-Title': 'EU Banking Chat Generator'
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
    """Get collection statistics for chats"""
    try:
        total_count = chat_col.count_documents({})
        
        with_complete_analysis = chat_col.count_documents({
            "chat_summary": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "follow_up_reason": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Stats by urgency
        urgent_chats = chat_col.count_documents({"urgency": True})
        
        # Stats by priority
        pipeline_priority = [
            {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        priority_dist = list(chat_col.aggregate(pipeline_priority))
        
        # Stats by resolution status
        pipeline_resolution = [
            {"$group": {"_id": "$resolution_status", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        resolution_dist = list(chat_col.aggregate(pipeline_resolution))
        
        without_complete_analysis = total_count - with_complete_analysis
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(chat_col.aggregate(pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"  Total chats: {total_count}")
        logger.info(f"  With complete LLM analysis: {with_complete_analysis}")
        logger.info(f"  Without complete analysis: {without_complete_analysis}")
        logger.info(f"  Urgent chats: {urgent_chats} ({(urgent_chats/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent chats: 0")
        logger.info(f"  Completion rate: {(with_complete_analysis/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
        logger.info("Priority Distribution:")
        for item in priority_dist:
            logger.info(f"  {item['_id']}: {item['count']} chats")
        
        logger.info("Resolution Status Distribution:")
        for item in resolution_dist:
            logger.info(f"  {item['_id']}: {item['count']} chats")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"  {i}. {topic['_id']}: {topic['count']} chats")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_chats(limit=3):
    """Get sample chats with generated analysis"""
    try:
        samples = list(chat_col.find({
            "chat_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Chat Analysis:")
        for i, chat in enumerate(samples, 1):
            logger.info(f"--- Sample Chat {i} ---")
            logger.info(f"Chat ID: {chat.get('_id', 'N/A')}")
            logger.info(f"Dominant Topic: {chat.get('dominant_topic', 'N/A')}")
            logger.info(f"Urgency: {chat.get('urgency', 'N/A')}")
            logger.info(f"Priority: {chat.get('priority', 'N/A')}")
            logger.info(f"Resolution Status: {chat.get('resolution_status', 'N/A')}")
            logger.info(f"Chat Summary: {str(chat.get('chat_summary', 'N/A'))[:150]}...")
            if 'overall_sentiment' in chat:
                logger.info(f"Overall Sentiment: {chat['overall_sentiment']}")
            
    except Exception as e:
        logger.error(f"Error getting sample chats: {e}")

async def main():
    """Main async function"""
    logger.info("Optimized EU Banking Chat Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{CHAT_COLLECTION}")
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
        await process_chats_optimized()
        
        # Show final stats
        logger.info("="*60)
        logger.info("FINAL STATISTICS")
        logger.info("="*60)
        get_collection_stats()
        get_sample_generated_chats(3)
        
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
