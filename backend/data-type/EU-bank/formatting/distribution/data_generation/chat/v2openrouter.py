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

# Ultra-conservative configuration for OpenRouter free tier
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"
BATCH_SIZE = 1  # Process only 1 chat at a time
MAX_CONCURRENT = 1  # Single concurrent call to avoid rate limits
REQUEST_TIMEOUT = 180  # Reduced timeout to 3 minutes
MAX_RETRIES = 3  # Fewer retries to avoid long waits
RETRY_DELAY = 300  # 5 minute retry delay for rate limits
BATCH_DELAY = 30.0  # 30 second delay between batches
API_CALL_DELAY = 15.0  # 15 second delay between API calls
CHECKPOINT_SAVE_INTERVAL = 5  # Very frequent checkpoints
RATE_LIMIT_BACKOFF_MULTIPLIER = 2  # Moderate backoff
MAX_RATE_LIMIT_WAIT = 1800  # 30 minute max wait for rate limits

# OpenRouter setup
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY2")
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
    def __init__(self, failure_threshold=3, recovery_timeout=300):
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
                    self.processed_chats = set(data.get('processed_chats', []))
                    self.failed_chats = set(data.get('failed_chats', []))
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
                    'stats': self.stats,
                    'timestamp': datetime.now().isoformat()
                }
                with open(self.checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
            except Exception as e:
                logger.error(f"Could not save checkpoint: {e}")
    
    def is_processed(self, chat_id):
        return str(chat_id) in self.processed_chats
    
    async def mark_processed(self, chat_id, success=True):
        async with self._lock:
            chat_id_str = str(chat_id)
            self.processed_chats.add(chat_id_str)
            self.stats['processed_count'] += 1
            
            if success:
                self.stats['success_count'] += 1
                self.failed_chats.discard(chat_id_str)
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
        """Async OpenRouter API call with rate limiting and retries"""
        async with self.semaphore:
            # Intelligent rate limiting based on recent rate limit hits
            async with self._lock:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                
                # Decay rate limit counter over time (reset after 10 minutes)
                if self.rate_limit_count > 0 and current_time - self.last_rate_limit_time > 600:
                    self.rate_limit_count = 0
                    logger.info("Rate limit counter decayed, resetting to normal delays")
                
                # If we've hit rate limits recently, increase delay
                if self.rate_limit_count > 0:
                    # Increase delay based on recent rate limits
                    base_delay = API_CALL_DELAY + (self.rate_limit_count * 30)  # Add 30s per recent rate limit
                    delay = max(base_delay, 60)  # Minimum 60 seconds
                    logger.info(f"Rate limit history detected ({self.rate_limit_count} recent), using {delay}s delay")
                else:
                    delay = API_CALL_DELAY
                
                if time_since_last < delay:
                    await asyncio.sleep(delay - time_since_last)
                self.last_request_time = time.time()
            
            # Ultra-conservative delay for OpenRouter free tier
            await asyncio.sleep(30.0)
            
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
                            
                            wait_time = min(MAX_RATE_LIMIT_WAIT, RETRY_DELAY * (RATE_LIMIT_BACKOFF_MULTIPLIER ** attempt))
                            logger.warning(f"Rate limited (429) on attempt {attempt+1}/{max_retries}, waiting {wait_time}s")
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
    dominant_topic = chat_data.get('dominant_topic', 'General Banking')
    subtopics = chat_data.get('subtopics', 'General operations')
    messages = chat_data.get('messages', [])
    message_count = len(messages) if messages else 2
    
    # EXISTING FIELDS FROM chat_new collection - USE THESE EXACT VALUES
    urgency = chat_data.get('urgency', False)
    follow_up_required = chat_data.get('follow_up_required', 'no')
    action_pending_status = chat_data.get('action_pending_status', 'no')
    action_pending_from = chat_data.get('action_pending_from', None)
    priority = chat_data.get('priority', 'P3-Medium')
    resolution_status = chat_data.get('resolution_status', 'open')
    overall_sentiment = chat_data.get('overall_sentiment', 1)
    
    # Extract participant names from messages
    participant_names = []
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
    
    # If no valid participants found, generate generic names
    if len(participant_names) < 2:
        participant_names = [f"User_{i+1}" for i in range(2)]
    
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
    
    # Map urgency to conversation context
    urgency_context = "URGENT" if urgency else "NON-URGENT"
    
    # Build dynamic prompt based on actual data
    metadata_parts = []
    if dominant_topic:
        metadata_parts.append(f"Topic:{dominant_topic}")
    if subtopics:
        metadata_parts.append(f"Subtopic:{subtopics}")
    if overall_sentiment is not None:
        metadata_parts.append(f"Sentiment:{overall_sentiment}/5")
    if urgency is not None:
        metadata_parts.append(f"Urgency:{'URGENT' if urgency else 'NON-URGENT'}")
    if follow_up_required:
        metadata_parts.append(f"Follow-up:{follow_up_required}")
    if action_pending_status:
        metadata_parts.append(f"Action:{action_pending_status}")
    if action_pending_from:
        metadata_parts.append(f"Action From:{action_pending_from}")
    if priority:
        metadata_parts.append(f"Priority:{priority}")
    if resolution_status:
        metadata_parts.append(f"Resolution:{resolution_status}")
    
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
    
    # Build participant string
    participant_str = " and ".join(participant_names) if len(participant_names) >= 2 else f"{participant_names[0]} and User_2" if participant_names else "User_1 and User_2"
    
    # Build message generation instructions
    message_instructions = []
    for i in range(message_count):
        if i < len(participant_names):
            user_name = participant_names[i]
        else:
            user_name = f"User_{i+1}"
        
        if i == 0:
            message_instructions.append(f'{{"content": "Realistic chat message 10-100 words from {user_name}. Make it sound like real people talking - use natural language, contractions, informal tone, emojis when appropriate, and realistic banking scenarios. Base content on {dominant_topic or "banking business"}. Use conversational style like colleagues chatting - not formal emails. Include realistic details, questions, reactions, and natural flow. Vary length: short responses (10-30 words) for quick replies, medium (40-70 words) for explanations, longer (80-100 words) for detailed discussions.", "from_user": "{user_name}", "timestamp": "2025-MM-DD HH:MM:SS (Generate date between 2025-01-01 and 2025-06-30, use realistic business hours 08:00-18:00 for routine discussions, 00:00-23:59 for urgent matters)"}}')
        else:
            message_instructions.append(f'{{"content": "Realistic chat message 10-100 words from {user_name}. Continue the conversation naturally - respond to previous message with real human reactions, questions, clarifications, or follow-ups. Use natural language, contractions, informal tone, emojis when appropriate. Base content on {dominant_topic or "banking business"}. Sound like colleagues having a real conversation - not formal business communication. Include realistic details, emotions, and natural conversation flow. Vary length: short responses (10-30 words) for quick replies, medium (40-70 words) for explanations, longer (80-100 words) for detailed discussions.", "from_user": "{user_name}", "timestamp": "2025-MM-DD HH:MM:SS (Generate date between 2025-01-01 and 2025-06-30, use realistic business hours 08:00-18:00 for routine discussions, 00:00-23:59 for urgent matters)"}}')
    
    messages_json = ",\n  ".join(message_instructions)
    
    # No sentiment generation needed - use existing overall_sentiment
    
    prompt = f"""Generate EU banking chat conversation with {message_count} messages.

**METADATA:** Topic:{dominant_topic} | Subtopic:{subtopics} | Sentiment:{overall_sentiment}/5 | Urgency:{urgency} | Follow-up:{follow_up_required} | Action:{action_pending_status} | Action From:{action_pending_from} | Priority:{priority} | Resolution:{resolution_status}

**ACTION PENDING CONTEXT:** {action_pending_context}

**PARTICIPANTS:** {participant_str}

**RULES:** 
- Sentiment {overall_sentiment}/5: {"Extreme frustration throughout ALL messages" if overall_sentiment == 5 else "Clear anger/frustration" if overall_sentiment == 4 else "Moderate concern/unease" if overall_sentiment == 3 else "Slight irritation/impatience" if overall_sentiment == 2 else "Calm professional baseline" if overall_sentiment == 1 else "Positive satisfied communication"}
- Bank employees: ALWAYS calm, professional, helpful
- Follow-up {follow_up_required}: {"End with open-ended scenarios" if follow_up_required == "yes" else "End with complete resolution"}
- Action {action_pending_status}: {"Show waiting scenarios" if action_pending_status == "yes" else "Show completed processes"}
- Action Pending From {action_pending_from}: {"End with customer needing to respond/take action" if action_pending_from and action_pending_from.lower() == "customer" else "End with bank needing to respond/take action" if action_pending_from and action_pending_from.lower() == "bank" else "End with appropriate party needing to take action" if action_pending_status == "yes" else "End with completed process"}

**STRUCTURE:** Realistic chat messages 10-100 words each | Natural human conversation flow | Use contractions, informal tone, emojis | Dates: 2025-01-01 to 2025-06-30 | Business hours 08:00-18:00 for routine, any time for urgent

**BANKING:** Realistic EU accounts | Specific amounts | Transaction IDs | Customer details | Authentic banking terminology

**CONVERSATION STYLE:** 
- Sound like real colleagues chatting, not formal business emails
- Use contractions (I'm, we're, can't, won't, etc.)
- Include natural reactions (oh no!, really?, that's interesting, etc.)
- Use emojis appropriately (😅, 👍, 🤔, etc.)
- Ask follow-up questions naturally
- Show emotions and personality
- Use informal language while staying professional
- Include realistic banking scenarios and problems

**EXAMPLES OF GOOD CHAT MESSAGES:**
- "Hey Sarah, just got a call about that loan application. The customer's asking about the status 😅"
- "Oh really? What's the issue? I thought we processed it yesterday"
- "Yeah, there's a missing document. Can you check the file? It's urgent apparently"
- "Sure thing! Let me look into it right now 👍"

**OUTPUT:** {{
  "messages": [
    {messages_json}
  ],
  "analysis": {{
    "chat_summary": "Business summary 150-200 words describing discussion topic, participants, key points",
    "follow_up_reason": {"[WHY follow-up is needed - the trigger/justification]" if follow_up_required == "yes" else "null"},
    "next_action_suggestion": {"[WHAT step to take - the action recommendation]" if follow_up_required == "yes" and action_pending_status == "yes" else "null"}
  }}
}}

Use EXACT metadata values. Implement concepts through natural scenarios, NOT explicit mentions. Generate authentic banking content with specific details.

**CRITICAL:** 
- Follow-up reason = "WHY" (the trigger/justification for follow-up) - ONLY if follow_up_required="yes", otherwise "null"
- Next-action suggestion = "WHAT" (the step you advise taking) - ONLY generate if follow_up_required="yes" AND action_pending_status="yes":
  * If action_pending_from="Customer": Suggest what the customer needs to do
  * If action_pending_from="Bank": Suggest what the bank needs to do
  * If both follow_up_required="no" AND action_pending_status="no": Set to "null"

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
        prompt = generate_optimized_chat_prompt(chat_data)
        
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
        
        # Validate analysis fields
        analysis_fields = ['chat_summary', 'follow_up_reason', 'next_action_suggestion']
        for field in analysis_fields:
            if field not in result['analysis']:
                logger.error(f"Chat {chat_id}: Missing analysis field: {field}")
                raise ValueError(f"Missing analysis field: {field}")
        
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
    """Process a single chat record with all optimizations"""
    if shutdown_flag.is_set():
        return None
    
    chat_id = str(chat_record.get('_id', 'unknown'))
    
    try:
        return await _process_single_chat_internal(chat_record, total_chats)
    except Exception as e:
        logger.error(f"Chat {chat_id} processing failed with error: {str(e)[:100]}")
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
                update_doc['follow_up_reason'] = analysis.get('follow_up_reason')
                update_doc['next_action_suggestion'] = analysis.get('next_action_suggestion')
        
        # Add LLM processing tracking
        update_doc['llm_processed'] = True
        update_doc['llm_processed_at'] = datetime.now().isoformat()
        update_doc['llm_model_used'] = OPENROUTER_MODEL
        
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
                      "overall_sentiment", "subtopics"]:
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
                
                # Process single task with maximum conservative settings
                try:
                    task = batch_tasks[0]  # Only one task
                    task_timeout = REQUEST_TIMEOUT + 30  # 3.5 minutes per task
                    logger.info(f"Starting single task with {task_timeout}s timeout")
                    
                    result = await asyncio.wait_for(task, timeout=task_timeout)
                    
                    if result:
                        successful_results.append(result)
                        # Mark as processed (non-blocking)
                        asyncio.create_task(
                            checkpoint_manager.mark_processed(result['chat_id'], success=True)
                        )
                        logger.info(f"Single task completed successfully")
                    else:
                        failed_count += 1
                        logger.warning(f"Single task returned no result")
                        
                except asyncio.TimeoutError:
                    failed_count += 1
                    logger.error(f"Single task timed out after {task_timeout}s")
                except Exception as e:
                    failed_count += 1
                    error_msg = str(e).lower()
                    if "rate limit" in error_msg or "429" in error_msg:
                        logger.warning(f"Rate limit detected, pausing for 300 seconds (5 minutes)...")
                        await asyncio.sleep(300)  # 5 minute pause for rate limits
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
            
            # Longer delay between batches to manage rate limits
            if processed_count < total_chats and not shutdown_flag.is_set():
                logger.info(f"Waiting {BATCH_DELAY}s before next batch to avoid rate limits...")
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
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_chat = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per chat: {avg_time_per_chat:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} chats/hour" if total_time > 0 else "Processing rate: N/A")
        
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
