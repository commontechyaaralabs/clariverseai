# EU Banking Chat Content Generator - Optimized Version
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
CHAT_COLLECTION = "chat"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"optimized_chat_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"progress_{timestamp}.log"
CHECKPOINT_FILE = LOG_DIR / f"checkpoint_{timestamp}.json"

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

# Conservative configuration to avoid rate limiting
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"
BATCH_SIZE = 3  # Very small batch size to reduce load
MAX_CONCURRENT = 1  # Single concurrent call to avoid rate limits
REQUEST_TIMEOUT = 120  # Keep timeout for detailed generation
MAX_RETRIES = 5  # More retries for rate limit recovery
RETRY_DELAY = 5  # Longer retry delay
BATCH_DELAY = 5.0  # Much longer batch delay
API_CALL_DELAY = 2.0  # Much longer API delay between calls
CHECKPOINT_SAVE_INTERVAL = 10  # Very frequent checkpoints

# OpenRouter setup
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY2")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

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
        if self.chats_processed % 100 == 0 and self.chats_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.chats_processed / elapsed if elapsed > 0 else 0
            remaining_chats = (total_chats - self.chats_processed) if total_chats else 0
            eta = remaining_chats / rate if rate > 0 and remaining_chats > 0 else 0
            
            logger.info(f"Performance Stats:")
            if total_chats:
                logger.info(f"  Processed: {self.chats_processed}/{total_chats} chats")
            else:
                logger.info(f"  Processed: {self.chats_processed} chats")
            logger.info(f"  Rate: {rate:.2f} chats/second ({rate*3600:.0f} chats/hour)")
            logger.info(f"  Success rate: {self.successful_requests/(self.successful_requests + self.failed_requests)*100:.1f}%")
            if eta > 0:
                logger.info(f"  ETA: {eta/3600:.1f} hours remaining")

performance_monitor = PerformanceMonitor()

# Circuit Breaker for handling rate limits
class CircuitBreaker:
    def __init__(self, failure_threshold=5, recovery_timeout=60):
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
            
            # Auto-save every CHECKPOINT_SAVE_INTERVAL
            if self.stats['processed_count'] % CHECKPOINT_SAVE_INTERVAL == 0:
                await self.save_checkpoint()

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
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def generate_optimized_prompt(chat_data):
    """Generate highly optimized prompt for chat content"""
    dominant_topic = chat_data.get('dominant_topic', 'General Banking Discussion')
    subtopics = chat_data.get('subtopics', 'Business discussion')
    message_count = chat_data.get('chat', {}).get('message_count', 2)
    existing_urgency = chat_data.get('urgency', False)
    existing_follow_up = chat_data.get('follow_up_required', 'no')
    
    # Get participant names from chat record
    participants = chat_data.get('chat', {}).get('members', [])
    participant_names = [member.get('displayName', 'User') for member in participants[:2]]  # Get first 2 participants
    if len(participant_names) < 2:
        participant_names = ['Alexa Thomas', 'Derrick Perry']  # Default names
    
    urgency_context = "URGENT" if existing_urgency else "NON-URGENT"
    
    prompt = f"""Generate EU banking chat conversation JSON. CRITICAL: Return ONLY valid JSON, no other text.

CONTEXT:
Topic: {dominant_topic} | Subtopic: {subtopics}
Messages: {message_count} | Urgency: {urgency_context} ({existing_urgency})
Follow-up Required: {existing_follow_up} (MUST PRESERVE THIS VALUE)
Participants: {participant_names[0]} and {participant_names[1]}

DEPARTMENTS AVAILABLE:
- Finance Department
- Risk Management
- Compliance Department  
- IT Department
- Operations Department
- Human Resources
- Audit Department
- Customer Service
- Legal Department

OUTPUT FORMAT REQUIRED - EXACTLY THIS STRUCTURE:
{{
  "assigned_department": "exact department name from list above",
  "chat_summary": "Business summary 150-200 words describing discussion topic, participants, key points",
  "action_pending_status": "yes|no",
  "action_pending_from": "company|customer|null (null if action_pending_status=no)",
  "chat_status": "active|resolved|archived",
  "follow_up_required": "yes|no (MUST match existing value: {existing_follow_up})",
  "follow_up_date": "2025-MM-DDTHH:MM:SS or null (provide date if follow_up_required=yes, can be null if no)",
  "follow_up_reason": "specific reason WHY follow-up is needed or null (examples: 'To finalize quarterly budget review', 'To complete compliance documentation', 'To review implementation progress', 'To gather additional stakeholder feedback', 'To confirm action items completion', 'To schedule follow-up meeting', 'To validate proposed solutions', 'To monitor project status', 'To ensure regulatory requirements are met' - provide contextual reason if follow_up_required=yes, null if no)",
  "next_action_suggestion": "Next step recommendation 50-80 words",
  "messages": [
    {{
      "content": "Professional chat message 10-150 words from {participant_names[0]}. Use natural business conversation style with proper formatting. Base content on {dominant_topic} and {subtopics}. Message should be contextually relevant to banking business discussion. Use professional but conversational tone appropriate for internal team chat. Vary message length realistically - some messages can be short (10-30 words) for quick responses, others longer (50-150 words) for detailed discussions.",
      "from_user": "{participant_names[0]}",
      "timestamp": "2025-MM-DD HH:MM:SS (Generate date between 2025-01-01 and 2025-06-30, use realistic business hours 08:00-18:00 for routine discussions, 00:00-23:59 for urgent matters)"
    }}{"," if message_count > 1 else ""}
    {"{"}"content": "Professional chat message 10-150 words from {participant_names[1]}. Use natural business conversation style responding to previous message. Base content on {dominant_topic} and {subtopics}. Message should continue the conversation naturally. Use professional but conversational tone appropriate for internal team chat. Vary message length realistically - some messages can be short (10-30 words) for quick responses, others longer (50-150 words) for detailed discussions.",
    "from_user": "{participant_names[1]}",
    "timestamp": "2025-MM-DD HH:MM:SS (Generate date between 2025-01-01 and 2025-06-30, use realistic business hours 08:00-18:00 for routine discussions, 00:00-23:59 for urgent matters)"
    {"}"}{"" if message_count <= 2 else "... continue alternating pattern for " + str(message_count) + " total messages with same detailed format"}
  ],
  "sentiment": {{"0": sentiment_score_message_1, "1": sentiment_score_message_2{"..." if message_count > 2 else ""}}} (Individual message sentiment analysis using human emotional tone 0-5 scale. Generate sentiment for each message based on message_count:
- 0: Happy (pleased, satisfied, positive)
- 1: Calm (baseline for professional communication)  
- 2: Bit Concerned (slight concern or questioning)
- 3: Moderately Concerned (growing concern or urgency)
- 4: Stressed (clear stress or pressure)
- 5: Very Stressed (high stress, very urgent)
CRITICAL: If message_count is 1, only generate sentiment for message "0". If message_count is 2, generate sentiment for "0" and "1", etc.),
  "overall_sentiment": 0.0-5.0 (overall chat sentiment based on discussion urgency and tone),
  "chat_started": "2025-01-01T08:00:00 to 2025-06-30T18:00:00 (business hours for routine, after-hours for urgent)",
  "thread_dates": {{
    "first_message_at": "2025-MM-DD HH:MM:SS (Use the earliest timestamp from messages)",
    "last_message_at": "2025-MM-DD HH:MM:SS (Use the latest timestamp from messages)"
  }}
}}

VALIDATION REQUIREMENTS:
✓ Generate exactly {message_count} messages alternating between {participant_names[0]} and {participant_names[1]}
✓ Match urgency={existing_urgency} in content tone
✓ Each message must be 10-150 words with realistic business chat format (vary length naturally - short responses 10-30 words, detailed messages 50-150 words)
✓ Messages should be natural conversation about {dominant_topic} related to banking business
✓ Use professional but conversational tone suitable for internal team communication
✓ Sentiment matches message count: {message_count} entries (CRITICAL: If message_count=1, only generate sentiment for "0". If message_count=2, generate for "0" and "1", etc.)
✓ Sentiment analysis: Use business communication tone scale (0: Happy/positive, 1: Calm/professional baseline, 2: Bit Concerned/questioning, 3: Moderately Concerned/urgent, 4: Stressed/pressured, 5: Very Stressed/critical)
✓ Overall sentiment: Consider discussion urgency and business impact
✓ Follow-up fields: CRITICAL - follow_up_required MUST match existing value "{existing_follow_up}". If existing value is "no", set follow_up_required="no" and leave date/reason as null. If existing value is "yes", set follow_up_required="yes" and provide meaningful follow_up_date and specific follow_up_reason explaining WHY follow-up is needed
✓ Date generation: CRITICAL - All dates must be between 2025-01-01 and 2025-06-30. Use format "2025-MM-DD HH:MM:SS". Generate realistic chronological order. Use business hours (08:00-18:00) for routine discussions, any time (00:00-23:59) for urgent matters. Set thread_dates properly based on message timestamps.
✓ Include realistic banking business terminology appropriate to the specific topic
✓ Department assignment should be contextually relevant to the discussion topic

Return ONLY the JSON object above with realistic values.
"""
    
    return prompt

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
                    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    async with session.post(OPENROUTER_URL, json=payload, headers=headers, timeout=timeout) as response:
                        
                        if response.status == 429:  # Rate limited
                            wait_time = min(30, 5 * (2 ** attempt))  # Exponential backoff with max 30s
                            logger.warning(f"Rate limited, waiting {wait_time}s before retry {attempt+1}/{max_retries}")
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
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    raise
                
                except aiohttp.ClientResponseError as e:
                    if e.status == 429:  # Rate limit - already handled above
                        continue
                    logger.error(f"HTTP error {e.status} on attempt {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    raise
                
                except Exception as e:
                    logger.warning(f"Request failed on attempt {attempt+1}/{max_retries}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    raise
            
            raise Exception(f"All {max_retries} attempts failed")

processor = RateLimitedProcessor()

async def generate_chat_content(chat_data):
    """Generate chat content with optimized processing"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    chat_id = str(chat_data.get('_id', 'unknown'))
    
    try:
        prompt = generate_optimized_prompt(chat_data)
        
        # Create session for this batch
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
            logger.info(f"Chat {chat_id}: JSON parsing successful. Keys: {list(result.keys())}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing failed for chat {chat_id}. Raw response: {reply[:300]}...")
            logger.error(f"Chat {chat_id}: Full LLM response: {response[:500]}...")
            raise ValueError(f"Invalid JSON response from LLM: {json_err}")
        
        # Validate required fields
        required_fields = [
            'assigned_department', 'chat_summary',
            'action_pending_status', 'action_pending_from', 'chat_status',
            'follow_up_required', 'follow_up_date', 'follow_up_reason', 'next_action_suggestion', 
            'sentiment', 'overall_sentiment', 'chat_started', 'messages'
        ]
        
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            logger.error(f"Chat {chat_id}: Missing required fields: {missing_fields}")
            logger.error(f"Chat {chat_id}: Generated result keys: {list(result.keys())}")
            logger.error(f"Chat {chat_id}: Raw LLM response: {response[:500]}...")
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Validate messages count
        message_count = chat_data.get('chat', {}).get('message_count', 2)
        if len(result['messages']) != message_count:
            logger.warning(f"Chat {chat_id}: Expected {message_count} messages, got {len(result['messages'])}")
            # Adjust to correct count
            if len(result['messages']) > message_count:
                result['messages'] = result['messages'][:message_count]
        
        # Validate sentiment count matches message count
        if len(result['sentiment']) != len(result['messages']):
            logger.warning(f"Chat {chat_id}: Sentiment count mismatch, adjusting...")
            result['sentiment'] = {str(i): result['sentiment'].get(str(i), 1) for i in range(len(result['messages']))}
        
        # Validate message word counts for realism
        for i, message in enumerate(result.get('messages', [])):
            if isinstance(message, dict) and 'content' in message:
                content = message['content']
                word_count = len(content.split())
                if word_count < 10:
                    logger.warning(f"Chat {chat_id}: Message {i} too short ({word_count} words), expanding...")
                    # Add some context to make it more realistic
                    message['content'] = f"{content} Let me provide more details about this matter."
                elif word_count > 150:
                    logger.warning(f"Chat {chat_id}: Message {i} too long ({word_count} words), truncating...")
                    # Truncate to 150 words
                    words = content.split()
                    message['content'] = ' '.join(words[:150])
        
        # Validate follow_up_required matches existing value
        existing_follow_up = chat_data.get('follow_up_required', 'no')
        if result.get('follow_up_required') != existing_follow_up:
            logger.warning(f"Chat {chat_id}: LLM generated follow_up_required='{result.get('follow_up_required')}' but existing value is '{existing_follow_up}'. Correcting...")
            result['follow_up_required'] = existing_follow_up
        
        # Validate action_pending_from values
        valid_action_sources = ['company', 'customer', None, 'null']
        action_pending_from = result.get('action_pending_from')
        if action_pending_from not in valid_action_sources:
            logger.warning(f"Chat {chat_id}: Invalid action_pending_from='{action_pending_from}', correcting to 'company'")
            result['action_pending_from'] = 'company'
        elif action_pending_from == 'null':
            result['action_pending_from'] = None
        
        # Validate date format and range
        try:
            chat_date = datetime.fromisoformat(result['chat_started'].replace('Z', ''))
            start_date = datetime(2025, 1, 1)
            end_date = datetime(2025, 6, 30, 23, 59, 59)
            
            if not (start_date <= chat_date <= end_date):
                if chat_date > end_date:
                    chat_date = end_date
                elif chat_date < start_date:
                    chat_date = start_date
                result['chat_started'] = chat_date.strftime('%Y-%m-%dT%H:%M:%S')
        except:
            # Default date if parsing fails
            result['chat_started'] = '2025-03-15T12:00:00'
        
        generation_time = time.time() - start_time
        
        # Log success
        success_info = {
            'chat_id': chat_id,
            'dominant_topic': chat_data.get('dominant_topic'),
            'urgency': chat_data.get('urgency'),
            'chat_status': result['chat_status'],
            'generation_time': generation_time
        }
        success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
        
        return result
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'chat_id': chat_id,
            'dominant_topic': chat_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
        raise

def populate_chat_messages(chat_record, generated_messages):
    """Populate chat messages with generated content"""
    updates = {}
    
    # Update messages with generated content
    if chat_record.get('messages') and generated_messages:
        for msg_idx, message in enumerate(chat_record['messages']):
            if msg_idx < len(generated_messages):
                generated_msg = generated_messages[msg_idx]
                
                # Update message content
                updates[f'messages.{msg_idx}.body.content'] = generated_msg['content']
                
                # Update timestamp
                if 'timestamp' in generated_msg:
                    updates[f'messages.{msg_idx}.createdDateTime'] = generated_msg['timestamp']
    
    return updates

async def process_single_chat(chat_record, total_chats=None):
    """Process a single chat with all optimizations"""
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
        
        # Debug: Log the generated content structure
        logger.info(f"Chat {chat_id}: Generated content keys: {list(chat_content.keys()) if isinstance(chat_content, dict) else 'Not a dict'}")
        if isinstance(chat_content, dict) and 'chat_summary' in chat_content:
            logger.info(f"Chat {chat_id}: Chat summary field found: {chat_content['chat_summary'][:50]}...")
        else:
            logger.error(f"Chat {chat_id}: Chat summary field missing from generated content")
            logger.error(f"Chat {chat_id}: Full content structure: {chat_content}")
            await performance_monitor.record_failure(total_chats)
            return None
        
        # Debug: Check messages structure
        if 'messages' in chat_content and chat_content['messages']:
            logger.info(f"Chat {chat_id}: Messages count: {len(chat_content['messages'])}")
            for i, msg in enumerate(chat_content['messages']):
                logger.info(f"Chat {chat_id}: Message {i} keys: {list(msg.keys()) if isinstance(msg, dict) else 'Not a dict'}")
                if isinstance(msg, dict) and 'from_user' in msg:
                    logger.info(f"Chat {chat_id}: Message {i} from_user: {msg['from_user']}")
        else:
            logger.error(f"Chat {chat_id}: No messages or empty messages array")
        
        # Handle follow_up fields logic programmatically - RESPECT EXISTING DB VALUES
        existing_follow_up_required = chat_record.get('follow_up_required', 'no')
        
        if existing_follow_up_required == 'no':
            # If DB has follow_up_required='no', keep it as 'no' and set date/reason to null
            follow_up_required = 'no'
            follow_up_date = None
            follow_up_reason = None
            logger.info(f"Chat {chat_id}: DB has follow_up_required='no', keeping as 'no' and setting date/reason=null")
        else:
            # If DB has follow_up_required='yes', use LLM generated values but validate
            llm_follow_up = chat_content.get('follow_up_required', 'no')
            if llm_follow_up != 'yes':
                logger.warning(f"Chat {chat_id}: LLM generated follow_up_required='{llm_follow_up}' but DB has 'yes'. Forcing to 'yes'.")
                follow_up_required = 'yes'
            else:
                follow_up_required = 'yes'
            
            follow_up_date = chat_content.get('follow_up_date')
            follow_up_reason = chat_content.get('follow_up_reason')
            logger.info(f"Chat {chat_id}: DB has follow_up_required='{existing_follow_up_required}', using LLM values: required={follow_up_required}, date={follow_up_date}, reason={follow_up_reason}")
        
        # Prepare update document
        update_doc = {
            "assigned_department": chat_content['assigned_department'],
            "chat_summary": chat_content['chat_summary'],
            "action_pending_status": chat_content['action_pending_status'],
            "action_pending_from": chat_content['action_pending_from'],
            "chat_status": chat_content['chat_status'],
            "follow_up_required": follow_up_required,
            "follow_up_date": follow_up_date,
            "follow_up_reason": follow_up_reason,
            "next_action_suggestion": chat_content['next_action_suggestion'],
            "sentiment": chat_content['sentiment'],
            "overall_sentiment": chat_content['overall_sentiment'],
            "chat_started": chat_content['chat_started'],
            # Add LLM processing tracking
            "llm_processed": True,
            "llm_processed_at": datetime.now().isoformat(),
            "llm_model_used": OPENROUTER_MODEL
        }
        
        # Add message updates
        logger.info(f"Chat {chat_id}: About to populate chat messages...")
        try:
            message_updates = populate_chat_messages(chat_record, chat_content['messages'])
            update_doc.update(message_updates)
            logger.info(f"Chat {chat_id}: Message updates completed successfully")
        except Exception as message_err:
            logger.error(f"Chat {chat_id}: Error in populate_chat_messages: {message_err}")
            raise
        
        # Add thread dates from LLM generated content
        if 'thread_dates' in chat_content:
            thread_dates = chat_content['thread_dates']
            if 'first_message_at' in thread_dates:
                update_doc['chat.createdDateTime'] = thread_dates['first_message_at']
            if 'last_message_at' in thread_dates:
                update_doc['chat.lastUpdatedDateTime'] = thread_dates['last_message_at']
            logger.info(f"Chat {chat_id}: Thread dates set successfully")
        
        # Add message timestamps from LLM generated content
        if chat_content.get('messages'):
            logger.info(f"Chat {chat_id}: Setting timestamps for {len(chat_content['messages'])} messages...")
            for i, message in enumerate(chat_content['messages']):
                if message.get('timestamp'):
                    update_doc[f'messages.{i}.createdDateTime'] = message['timestamp']
            logger.info(f"Chat {chat_id}: Message timestamps set successfully")
        
        logger.info(f"Chat {chat_id}: About to record success...")
        await performance_monitor.record_success(total_chats)
        logger.info(f"Chat {chat_id}: Success recorded, incrementing counter...")
        await success_counter.increment()
        logger.info(f"Chat {chat_id}: Counter incremented, returning result...")
        
        return {
            'chat_id': str(chat_record['_id']),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Chat {chat_id} internal processing failed: {str(e)[:100]}")
        raise  # Re-raise to be caught by the outer timeout handler

async def save_batch_to_database(batch_updates):
    """Save batch updates to database with optimized bulk operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create bulk operations
        bulk_operations = []
        chat_ids = []
        
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"_id": ObjectId(update_data['chat_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            chat_ids.append(update_data['chat_id'])
        
        if bulk_operations:
            try:
                result = chat_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Update counter
                await update_counter.increment()
                
                logger.info(f"Successfully saved {updated_count} records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Fallback to individual updates
                individual_success = 0
                for update_data in batch_updates:
                    try:
                        result = chat_col.update_one(
                            {"_id": ObjectId(update_data['chat_id'])},
                            {"$set": update_data['update_doc']}
                        )
                        if result.modified_count > 0:
                            individual_success += 1
                    except Exception as individual_error:
                        logger.error(f"Individual update failed for {update_data['chat_id']}: {individual_error}")
                
                logger.info(f"Fallback: {individual_success} records saved individually")
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

async def process_chats_optimized():
    """Main optimized processing function"""
    logger.info("Starting Optimized EU Banking Chat Content Generation...")
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
        # Simple and accurate query: chats that are missing ALL LLM fields
        query = {
            "$and": [
                # Must have basic chat structure
                {"_id": {"$exists": True}},
                {"chat": {"$exists": True}},
                # Must be missing ALL core LLM fields
                {
                    "$and": [
                        {"assigned_department": {"$exists": False}},
                        {"chat_summary": {"$exists": False}},
                        {"chat_status": {"$exists": False}},
                        {"overall_sentiment": {"$exists": False}},
                        {"chat_started": {"$exists": False}},
                        {"action_pending_status": {"$exists": False}},
                        {"action_pending_from": {"$exists": False}},
                        {"next_action_suggestion": {"$exists": False}},
                        {"sentiment": {"$exists": False}}
                    ]
                }
            ]
        }
        
        # Exclude already processed chats
        if checkpoint_manager.processed_chats:
            processed_ids = [ObjectId(cid) for cid in checkpoint_manager.processed_chats if ObjectId.is_valid(cid)]
            query["_id"] = {"$nin": processed_ids}
        
        # First, let's check what chats exist and their status
        total_chats_in_db = chat_col.count_documents({})
        chats_processed_by_llm = chat_col.count_documents({"llm_processed": True})
        chats_with_some_llm_fields = chat_col.count_documents({
            "$or": [
                {"assigned_department": {"$exists": True, "$ne": None, "$ne": ""}},
                {"chat_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"chat_status": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        chats_with_all_llm_fields = chat_col.count_documents({
            "$and": [
                {"assigned_department": {"$exists": True, "$ne": None, "$ne": ""}},
                {"chat_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"chat_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"overall_sentiment": {"$exists": True, "$ne": None, "$ne": ""}},
                {"chat_started": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_from": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"sentiment": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        
        # Calculate actual chats needing processing using the same query
        chats_needing_processing = chat_col.count_documents(query)
        
        logger.info(f"Database Status:")
        logger.info(f"  Total chats in DB: {total_chats_in_db}")
        logger.info(f"  Chats processed by LLM (llm_processed=True): {chats_processed_by_llm}")
        logger.info(f"  Chats with some LLM fields: {chats_with_some_llm_fields}")
        logger.info(f"  Chats with ALL LLM fields: {chats_with_all_llm_fields}")
        logger.info(f"  Chats needing processing: {chats_needing_processing}")
        
        chat_records = list(chat_col.find(query))
        total_chats = len(chat_records)
        
        if total_chats == 0:
            logger.info("No chats found that need processing!")
            logger.info("All chats appear to have been processed by LLM already.")
            return
        
        logger.info(f"Found {total_chats} chats that need LLM processing")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_chats)} chats")
        progress_logger.info(f"BATCH_START: total_chats={total_chats}")
        
    except Exception as e:
        logger.error(f"Error fetching chat records: {e}")
        return
    
    # Process chats in optimized batches
    total_updated = 0
    batch_updates = []
    
    try:
        # Process chats in concurrent batches
        for i in range(0, total_chats, BATCH_SIZE):
            if shutdown_flag.is_set():
                logger.info("Shutdown requested, stopping processing")
                break
            
            batch = chat_records[i:i + BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (total_chats + BATCH_SIZE - 1)//BATCH_SIZE
            logger.info(f"Processing batch {batch_num}/{total_batches} (chats {i+1}-{min(i+BATCH_SIZE, total_chats)})")
            
            # Process batch concurrently
            batch_tasks = []
            for chat in batch:
                if not checkpoint_manager.is_processed(chat['_id']):
                    task = process_single_chat(chat, total_chats)
                    batch_tasks.append(task)
            
            logger.info(f"Created {len(batch_tasks)} tasks for batch {batch_num}")
            
            if batch_tasks:
                # Process tasks individually but WAIT for ALL to complete before moving to next batch
                logger.info(f"Processing {len(batch_tasks)} tasks individually in batch {batch_num}")
                logger.info(f"Will wait for ALL tasks to complete before moving to next batch...")
                
                batch_start_time = time.time()
                successful_results = []
                failed_count = 0
                
                # Process each task individually with reasonable timeout
                for task_idx, task in enumerate(batch_tasks, 1):
                    logger.info(f"Starting task {task_idx}/{len(batch_tasks)} in batch {batch_num}")
                    start_time = time.time()
                    try:
                        # Add reasonable timeout to prevent infinite hanging
                        logger.info(f"Waiting for task {task_idx} to complete (max {REQUEST_TIMEOUT * 3}s)...")
                        result = await asyncio.wait_for(task, timeout=REQUEST_TIMEOUT * 3)  # 6 minutes per task
                        elapsed = time.time() - start_time
                        logger.info(f"Task {task_idx} finished, processing result...")
                        if result:
                            successful_results.append(result)
                            try:
                                await asyncio.wait_for(
                                    checkpoint_manager.mark_processed(result['chat_id'], success=True),
                                    timeout=10.0  # 10 second timeout for checkpoint
                                )
                                logger.info(f"Task {task_idx}/{len(batch_tasks)} completed successfully in {elapsed:.1f}s")
                            except asyncio.TimeoutError:
                                logger.warning(f"Checkpoint save timed out for task {task_idx}, but task completed successfully")
                                logger.info(f"Task {task_idx}/{len(batch_tasks)} completed successfully in {elapsed:.1f}s")
                        else:
                            failed_count += 1
                            logger.warning(f"Task {task_idx}/{len(batch_tasks)} returned no result after {elapsed:.1f}s")
                    except asyncio.TimeoutError:
                        elapsed = time.time() - start_time
                        logger.error(f"Task {task_idx}/{len(batch_tasks)} timed out after {elapsed:.1f}s, continuing to next task...")
                        failed_count += 1
                    except Exception as e:
                        elapsed = time.time() - start_time
                        logger.error(f"Task {task_idx}/{len(batch_tasks)} failed after {elapsed:.1f}s with error: {e}")
                        failed_count += 1
                    
                    logger.info(f"Finished processing task {task_idx}/{len(batch_tasks)}, moving to next...")
                
                if successful_results:
                    batch_updates.extend(successful_results)
                
                batch_elapsed = time.time() - batch_start_time
                logger.info(f"Batch {batch_num} FULLY completed in {batch_elapsed:.1f}s: {len(successful_results)}/{len(batch_tasks)} successful, {failed_count} failed")
            
            # Save to database when we have enough updates
            if len(batch_updates) >= BATCH_SIZE:
                saved_count = await save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear batch
            
            # Progress update
            processed_so_far = min(i + BATCH_SIZE, total_chats)
            progress_pct = (processed_so_far / total_chats) * 100
            logger.info(f"Overall Progress: {progress_pct:.1f}% ({processed_so_far}/{total_chats})")
            
            # Update performance monitor with actual total
            await performance_monitor.log_progress(total_chats)
            
            # Brief delay between batches to manage rate limits
            if i + BATCH_SIZE < total_chats and not shutdown_flag.is_set():
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
        
        logger.info(f"Final Results:")
        logger.info(f"  Total chats updated: {total_updated}")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_chat = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per chat: {avg_time_per_chat:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} chats/hour" if total_time > 0 else "Processing rate: N/A")
        
        progress_logger.info(f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}, total_time={total_time/3600:.2f}h, rate={success_counter.value/(total_time/3600):.0f}/h" if total_time > 0 else f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}")
        
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
    """Get collection statistics"""
    try:
        total_count = chat_col.count_documents({})
        
        with_complete_fields = chat_col.count_documents({
            "assigned_department": {"$exists": True, "$ne": "", "$ne": None},
            "chat_summary": {"$exists": True, "$ne": "", "$ne": None},
            "chat_status": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "chat_started": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_status": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_from": {"$exists": True, "$ne": "", "$ne": None},
            "follow_up_required": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        urgent_chats = chat_col.count_documents({"urgency": True})
        without_complete_fields = total_count - with_complete_fields
        
        logger.info("Collection Statistics:")
        logger.info(f"  Total chats: {total_count}")
        logger.info(f"  With complete fields: {with_complete_fields}")
        logger.info(f"  Without complete fields: {without_complete_fields}")
        logger.info(f"  Urgent chats: {urgent_chats} ({(urgent_chats/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent chats: 0")
        logger.info(f"  Completion rate: {(with_complete_fields/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

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