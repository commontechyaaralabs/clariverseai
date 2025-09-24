# EU Banking Voice Transcript Content Generator - Enhanced Version with Strict Message Count
import os
import random
import time
import json
import asyncio
import aiohttp
import signal
import sys
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
VOICE_COLLECTION = "voice_transcripts"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"voice_transcript_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_voice_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_voice_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"voice_progress_{timestamp}.log"
CHECKPOINT_FILE = LOG_DIR / f"voice_checkpoint_{timestamp}.json"

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
OLLAMA_MODEL = "gemma3:27b"  # Force gemma3:27b instead of reading from environment
BATCH_SIZE = 3  # Very small batch size to reduce load
MAX_CONCURRENT = 1  # Single concurrent call to avoid rate limits
REQUEST_TIMEOUT = 120  # Keep timeout for detailed generation
MAX_RETRIES = 5  # More retries for rate limit recovery
RETRY_DELAY = 5  # Longer retry delay
BATCH_DELAY = 5.0  # Much longer batch delay
API_CALL_DELAY = 2.0  # Much longer API delay between calls
CHECKPOINT_SAVE_INTERVAL = 10  # Very frequent checkpoints

# Ollama setup (chat-style, like chat_ollama.py)
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "bb5beda97d9aee7559d30061452b9fcf402b93818eb0d23d815292f5c479ae93")
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://34.147.17.26:29020/api/chat")

# Global variables for graceful shutdown
shutdown_flag = asyncio.Event()
client = None
db = None
voice_col = None

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
retry_counter = AtomicCounter("RETRY_COUNT")
message_count_mismatch_counter = AtomicCounter("MESSAGE_COUNT_MISMATCH_COUNT")

# Performance Monitor
class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.calls_processed = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self._lock = asyncio.Lock()
    
    async def record_success(self):
        async with self._lock:
            self.successful_requests += 1
            self.calls_processed += 1
            await self.log_progress()
    
    async def record_failure(self):
        async with self._lock:
            self.failed_requests += 1
            await self.log_progress()
    
    async def log_progress(self):
        if self.calls_processed % 50 == 0 and self.calls_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.calls_processed / elapsed if elapsed > 0 else 0
            
            logger.info(f"Performance Stats:")
            logger.info(f"  Processed: {self.calls_processed} voice calls")
            logger.info(f"  Rate: {rate:.2f} calls/second ({rate*3600:.0f} calls/hour)")
            logger.info(f"  Success rate: {self.successful_requests/(self.successful_requests + self.failed_requests)*100:.1f}%")

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
        self.processed_calls = set()
        self.failed_calls = set()
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
                    self.processed_calls = set(data.get('processed_calls', []))
                    self.failed_calls = set(data.get('failed_calls', []))
                    self.stats.update(data.get('stats', {}))
                logger.info(f"Loaded checkpoint: {len(self.processed_calls)} processed, {len(self.failed_calls)} failed")
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    
    async def save_checkpoint(self):
        async with self._lock:
            try:
                checkpoint_data = {
                    'processed_calls': list(self.processed_calls),
                    'failed_calls': list(self.failed_calls),
                    'stats': self.stats,
                    'timestamp': datetime.now().isoformat()
                }
                with open(self.checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
            except Exception as e:
                logger.error(f"Could not save checkpoint: {e}")
    
    def is_processed(self, call_id):
        return str(call_id) in self.processed_calls
    
    async def mark_processed(self, call_id, success=True):
        async with self._lock:
            call_id_str = str(call_id)
            self.processed_calls.add(call_id_str)
            self.stats['processed_count'] += 1
            
            if success:
                self.stats['success_count'] += 1
                self.failed_calls.discard(call_id_str)
            else:
                self.stats['failure_count'] += 1
                self.failed_calls.add(call_id_str)
            
            # Auto-save every CHECKPOINT_SAVE_INTERVAL
            if self.stats['processed_count'] % CHECKPOINT_SAVE_INTERVAL == 0:
                await self.save_checkpoint()

checkpoint_manager = CheckpointManager(CHECKPOINT_FILE)

def setup_signal_handlers():
    """Setup signal handlers for graceful shutdown"""
    def signal_handler(signum, frame):
        logger.info(f"Received signal {signum}. Initiating graceful shutdown...")
        # shutdown_flag is an asyncio.Event; set() is synchronous
        try:
            shutdown_flag.set()
        except Exception as e:
            logger.warning(f"Failed to set shutdown flag: {e}")
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
    global client, db, voice_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        voice_col = db[VOICE_COLLECTION]
        
        # Create indexes for better performance
        voice_col.create_index("_id")
        voice_col.create_index("dominant_topic")
        voice_col.create_index("urgency")
        voice_col.create_index("call_id")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def generate_realistic_banking_details(voice_record=None):
    """Generate realistic banking details for use in voice transcripts"""
    details = {
        'call_id': f"CALL{random.randint(10000, 99999)}",
        'account_number': f"{random.randint(100000000000, 999999999999)}",
        'customer_id': f"CID{random.randint(100000, 999999)}",
        'system_name': random.choice(['CoreBanking', 'PaymentHub', 'OnlineBanking', 'MobileApp']),
        'amount': f"{random.randint(100, 50000)}.{random.randint(10, 99)}",
        'currency': random.choice(['EUR', 'GBP', 'USD']),
        'error_code': f"ERR_{random.randint(1000, 9999)}",
        'reference_number': f"REF{random.randint(100000, 999999)}",
        'date': fake.date_between(start_date='-7d', end_date='today').strftime('%d/%m/%Y'),
        'time': f"{random.randint(8, 18):02d}:{random.randint(0, 59):02d}"
    }
    
    # Get customer name from voice record if available
    if voice_record and voice_record.get('thread', {}).get('members'):
        for member in voice_record['thread']['members']:
            if member.get('id') != 'agent@bank.com' and member.get('displayName'):
                details['customer_name'] = member['displayName']
                break
    
    if 'customer_name' not in details:
        customer_names = ["John Smith", "Sarah Johnson", "Michael Brown", "Emma Wilson", "David Jones", "Mary Gillespie"]
        details['customer_name'] = random.choice(customer_names)
    
    return details

def derive_roles_from_messages(voice_record):
    """Derive speaker roles from message structure"""
    roles = []
    messages = voice_record.get('messages', [])
    if not messages:
        return roles
    
    # Get agent email from thread members
    agent_emails = set()
    thread_info = voice_record.get('thread', {})
    if isinstance(thread_info, dict):
        members = thread_info.get('members', [])
        if isinstance(members, list):
            for member in members:
                if isinstance(member, dict) and member.get('id', '').endswith('@bank.com'):
                    agent_emails.add(member.get('id', ''))
    
    for msg in messages:
        if not isinstance(msg, dict):
            continue
            
        from_info = msg.get('from', {})
        if not isinstance(from_info, dict):
            continue
            
        user_info = from_info.get('user', {})
        if not isinstance(user_info, dict):
            continue
            
        sender_id = user_info.get('id', '')
        if sender_id in agent_emails or sender_id.endswith('@bank.com'):
            roles.append('company')
        else:
            roles.append('customer')
    return roles

def get_participant_names(voice_record):
    """Get participant names from voice record"""
    customer_name = 'Customer'
    agent_name = 'Bank Agent'
    
    thread_info = voice_record.get('thread', {})
    if not isinstance(thread_info, dict):
        return {'customer': customer_name, 'agent': agent_name}
    
    members = thread_info.get('members', [])
    if not isinstance(members, list):
        return {'customer': customer_name, 'agent': agent_name}
    
    for member in members:
        if not isinstance(member, dict):
            continue
            
        member_id = member.get('id', '')
        display_name = member.get('displayName', '')
        
        if member_id.endswith('@bank.com'):
            agent_name = display_name or agent_name
        else:
            customer_name = display_name or customer_name
    
    return {'customer': customer_name, 'agent': agent_name}

def generate_optimized_voice_prompt(voice_data, retry_attempt=0):
    """Generate extremely strict prompt for voice transcripts with MANDATORY message count enforcement"""
    dominant_topic = voice_data.get('dominant_topic', 'General Banking Inquiry')
    subtopics = voice_data.get('subtopics', 'Account information')
    message_count = voice_data.get('thread', {}).get('message_count', 30)
    existing_urgency = voice_data.get('urgency', False)
    existing_follow_up = voice_data.get('follow_up_required', 'no')
    call_id = voice_data.get('call_id', 'CALL12345')
    
    banking_details = generate_realistic_banking_details(voice_data)
    roles = derive_roles_from_messages(voice_data)
    names = get_participant_names(voice_data)
    
    urgency_context = "URGENT" if existing_urgency else "NON-URGENT"
    retry_warning = f" [RETRY ATTEMPT {retry_attempt}/10 - PREVIOUS ATTEMPTS FAILED MESSAGE COUNT VALIDATION]" if retry_attempt > 0 else ""
    
    # Ensure we have enough roles for the message count
    if len(roles) < message_count:
        # Alternate customer/company pattern to fill remaining slots
        pattern = ['customer', 'company']
        while len(roles) < message_count:
            roles.append(pattern[len(roles) % 2])
    
    # Truncate if we have too many roles
    roles = roles[:message_count]
    
    # Build explicit message templates for each index - FIXED INDEXING
    message_templates = []
    for i in range(message_count):
        sender_type = roles[i]
        message_number = i + 1  # Human-readable numbering
        
        if sender_type == 'customer':
            template = f'''    {{
      "content": "Customer message {message_number} about {dominant_topic}. Include account {banking_details['account_number']} and natural speech patterns. 10-100 words.",
      "sender_type": "customer",
      "headers": {{
        "date": "2025-MM-DD HH:MM:SS"
      }}
    }}'''
        else:
            template = f'''    {{
      "content": "Voice agent response {message_number} in call-center spoken style: concise confirmations, read-back of details, brief hold/transition phrases. Professional tone. Include call reference {call_id}. 10-100 words.",
      "sender_type": "company", 
      "headers": {{
        "date": "2025-MM-DD HH:MM:SS"
      }}
    }}'''
        message_templates.append(template)
    
    # Join templates with commas
    messages_section = ",\n".join(message_templates)
    
    # Build sentiment indices explicitly - FIXED TO USE CORRECT RANGE
    sentiment_indices = [f'"{i}": "sentiment_score_between_0_and_5_for_message_{i+1}"' for i in range(message_count)]
    sentiment_section = ", ".join(sentiment_indices)
    
    prompt = f"""CRITICAL TASK{retry_warning}: Generate EU banking voice call transcript JSON. Return ONLY valid JSON.

CONTEXT: {dominant_topic} | Customer: {banking_details['customer_name']} | Account: {banking_details['account_number']} | Urgency: {urgency_context}

⚠️⚠️⚠️ ABSOLUTE MANDATORY REQUIREMENTS - ZERO TOLERANCE FOR DEVIATION ⚠️⚠️⚠️
1. GENERATE EXACTLY {message_count} MESSAGES IN THE "messages" ARRAY - NO MORE, NO LESS
2. GENERATE EXACTLY {message_count} SENTIMENT ENTRIES WITH KEYS "0" TO "{message_count-1}"
3. FOLLOW EXACT ROLES PATTERN: {roles}
4. EACH MESSAGE MUST BE 10-100 WORDS
5. OUTPUT MUST BE VALID JSON WITH NO EXTRA TEXT, MARKDOWN, OR CODE BLOCKS

🔥 COUNTING VERIFICATION - DO THIS BEFORE RESPONDING:
- Count messages array entries: MUST = {message_count} (currently required: {message_count})
- Count sentiment object keys: MUST = {message_count} (currently required: {message_count})
- Verify sender_type matches roles: {roles}
- Verify JSON syntax is perfect

❌ FAILURE CONDITIONS THAT WILL CAUSE REJECTION:
- messages.length ≠ {message_count}
- sentiment object key count ≠ {message_count}
- Any markdown formatting (```, ```json, etc.)
- Any explanatory text outside JSON
- Invalid JSON syntax
- Missing required fields

✅ SUCCESS CHECKLIST (VERIFY ALL BEFORE RESPONDING):
□ messages array has exactly {message_count} entries
□ sentiment object has exactly {message_count} keys: "0", "1", "2", ..., "{message_count-1}"
□ sender_type pattern matches: {roles}
□ All messages 10-100 words
□ Valid JSON structure
□ No extra formatting or text

REQUIRED JSON STRUCTURE (GENERATE EXACTLY THIS):
{{
  "call_summary": "Brief call summary describing the main issue and outcome in 100 words",
  "action_pending_status": "yes|no",
  "action_pending_from": "company|customer|null (null if action_pending_status=no)",
  "resolution_status": "open|inprogress|closed",
  "next_action_suggestion": "Next step recommendation 50-80 words",
  "messages": [
{messages_section}
  ],
  "sentiment": {{{sentiment_section}}},
  "overall_sentiment": 0.0 to 5.0 based on the sentiment of the messages,
  "thread_dates": {{
    "first_message_at": "2025-03-15 09:30:00",
    "last_message_at": "2025-03-15 09:35:00"
  }}
}}

SENTIMENT SCALE (0-5 ONLY):
0=Happy, 1=Calm, 2=Bit Irritated, 3=Moderately Concerned, 4=Anger, 5=Frustrated

🚨 FINAL WARNING: If you generate {message_count-1} or {message_count+1} or any number other than EXACTLY {message_count} messages, this will be REJECTED and you will be asked to retry. COUNT CAREFULLY!

Generate the JSON response with EXACTLY {message_count} messages and EXACTLY {message_count} sentiment entries:"""
    
    return prompt

# Rate Limited Processor
class RateLimitedProcessor:
    def __init__(self, max_concurrent=MAX_CONCURRENT):
        self.semaphore = Semaphore(max_concurrent)
        self.last_request_time = 0
        self._lock = asyncio.Lock()
    
    async def call_ollama_async(self, session, prompt, max_retries=MAX_RETRIES):
        """Async Ollama API call (chat-style) with rate limiting and retries"""
        async with self.semaphore:
            # Rate limiting - ensure minimum delay between requests
            async with self._lock:
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                if time_since_last < API_CALL_DELAY:
                    await asyncio.sleep(API_CALL_DELAY - time_since_last)
                self.last_request_time = time.time()
            
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
                    "temperature": 0.5,
                    "num_predict": 100000
                }
            }
            
            for attempt in range(max_retries):
                try:
                    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
                    async with session.post(OLLAMA_URL, json=payload, headers=headers, timeout=timeout) as response:
                        if response.status == 429:
                            wait_time = min(30, 5 * (2 ** attempt))
                            logger.warning(f"Rate limited, waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                            await asyncio.sleep(wait_time)
                            continue
                        response.raise_for_status()
                        result = await response.json()
                        if "message" not in result or "content" not in result["message"]:
                            logger.error(f"No 'message.content' field. Fields: {list(result.keys())}")
                            raise KeyError("No 'message.content' field in Ollama response")
                        return result["message"]["content"]
                except asyncio.TimeoutError:
                    logger.warning(f"Request timeout on attempt {attempt+1}/{max_retries}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(RETRY_DELAY)
                        continue
                    raise
                
                except aiohttp.ClientResponseError as e:
                    if e.status == 429:
                        wait_time = min(30, 5 * (2 ** attempt))
                        logger.warning(f"Rate limited, waiting {wait_time}s before retry {attempt+1}/{max_retries}")
                        await asyncio.sleep(wait_time)
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


async def generate_voice_transcript_content(voice_data, max_retries=10):
    """Generate voice transcript content with strict retry mechanism for message count validation"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    call_id = str(voice_data.get('_id', 'unknown'))
    requested_message_count = voice_data.get('thread', {}).get('message_count')
    
    if not isinstance(requested_message_count, int) or requested_message_count <= 0:
        logger.error(f"Call {call_id}: Invalid message_count: {requested_message_count}")
        raise ValueError(f"Invalid message_count: {requested_message_count}")
    
    # Retry loop for message count validation
    for retry_attempt in range(max_retries):
        try:
            # Generate prompt with retry attempt information
            prompt = generate_optimized_voice_prompt(voice_data, retry_attempt)
            
            # Create session for this batch
            connector = aiohttp.TCPConnector(limit=10, force_close=True, enable_cleanup_closed=True)
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT + 10)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                response = await circuit_breaker.call(
                    processor.call_ollama_async,
                    session,
                    prompt
                )
            
            if not response or not response.strip():
                logger.warning(f"Call {call_id}: Empty response from LLM on attempt {retry_attempt + 1}/{max_retries}")
                if retry_attempt < max_retries - 1:
                    continue
                raise ValueError("Empty response from LLM after all retries")
            
            # Clean and parse JSON response
            reply = response.strip()
            
            # Remove markdown formatting if present
            if "```" in reply:
                reply = reply.replace("```json", "").replace("```", "")
            
            # Extract JSON
            json_start = reply.find('{')
            json_end = reply.rfind('}') + 1
            
            if json_start == -1 or json_end <= json_start:
                logger.warning(f"Call {call_id}: No valid JSON found in LLM response on attempt {retry_attempt + 1}/{max_retries}")
                if retry_attempt < max_retries - 1:
                    continue
                raise ValueError("No valid JSON found in LLM response after all retries")
            
            reply = reply[json_start:json_end]
            
            try:
                result = json.loads(reply)
                logger.info(f"Call {call_id}: JSON parsing successful on attempt {retry_attempt + 1}. Keys: {list(result.keys())}")
            except json.JSONDecodeError as json_err:
                logger.warning(f"Call {call_id}: JSON parsing failed on attempt {retry_attempt + 1}/{max_retries}. Error: {json_err}")
                if retry_attempt < max_retries - 1:
                    continue
                logger.error(f"Call {call_id}: JSON parsing failed on all attempts. Raw response: {reply[:300]}...")
                raise ValueError(f"Invalid JSON response from LLM after all retries: {json_err}")
            
            # Validate required fields
            required_fields = [
                'call_summary', 'messages', 'sentiment', 'overall_sentiment', 
                'thread_dates'
            ]
            
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                logger.warning(f"Call {call_id}: Missing required fields on attempt {retry_attempt + 1}/{max_retries}: {missing_fields}")
                if retry_attempt < max_retries - 1:
                    await retry_counter.increment()
                    continue
                logger.error(f"Call {call_id}: Missing required fields after all attempts: {missing_fields}")
                raise ValueError(f"Missing required fields after all retries: {missing_fields}")
            
            # CRITICAL VALIDATION: Check messages count vs requested - THIS IS THE PRIMARY RETRY TRIGGER
            actual_count = len(result.get('messages', [])) if isinstance(result.get('messages'), list) else 0
            
            if actual_count != requested_message_count:
                await message_count_mismatch_counter.increment()
                logger.warning(f"Call {call_id}: MESSAGE COUNT MISMATCH on attempt {retry_attempt + 1}/{max_retries}: generated {actual_count}, requested {requested_message_count}")
                if retry_attempt < max_retries - 1:
                    # This is the key condition that triggers retries
                    await retry_counter.increment()
                    await asyncio.sleep(1.0)  # Brief pause between retries
                    continue
                logger.error(f"Call {call_id}: MESSAGE COUNT MISMATCH after all {max_retries} attempts: generated {actual_count}, requested {requested_message_count}. GIVING UP.")
                raise ValueError(f"Message count mismatch after {max_retries} retries: generated {actual_count}, requested {requested_message_count}")
            
            # CRITICAL VALIDATION: Check sentiment count matches message count
            sentiment_data = result.get('sentiment', {})
            if isinstance(sentiment_data, dict):
                sentiment_count = len(sentiment_data)
                if sentiment_count != actual_count:
                    logger.warning(f"Call {call_id}: SENTIMENT COUNT MISMATCH on attempt {retry_attempt + 1}/{max_retries}: {sentiment_count} sentiments for {actual_count} messages")
                    if retry_attempt < max_retries - 1:
                        await retry_counter.increment()
                        continue
                    logger.error(f"Call {call_id}: SENTIMENT COUNT MISMATCH after all attempts: {sentiment_count} sentiments for {actual_count} messages")
                    raise ValueError(f"Sentiment count mismatch after {max_retries} retries: {sentiment_count} sentiments for {actual_count} messages")
            
            # SUCCESS! All validations passed
            logger.info(f"Call {call_id}: ✅ SUCCESS on attempt {retry_attempt + 1}/{max_retries} - Message count: {actual_count}, Sentiment count: {len(sentiment_data)}")
            
            # If sentiment is an array, convert to indexed dict for storage
            if 'sentiment' in result:
                sent = result['sentiment']
                if isinstance(sent, list):
                    result['sentiment'] = {str(i): sent[i] for i in range(len(sent))}
            
            # Validate follow_up_required matches existing value
            existing_follow_up = voice_data.get('follow_up_required', 'no')
            if result.get('follow_up_required') != existing_follow_up:
                logger.warning(f"Call {call_id}: LLM generated follow_up_required='{result.get('follow_up_required')}' but existing value is '{existing_follow_up}'. Correcting...")
                result['follow_up_required'] = existing_follow_up
            
            generation_time = time.time() - start_time
            
            # Log success with enhanced metrics including retry info
            success_info = {
                'call_id': call_id,
                'dominant_topic': voice_data.get('dominant_topic'),
                'call_summary': result['call_summary'][:100] + "..." if len(result['call_summary']) > 100 else result['call_summary'],
                'urgency': voice_data.get('urgency'),
                'overall_sentiment': result['overall_sentiment'],
                'requested_message_count': requested_message_count,
                'actual_message_count': actual_count,
                'sentiment_count': len(result.get('sentiment', {})),
                'retry_attempts': retry_attempt + 1,
                'generation_time': generation_time
            }
            success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
            
            return result
        
        except Exception as e:
            if "count mismatch" in str(e).lower() and retry_attempt < max_retries - 1:
                # This is a count mismatch, continue to next retry
                logger.warning(f"Call {call_id}: Retry {retry_attempt + 1}/{max_retries} failed due to count mismatch: {e}")
                await asyncio.sleep(1.0)  # Brief pause between retries
                continue
            elif retry_attempt < max_retries - 1:
                # Other error, but still have retries left
                logger.warning(f"Call {call_id}: Attempt {retry_attempt + 1}/{max_retries} failed: {e}")
                await asyncio.sleep(1.0)
                continue
            else:
                # Last attempt or non-recoverable error
                generation_time = time.time() - start_time
                error_info = {
                    'call_id': call_id,
                    'dominant_topic': voice_data.get('dominant_topic', 'Unknown'),
                    'error': str(e)[:200],
                    'retry_attempts': retry_attempt + 1,
                    'generation_time': generation_time
                }
                failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
                raise
    
    # This should never be reached, but just in case
    raise ValueError(f"Call {call_id}: Exhausted all {max_retries} retry attempts without success")

def normalize_message_contents(messages_json, target_count):
    """Deprecated: No longer normalizing to target_count; kept for backward compatibility."""
    contents = []
    for m in messages_json:
        if isinstance(m, dict) and 'content' in m:
            contents.append(str(m.get('content', '')).strip())
        elif isinstance(m, str):
            contents.append(m.strip())
        else:
            contents.append(str(m))
    return contents

def build_update_from_voice_result(voice_record, generated):
    """Build update document from generated voice transcript result"""
    msgs = generated.get('messages', []) if isinstance(generated.get('messages'), list) else []
    
    update = {}
    
    # Update message contents only for messages actually generated (no padding/truncation)
    existing_messages_len = len(voice_record.get('messages', [])) if isinstance(voice_record.get('messages'), list) else 0
    for i, msg in enumerate(msgs):
        if existing_messages_len == 0 or i < existing_messages_len:
            content_value = ""
            if isinstance(msg, dict):
                content_value = str(msg.get('content', '')).strip()
            elif isinstance(msg, str):
                content_value = msg.strip()
            else:
                content_value = str(msg)
            update[f'messages.{i}.body.content'] = content_value
    
    # Map call_summary
    if 'call_summary' in generated:
        update['call_summary'] = str(generated.get('call_summary', '')).strip()
    
    # Handle follow_up fields logic programmatically - RESPECT EXISTING DB VALUES
    existing_follow_up_required = voice_record.get('follow_up_required', 'no')
    
    if existing_follow_up_required == 'no':
        # If DB has follow_up_required='no', keep it as 'no' and set date/reason to null
        follow_up_required = 'no'
        follow_up_date = None
        follow_up_reason = None
        logger.info(f"Call {voice_record.get('_id', 'unknown')}: DB has follow_up_required='no', keeping as 'no' and setting date/reason=null")
    else:
        # If DB has follow_up_required='yes', use LLM generated values but validate
        llm_follow_up = generated.get('follow_up_required', 'no')
        if llm_follow_up != 'yes':
            logger.warning(f"Call {voice_record.get('_id', 'unknown')}: LLM generated follow_up_required='{llm_follow_up}' but DB has 'yes'. Forcing to 'yes'.")
            follow_up_required = 'yes'
        else:
            follow_up_required = 'yes'
        
        follow_up_date = generated.get('follow_up_date')
        follow_up_reason = generated.get('follow_up_reason')
        logger.info(f"Call {voice_record.get('_id', 'unknown')}: DB has follow_up_required='{existing_follow_up_required}', using LLM values: required={follow_up_required}, date={follow_up_date}, reason={follow_up_reason}")
    
    # Add follow-up fields to update
    update['follow_up_required'] = follow_up_required
    update['follow_up_date'] = follow_up_date
    update['follow_up_reason'] = follow_up_reason
    
    # Add action/status fields similar to ticket logic if present
    if isinstance(generated, dict):
        if 'action_pending_status' in generated:
            update['action_pending_status'] = generated.get('action_pending_status')
        if 'action_pending_from' in generated:
            update['action_pending_from'] = generated.get('action_pending_from')
        if 'resolution_status' in generated:
            update['resolution_status'] = generated.get('resolution_status')
        if 'next_action_suggestion' in generated:
            update['next_action_suggestion'] = generated.get('next_action_suggestion')
    
    # Add thread dates from LLM generated content
    thread_dates = generated.get('thread_dates') or {}
    if isinstance(thread_dates, dict):
        if 'first_message_at' in thread_dates:
            update['thread.first_message_at'] = thread_dates['first_message_at']
        if 'last_message_at' in thread_dates:
            update['thread.last_message_at'] = thread_dates['last_message_at']
    
    # Add message dates from LLM generated content for all generated messages
    if isinstance(generated.get('messages'), list):
        for i, message in enumerate(generated['messages']):
            if isinstance(message, dict) and message.get('headers', {}).get('date'):
                update[f'messages.{i}.createdDateTime'] = message['headers']['date']
    
    # Always update lastUpdated
    update['thread.lastUpdatedDateTime'] = datetime.utcnow().isoformat() + 'Z'
    
    # Add LLM processing tracking
    update['llm_processed'] = True
    update['llm_processed_at'] = datetime.now().isoformat()
    update['llm_model_used'] = OLLAMA_MODEL
    
    return update

async def process_single_voice_call(voice_record):
    """Process a single voice call with all optimizations"""
    if shutdown_flag.is_set():
        return None
    
    call_id = str(voice_record.get('_id', 'unknown'))
    
    try:
        return await _process_single_voice_call_internal(voice_record)
    except Exception as e:
        logger.error(f"Call {call_id} processing failed with error: {str(e)[:100]}")
        await performance_monitor.record_failure()
        await failure_counter.increment()
        await checkpoint_manager.mark_processed(call_id, success=False)
        return None

async def _process_single_voice_call_internal(voice_record):
    """Internal voice call processing logic"""
    call_id = str(voice_record.get('_id', 'unknown'))
    
    try:
        # Generate content
        voice_content = await generate_voice_transcript_content(voice_record)
        
        if not voice_content:
            await performance_monitor.record_failure()
            return None
        
        # Debug: Log the generated content structure
        logger.info(f"Call {call_id}: Generated content keys: {list(voice_content.keys()) if isinstance(voice_content, dict) else 'Not a dict'}")
        if isinstance(voice_content, dict) and 'call_summary' in voice_content:
            logger.info(f"Call {call_id}: Call summary found: {voice_content['call_summary'][:50]}...")
        else:
            logger.error(f"Call {call_id}: Call summary field missing from generated content")
            await performance_monitor.record_failure()
            return None
        
        # Debug: Check messages structure
        if 'messages' in voice_content and voice_content['messages']:
            logger.info(f"Call {call_id}: Messages count: {len(voice_content['messages'])}")
            for i, msg in enumerate(voice_content['messages']):
                if isinstance(msg, dict) and 'sender_type' in msg:
                    logger.info(f"Call {call_id}: Message {i} sender_type: {msg['sender_type']}")
        else:
            logger.error(f"Call {call_id}: No messages or empty messages array")
        
        # Prepare update document
        update_doc = build_update_from_voice_result(voice_record, voice_content)
        
        # Add additional voice-specific fields
        if 'overall_sentiment' in voice_content:
            update_doc['overall_sentiment'] = voice_content['overall_sentiment']
        
        if 'sentiment' in voice_content:
            update_doc['sentiment'] = voice_content['sentiment']
        
        logger.info(f"Call {call_id}: About to record success...")
        await performance_monitor.record_success()
        logger.info(f"Call {call_id}: Success recorded, incrementing counter...")
        await success_counter.increment()
        logger.info(f"Call {call_id}: Counter incremented, returning result...")
        
        return {
            'call_id': str(voice_record['_id']),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Call {call_id} internal processing failed: {str(e)[:100]}")
        raise

async def save_batch_to_database(batch_updates):
    """Save batch updates to database with optimized bulk operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} voice transcript updates to database...")
        
        # Create bulk operations
        bulk_operations = []
        call_ids = []
        
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"_id": ObjectId(update_data['call_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            call_ids.append(update_data['call_id'])
        
        if bulk_operations:
            try:
                result = voice_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Update counter
                await update_counter.increment()
                
                logger.info(f"Successfully saved {updated_count} voice records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} voice records saved")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Fallback to individual updates
                individual_success = 0
                for update_data in batch_updates:
                    try:
                        result = voice_col.update_one(
                            {"_id": ObjectId(update_data['call_id'])},
                            {"$set": update_data['update_doc']}
                        )
                        if result.modified_count > 0:
                            individual_success += 1
                    except Exception as individual_error:
                        logger.error(f"Individual update failed for {update_data['call_id']}: {individual_error}")
                
                logger.info(f"Fallback: {individual_success} voice records saved individually")
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

async def process_voice_calls_optimized():
    """Main optimized processing function for voice transcripts"""
    logger.info("Starting Optimized EU Banking Voice Transcript Content Generation...")
    logger.info(f"Optimized Configuration:")
    logger.info(f"  Max Concurrent: {MAX_CONCURRENT}")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info(f"  API Delay: {API_CALL_DELAY}s")
    logger.info(f"  Request Timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"  Model: {OLLAMA_MODEL}")
    
    # Test connection
    if not await test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        return
    
    # Get voice calls to process - only those that have NEVER been processed by LLM
    try:
        # Query for voice calls that need LLM processing
        # Look for calls that are missing the main LLM-generated content
        query = {
            "$and": [
                # Must have basic voice call structure
                {"_id": {"$exists": True}},
                {"thread": {"$exists": True}},
                {"messages": {"$exists": True}},
                # Must be missing the main LLM-generated content
                {
                    "$or": [
                        {"call_summary": {"$exists": False}},
                        {"call_summary": {"$eq": None}},
                        {"call_summary": {"$eq": ""}},
                        {"messages.0.body.content": {"$eq": None}},
                        {"messages.0.body.content": {"$eq": ""}}
                    ]
                }
            ]
        }
        
        # Exclude already processed calls
        if checkpoint_manager.processed_calls:
            processed_ids = [ObjectId(cid) for cid in checkpoint_manager.processed_calls if ObjectId.is_valid(cid)]
            query["_id"] = {"$nin": processed_ids}
        
        # First, let's check what calls exist and their status
        total_calls_in_db = voice_col.count_documents({})
        calls_processed_by_llm = voice_col.count_documents({"llm_processed": True})
        calls_with_call_summary = voice_col.count_documents({
            "call_summary": {"$exists": True, "$ne": None, "$ne": ""}
        })
        calls_with_message_content = voice_col.count_documents({
            "messages.0.body.content": {"$ne": None, "$ne": ""}
        })
        calls_with_complete_content = voice_col.count_documents({
            "$and": [
                {"call_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"messages.0.body.content": {"$ne": None, "$ne": ""}}
            ]
        })
        
        # Calculate actual calls needing processing using the same query
        calls_needing_processing = voice_col.count_documents(query)
        
        logger.info(f"Database Status:")
        logger.info(f"  Total voice calls in DB: {total_calls_in_db}")
        logger.info(f"  Calls processed by LLM (llm_processed=True): {calls_processed_by_llm}")
        logger.info(f"  Calls with call_summary: {calls_with_call_summary}")
        logger.info(f"  Calls with message content: {calls_with_message_content}")
        logger.info(f"  Calls with complete content: {calls_with_complete_content}")
        logger.info(f"  Calls needing processing: {calls_needing_processing}")
        
        voice_records = list(voice_col.find(query))
        total_calls = len(voice_records)
        
        if total_calls == 0:
            logger.info("No voice calls found that need processing!")
            logger.info("All voice calls appear to have been processed by LLM already.")
            return
        
        logger.info(f"Found {total_calls} voice calls that need LLM processing")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_calls)} calls")
        progress_logger.info(f"BATCH_START: total_calls={total_calls}")
        
    except Exception as e:
        logger.error(f"Error fetching voice call records: {e}")
        return
    
    # Process calls in optimized batches
    total_updated = 0
    batch_updates = []
    
    try:
        # Process calls in concurrent batches
        for i in range(0, total_calls, BATCH_SIZE):
            if shutdown_flag.is_set():
                logger.info("Shutdown requested, stopping processing")
                break
            
            batch = voice_records[i:i + BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (total_calls + BATCH_SIZE - 1)//BATCH_SIZE
            logger.info(f"Processing batch {batch_num}/{total_batches} (calls {i+1}-{min(i+BATCH_SIZE, total_calls)})")
            
            # Process batch concurrently
            batch_tasks = []
            for voice_call in batch:
                if not checkpoint_manager.is_processed(voice_call['_id']):
                    task = process_single_voice_call(voice_call)
                    batch_tasks.append(task)
            
            logger.info(f"Created {len(batch_tasks)} tasks for batch {batch_num}")
            
            if batch_tasks:
                batch_start_time = time.time()
                successful_results = []
                failed_count = 0
                
                # Process each task individually with reasonable timeout
                for i, task in enumerate(batch_tasks, 1):
                    logger.info(f"Starting task {i}/{len(batch_tasks)} in batch {batch_num}")
                    start_time = time.time()
                    try:
                        # Add reasonable timeout to prevent infinite hanging
                        logger.info(f"Waiting for task {i} to complete (max {REQUEST_TIMEOUT * 5}s)...")
                        result = await asyncio.wait_for(task, timeout=REQUEST_TIMEOUT * 5)
                        elapsed = time.time() - start_time
                        logger.info(f"Task {i} finished, processing result...")
                        if result:
                            successful_results.append(result)
                            try:
                                await asyncio.wait_for(
                                    checkpoint_manager.mark_processed(result['call_id'], success=True),
                                    timeout=10.0
                                )
                                logger.info(f"Task {i}/{len(batch_tasks)} completed successfully in {elapsed:.1f}s")
                            except asyncio.TimeoutError:
                                logger.warning(f"Checkpoint save timed out for task {i}, but task completed successfully")
                                logger.info(f"Task {i}/{len(batch_tasks)} completed successfully in {elapsed:.1f}s")
                        else:
                            failed_count += 1
                            logger.warning(f"Task {i}/{len(batch_tasks)} returned no result after {elapsed:.1f}s")
                    except asyncio.TimeoutError:
                        elapsed = time.time() - start_time
                        logger.error(f"Task {i}/{len(batch_tasks)} timed out after {elapsed:.1f}s, continuing to next task...")
                        failed_count += 1
                    except Exception as e:
                        elapsed = time.time() - start_time
                        logger.error(f"Task {i}/{len(batch_tasks)} failed after {elapsed:.1f}s with error: {e}")
                        failed_count += 1
                    
                    logger.info(f"Finished processing task {i}/{len(batch_tasks)}, moving to next...")
                
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
            processed_so_far = min(i + BATCH_SIZE, total_calls)
            progress_pct = (processed_so_far / total_calls) * 100
            logger.info(f"Overall Progress: {progress_pct:.1f}% ({processed_so_far}/{total_calls})")
            
            # Brief delay between batches to manage rate limits
            if i + BATCH_SIZE < total_calls and not shutdown_flag.is_set():
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
            logger.info("Optimized voice transcript content generation complete!")
        
        logger.info(f"Final Results:")
        logger.info(f"  Total voice calls updated: {total_updated}")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        logger.info(f"  Total retry attempts: {retry_counter.value}")
        logger.info(f"  Message count mismatches detected: {message_count_mismatch_counter.value}")
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        logger.info(f"  Average retries per call: {(retry_counter.value/success_counter.value):.2f}" if success_counter.value > 0 else "Average retries per call: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_call = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per call: {avg_time_per_call:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} calls/hour" if total_time > 0 else "Processing rate: N/A")
        
        progress_logger.info(f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}, retries={retry_counter.value}, message_mismatches={message_count_mismatch_counter.value}, total_time={total_time/3600:.2f}h, rate={success_counter.value/(total_time/3600):.0f}/h" if total_time > 0 else f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}, retries={retry_counter.value}, message_mismatches={message_count_mismatch_counter.value}")
        
    except Exception as e:
        logger.error(f"Unexpected error in main processing: {e}")
        logger.error(traceback.format_exc())
    finally:
        await checkpoint_manager.save_checkpoint()

async def test_ollama_connection():
    """Test Ollama connection with async client"""
    try:
        logger.info("Testing Ollama connection...")

        headers = {
            'Authorization': f'Bearer {OLLAMA_API_KEY}',
            'Content-Type': 'application/json'
        }

        test_payload = {
            "model": OLLAMA_MODEL,
            "messages": [{"role": "user", "content": "Generate a JSON object with 'test': 'success'"}],
            "stream": False,
            "options": {"num_predict": 20}
        }

        connector = aiohttp.TCPConnector(force_close=True, enable_cleanup_closed=True)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            # Direct POST to chat-style endpoint
            async with session.post(OLLAMA_URL, json=test_payload, headers=headers) as response:
                response.raise_for_status()
                result = await response.json()
                if "message" in result and "content" in result["message"]:
                    logger.info("Ollama connection test successful")
                    logger.info(f"Test response: {result['message']['content'][:100]}...")
                    return True
                else:
                    logger.error(f"No 'message.content' field in test. Fields: {list(result.keys())}")
                    return False
        
    except Exception as e:
        logger.error(f"Ollama connection test failed: {e}")
        return False

def get_collection_stats():
    """Get collection statistics"""
    try:
        total_count = voice_col.count_documents({})
        
        with_complete_fields = voice_col.count_documents({
            "call_summary": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "messages.0.body.content": {"$ne": "", "$ne": None}
        })
        
        with_some_llm_fields = voice_col.count_documents({
            "$or": [
                {"call_summary": {"$exists": True, "$ne": "", "$ne": None}},
                {"overall_sentiment": {"$exists": True, "$ne": "", "$ne": None}},
                {"sentiment": {"$exists": True, "$ne": "", "$ne": None}},
                {"messages.0.body.content": {"$ne": "", "$ne": None}}
            ]
        })
        
        urgent_calls = voice_col.count_documents({"urgency": True})
        without_complete_fields = total_count - with_complete_fields
        
        logger.info("Voice Collection Statistics:")
        logger.info(f"  Total voice calls: {total_count}")
        logger.info(f"  With complete LLM fields: {with_complete_fields}")
        logger.info(f"  With some LLM fields: {with_some_llm_fields}")
        logger.info(f"  Without complete LLM fields: {without_complete_fields}")
        logger.info(f"  Urgent calls: {urgent_calls} ({(urgent_calls/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent calls: 0")
        logger.info(f"  Completion rate: {(with_complete_fields/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

async def main():
    """Main async function"""
    logger.info("Enhanced EU Banking Voice Transcript Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{VOICE_COLLECTION}")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_URL}")
    logger.info(f"Configuration: {MAX_CONCURRENT} concurrent, {BATCH_SIZE} batch size")
    logger.info("ENHANCEMENT: Strict message count validation enabled")
    
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
        await process_voice_calls_optimized()
        
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

# Run the enhanced voice transcript generator
if __name__ == "__main__":
    asyncio.run(main())