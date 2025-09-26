# EU Banking Voice Transcript Content Generator - Enhanced Version
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
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

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

# Performance Monitor
class PerformanceMonitor:
    def __init__(self):
        self.start_time = time.time()
        self.calls_processed = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self._lock = asyncio.Lock()
    
    async def record_success(self, total_calls=None):
        async with self._lock:
            self.successful_requests += 1
            self.calls_processed += 1
            await self.log_progress(total_calls)
    
    async def record_failure(self, total_calls=None):
        async with self._lock:
            self.failed_requests += 1
            await self.log_progress(total_calls)
    
    async def log_progress(self, total_calls=None):
        if self.calls_processed % 100 == 0 and self.calls_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.calls_processed / elapsed if elapsed > 0 else 0
            remaining_calls = (total_calls - self.calls_processed) if total_calls else 0
            eta = remaining_calls / rate if rate > 0 and remaining_calls > 0 else 0
            
            logger.info(f"Performance Stats:")
            if total_calls:
                logger.info(f"  Processed: {self.calls_processed}/{total_calls} voice calls")
            else:
                logger.info(f"  Processed: {self.calls_processed} voice calls")
            logger.info(f"  Rate: {rate:.2f} calls/second ({rate*3600:.0f} calls/hour)")
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
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def get_participant_names_and_roles(voice_data):
    """Get participant names and their roles from voice record - FIXED LOGIC"""
    # Default values
    customer_name = 'Customer'
    agent_name = 'Bank Agent'
    
    # Get participant names and roles from voice record
    thread_info = voice_data.get('thread', {})
    if isinstance(thread_info, dict):
        members = thread_info.get('members', [])
        if isinstance(members, list):
            for member in members:
                if isinstance(member, dict):
                    display_name = member.get('displayName', '')
                    member_id = member.get('id', '')
                    
                    # FIXED LOGIC: If display name is "Bank Agent", this person is the agent
                    if display_name == 'Bank Agent' or member_id.endswith('@bank.com'):
                        agent_name = display_name or 'Bank Agent'
                    else:
                        # If display name is a common person name (not "Bank Agent"), this is the customer
                        customer_name = display_name or 'Customer'
    
    return {
        'customer': customer_name, 
        'agent': agent_name,
        # For conversation logic: determine who starts based on call flow (customer always calls first)
        'customer_starts': True  # Banking calls always start with customer calling
    }

def generate_optimized_voice_prompt(voice_data):
    """Generate highly optimized prompt for voice transcript content - FIXED CONVERSATION FLOW"""
    dominant_topic = voice_data.get('dominant_topic', 'General Banking Inquiry')
    subtopics = voice_data.get('subtopics', 'Account information')
    message_count = voice_data.get('thread', {}).get('message_count', 2)
    existing_urgency = voice_data.get('urgency', False)
    existing_follow_up = voice_data.get('follow_up_required', 'no')
    
    # Get participant names and roles - FIXED
    participant_info = get_participant_names_and_roles(voice_data)
    customer_name = participant_info['customer']
    agent_name = participant_info['agent']
    
    urgency_context = "URGENT" if existing_urgency else "NON-URGENT"
    
    # Generate realistic banking details
    account_number = f"{random.randint(100000000000, 999999999999)}"
    call_reference = f"CALL{random.randint(10000, 99999)}"
    
    prompt = f"""Generate EU banking voice call transcript JSON. CRITICAL: Return ONLY valid JSON, no other text.

CONTEXT:
Topic: {dominant_topic} | Subtopic: {subtopics}
Messages: {message_count} | Urgency: {urgency_context} ({existing_urgency})
Follow-up Required: {existing_follow_up} (MUST PRESERVE THIS VALUE)
Customer: {customer_name} | Agent: {agent_name}
Account: {account_number} | Call Reference: {call_reference}

CRITICAL CONVERSATION RULES - SPEAKER ASSIGNMENT:
- Customer ({customer_name}): ALWAYS speaks first (calls the bank), uses emotional/natural language
- Agent ({agent_name}): Responds professionally, uses bank terminology, NEVER says "I am a bank agent" or "I am {agent_name}"
- Alternate speakers: Customer → Agent → Customer → Agent (strict pattern)
- Agent responses should be helpful without mentioning their role/title: "I'm here to help", "Let me check that", "I'll guide you through this"

CONVERSATION FLOW (MANDATORY SEQUENCE):
1. Customer calls and states their problem briefly (emotional tone appropriate to {urgency_context})
2. Agent greets professionally: "Thank you for calling EU Bank, I'm here to help. Can I get your full name for verification?"
3. Agent asks for specific details based on the problem and {dominant_topic}
4. Customer provides requested information
5. Agent investigates/processes, asks clarifying questions if needed
6. Agent provides resolution or next steps
7. Professional call closure: "Thank you for your patience", "Have a great day"

VOICE CONVERSATION REQUIREMENTS:
- Natural spoken language with "um", "uh", contractions, pauses
- Agent uses helpful phrases: "I understand", "Let me check that", "I'll help you resolve this"
- Customer uses emotional tone appropriate to {urgency_context}
- NO repetition - each message progresses the conversation
- End properly when resolved - don't continue unnecessarily
- Messages should be 30-100 words (natural voice call length)
- Agent NEVER mentions being "bank agent" or their title - just be helpful and professional

OUTPUT FORMAT REQUIRED - EXACTLY THIS STRUCTURE:
{{
  "call_summary": "Professional call summary 100-150 words describing customer issue, verification process, resolution/action taken",
  "action_pending_status": "yes|no",
  "action_pending_from": "company|customer|null (null if action_pending_status=no)",
  "resolution_status": "open|inprogress|closed",
  "follow_up_required": "yes|no (MUST match existing value: {existing_follow_up})",
  "follow_up_date": "2025-MM-DDTHH:MM:SS or null (provide date if follow_up_required=yes, can be null if no)",
  "follow_up_reason": "specific reason WHY follow-up is needed or null (if follow_up_required=yes, provide contextual reason like 'To confirm transaction reversal completed', 'To verify card replacement received', null if no)",
  "next_action_suggestion": "Next step recommendation 50-80 words",
  "messages": [
    {{
      "content": "Customer voice message - initial problem description. Use natural spoken language: 'Hi, um, I'm calling because...'. Brief but clear about the issue. 30-100 words. Express appropriate emotion for {urgency_context}. DO NOT include 'Customer:' or 'Customer (name):' prefix - just the actual spoken content.",
      "sender_type": "customer",
      "headers": {{
        "date": "2025-MM-DD HH:MM:SS (Generate date between 2025-01-01 and 2025-06-30, use business hours 08:00-18:00 for routine, any time for urgent)"
      }}
    }}{"," if message_count > 1 else ""}
    {"{"}"content": "Agent response - Professional greeting and name request. DO NOT say 'I am a bank agent' or mention your title. Use: 'Thank you for calling EU Bank, I'm here to help you today. Can I get your full name please for verification?'. 40-80 words. DO NOT include 'Agent:' or 'Bank Agent:' prefix - just the actual spoken content.",
    "sender_type": "company",
    "headers": {{
      "date": "2025-MM-DD HH:MM:SS (Few seconds after customer call)"
    }}
    {"}"}{"" if message_count <= 2 else "... continue alternating pattern for " + str(message_count) + " total messages following conversation flow. Agent NEVER mentions being 'bank agent' - just be helpful: 'I'll help you with this', 'Let me guide you', 'I understand your concern'. DO NOT include speaker prefixes like 'Agent:' or 'Customer:' - just the actual spoken content"}
  ],
  "sentiment": {{"0": sentiment_score_message_1, "1": sentiment_score_message_2{"..." if message_count > 2 else ""}}} (Individual message sentiment analysis using voice emotional tone 0-5 scale:
- 0: Happy/Satisfied (pleased with service)
- 1: Calm/Professional (baseline conversation tone)  
- 2: Bit Concerned (slight worry or questioning)
- 3: Moderately Concerned (worried about issue)
- 4: Stressed/Frustrated (clear stress in voice)
- 5: Very Stressed/Urgent (high stress, very emotional)
CRITICAL: Generate sentiment for exactly {message_count} messages),
  "overall_sentiment": 0.0-5.0 (overall call sentiment based on issue resolution and customer satisfaction),
  "call_started": "2025-01-01T08:00:00 to 2025-06-30T18:00:00 (business hours for routine, after-hours for urgent)",
  "thread_dates": {{
    "first_message_at": "2025-MM-DD HH:MM:SS (earliest timestamp from messages)",
    "last_message_at": "2025-MM-DD HH:MM:SS (latest timestamp from messages)"
  }}
}}

VALIDATION REQUIREMENTS:
✓ Generate exactly {message_count} messages alternating customer/company (start with customer: {customer_name})
✓ Follow MANDATORY conversation sequence: problem → name verification → details → resolution → closure
✓ Agent MUST ask for customer's full name early in conversation
✓ Each message progresses conversation forward - NO repetition
✓ Use natural voice call language with spoken patterns, contractions
✓ Customer messages: Natural speech with emotional tone, include hesitations "um", "uh"
✓ Agent messages: Professional but friendly. NEVER say "I am a bank agent" or mention title/role
✓ Agent helpful phrases: "I'm here to help", "Let me check that", "I'll guide you", "I understand", "We'll resolve this"
✓ Agent closing: Professional conclusion like "Thank you for your patience", "Is there anything else I can help you with?", "Thank you for choosing EU Bank"
✓ CRITICAL: Message content must NOT include speaker prefixes like "Agent:", "Customer:", "Customer (Name):", "Bank Agent:" - only the actual spoken words
✓ Match urgency={existing_urgency} in conversation tone
✓ Sentiment matches message count: {message_count} entries exactly
✓ Follow-up fields: CRITICAL - follow_up_required MUST match existing value "{existing_follow_up}"
✓ Date generation: All dates between 2025-01-01 and 2025-06-30, chronological order
✓ Include realistic banking references: account {account_number}, call reference {call_reference}
✓ End conversation properly when issue is resolved - don't continue unnecessarily
✓ Use "thread_dates" (plural) with "first_message_at" and "last_message_at"

CRITICAL SPEAKER RULES:
- Message 1: ALWAYS customer ({customer_name}) - states problem
- Message 2: ALWAYS agent - professional greeting, asks for name, NO mention of being "bank agent"  
- Messages 3+: Alternate customer/agent - agent stays helpful without role mentions
- CONTENT RULE: Never include speaker prefixes in message content - only the actual spoken words

Return ONLY the JSON object above with realistic voice call values.
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
                'X-Title': 'EU Banking Voice Generator'
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

async def generate_voice_transcript_content(voice_data):
    """Generate voice transcript content with optimized processing"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    call_id = str(voice_data.get('_id', 'unknown'))
    
    try:
        prompt = generate_optimized_voice_prompt(voice_data)
        
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
            logger.info(f"Call {call_id}: JSON parsing successful. Keys: {list(result.keys())}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing failed for call {call_id}. Raw response: {reply[:300]}...")
            raise ValueError(f"Invalid JSON response from LLM: {json_err}")
        
        # Validate required fields
        required_fields = [
            'call_summary', 'action_pending_status', 'action_pending_from', 'resolution_status',
            'follow_up_required', 'follow_up_date', 'follow_up_reason', 'next_action_suggestion',
            'sentiment', 'overall_sentiment', 'call_started', 'messages'
        ]
        
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            logger.error(f"Call {call_id}: Missing required fields: {missing_fields}")
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Validate messages count
        message_count = voice_data.get('thread', {}).get('message_count', 2)
        if len(result['messages']) != message_count:
            logger.warning(f"Call {call_id}: Expected {message_count} messages, got {len(result['messages'])}")
            # Adjust to correct count
            if len(result['messages']) > message_count:
                result['messages'] = result['messages'][:message_count]
        
        # Validate sentiment count matches message count
        if len(result['sentiment']) != len(result['messages']):
            logger.warning(f"Call {call_id}: Sentiment count mismatch, adjusting...")
            result['sentiment'] = {str(i): result['sentiment'].get(str(i), 1) for i in range(len(result['messages']))}
        
        # Validate follow_up_required matches existing value
        existing_follow_up = voice_data.get('follow_up_required', 'no')
        if result.get('follow_up_required') != existing_follow_up:
            logger.warning(f"Call {call_id}: LLM generated follow_up_required='{result.get('follow_up_required')}' but existing value is '{existing_follow_up}'. Correcting...")
            result['follow_up_required'] = existing_follow_up
        
        # Validate action_pending_from values
        valid_action_sources = ['company', 'customer', None, 'null']
        action_pending_from = result.get('action_pending_from')
        if action_pending_from not in valid_action_sources:
            logger.warning(f"Call {call_id}: Invalid action_pending_from='{action_pending_from}', correcting to 'company'")
            result['action_pending_from'] = 'company'
        elif action_pending_from == 'null':
            result['action_pending_from'] = None
        
        generation_time = time.time() - start_time
        
        # Log success
        success_info = {
            'call_id': call_id,
            'dominant_topic': voice_data.get('dominant_topic'),
            'urgency': voice_data.get('urgency'),
            'resolution_status': result['resolution_status'],
            'generation_time': generation_time
        }
        success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
        
        return result
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'call_id': call_id,
            'dominant_topic': voice_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
        raise

def clean_speaker_prefixes(content):
    """Remove speaker prefixes from message content before saving to database"""
    if not content or not isinstance(content, str):
        return content
    
    original_content = content
    # Remove common speaker prefix patterns
    import re
    
    # Pattern 1: "Agent: " or "Agent:" at the start
    content = re.sub(r'^Agent:\s*', '', content, flags=re.IGNORECASE)
    
    # Pattern 2: "Customer (Name): " or "Customer (Name):" at the start
    content = re.sub(r'^Customer\s*\([^)]+\):\s*', '', content, flags=re.IGNORECASE)
    
    # Pattern 3: "Customer: " or "Customer:" at the start
    content = re.sub(r'^Customer:\s*', '', content, flags=re.IGNORECASE)
    
    # Pattern 4: "Bank Agent: " or "Bank Agent:" at the start
    content = re.sub(r'^Bank\s+Agent:\s*', '', content, flags=re.IGNORECASE)
    
    # Pattern 5: Any other "Name: " pattern at the start (fallback)
    content = re.sub(r'^[A-Za-z\s]+\([^)]+\):\s*', '', content)
    
    # Clean up any extra whitespace
    content = content.strip()
    
    # Log if content was modified
    if content != original_content:
        logger.debug(f"Cleaned speaker prefix: '{original_content[:50]}...' -> '{content[:50]}...'")
    
    return content

def populate_voice_messages(voice_record, generated_messages):
    """Populate voice messages with generated content"""
    updates = {}
    
    # Update messages with generated content
    if voice_record.get('messages') and generated_messages:
        for msg_idx, message in enumerate(voice_record['messages']):
            if msg_idx < len(generated_messages):
                generated_msg = generated_messages[msg_idx]
                
                # Clean speaker prefixes from content before saving
                cleaned_content = clean_speaker_prefixes(generated_msg['content'])
                
                # Update message content with cleaned version
                updates[f'messages.{msg_idx}.body.content'] = cleaned_content
                
                # Update timestamp
                if 'headers' in generated_msg and 'date' in generated_msg['headers']:
                    updates[f'messages.{msg_idx}.createdDateTime'] = generated_msg['headers']['date']
    
    return updates

async def process_single_voice_call(voice_record, total_calls=None):
    """Process a single voice call with all optimizations"""
    if shutdown_flag.is_set():
        return None
    
    call_id = str(voice_record.get('_id', 'unknown'))
    
    try:
        return await _process_single_voice_call_internal(voice_record, total_calls)
    except Exception as e:
        logger.error(f"Call {call_id} processing failed with error: {str(e)[:100]}")
        await performance_monitor.record_failure(total_calls)
        await failure_counter.increment()
        await checkpoint_manager.mark_processed(call_id, success=False)
        return None

async def _process_single_voice_call_internal(voice_record, total_calls=None):
    """Internal voice call processing logic"""
    call_id = str(voice_record.get('_id', 'unknown'))
    
    try:
        # Generate content
        voice_content = await generate_voice_transcript_content(voice_record)
        
        if not voice_content:
            await performance_monitor.record_failure(total_calls)
            return None
        
        # Debug: Log the generated content structure
        logger.info(f"Call {call_id}: Generated content keys: {list(voice_content.keys()) if isinstance(voice_content, dict) else 'Not a dict'}")
        if isinstance(voice_content, dict) and 'call_summary' in voice_content:
            logger.info(f"Call {call_id}: Call summary field found: {voice_content['call_summary'][:50]}...")
        else:
            logger.error(f"Call {call_id}: Call summary field missing from generated content")
            logger.error(f"Call {call_id}: Full content structure: {voice_content}")
            await performance_monitor.record_failure(total_calls)
            return None
        
        # Debug: Check messages structure
        if 'messages' in voice_content and voice_content['messages']:
            logger.info(f"Call {call_id}: Messages count: {len(voice_content['messages'])}")
            for i, msg in enumerate(voice_content['messages']):
                logger.info(f"Call {call_id}: Message {i} keys: {list(msg.keys()) if isinstance(msg, dict) else 'Not a dict'}")
                if isinstance(msg, dict) and 'sender_type' in msg:
                    logger.info(f"Call {call_id}: Message {i} sender_type: {msg['sender_type']}")
        else:
            logger.error(f"Call {call_id}: No messages or empty messages array")
        
        # Handle follow_up fields logic programmatically - RESPECT EXISTING DB VALUES
        existing_follow_up_required = voice_record.get('follow_up_required', 'no')
        
        if existing_follow_up_required == 'no':
            # If DB has follow_up_required='no', keep it as 'no' and set date/reason to null
            follow_up_required = 'no'
            follow_up_date = None
            follow_up_reason = None
            logger.info(f"Call {call_id}: DB has follow_up_required='no', keeping as 'no' and setting date/reason=null")
        else:
            # If DB has follow_up_required='yes', use LLM generated values but validate
            llm_follow_up = voice_content.get('follow_up_required', 'no')
            if llm_follow_up != 'yes':
                logger.warning(f"Call {call_id}: LLM generated follow_up_required='{llm_follow_up}' but DB has 'yes'. Forcing to 'yes'.")
                follow_up_required = 'yes'
            else:
                follow_up_required = 'yes'
            
            follow_up_date = voice_content.get('follow_up_date')
            follow_up_reason = voice_content.get('follow_up_reason')
            logger.info(f"Call {call_id}: DB has follow_up_required='{existing_follow_up_required}', using LLM values: required={follow_up_required}, date={follow_up_date}, reason={follow_up_reason}")
        
        # Prepare update document
        update_doc = {
            "call_summary": voice_content['call_summary'],
            "action_pending_status": voice_content['action_pending_status'],
            "action_pending_from": voice_content['action_pending_from'],
            "resolution_status": voice_content['resolution_status'],
            "follow_up_required": follow_up_required,
            "follow_up_date": follow_up_date,
            "follow_up_reason": follow_up_reason,
            "next_action_suggestion": voice_content['next_action_suggestion'],
            "sentiment": voice_content['sentiment'],
            "overall_sentiment": voice_content['overall_sentiment'],
            "call_started": voice_content['call_started'],
            # Add LLM processing tracking
            "llm_processed": True,
            "llm_processed_at": datetime.now().isoformat(),
            "llm_model_used": OPENROUTER_MODEL
        }
        
        # Add message updates
        logger.info(f"Call {call_id}: About to populate voice messages...")
        try:
            message_updates = populate_voice_messages(voice_record, voice_content['messages'])
            update_doc.update(message_updates)
            logger.info(f"Call {call_id}: Message updates completed successfully")
        except Exception as message_err:
            logger.error(f"Call {call_id}: Error in populate_voice_messages: {message_err}")
            raise
        
        # Add thread dates from LLM generated content
        if 'thread_dates' in voice_content:
            thread_dates = voice_content['thread_dates']
            if 'first_message_at' in thread_dates:
                update_doc['thread.createdDateTime'] = thread_dates['first_message_at']
            if 'last_message_at' in thread_dates:
                update_doc['thread.lastUpdatedDateTime'] = thread_dates['last_message_at']
            logger.info(f"Call {call_id}: Thread dates set successfully")
        
        # Add message timestamps from LLM generated content
        if voice_content.get('messages'):
            logger.info(f"Call {call_id}: Setting timestamps for {len(voice_content['messages'])} messages...")
            for i, message in enumerate(voice_content['messages']):
                if message.get('headers', {}).get('date'):
                    update_doc[f'messages.{i}.createdDateTime'] = message['headers']['date']
            logger.info(f"Call {call_id}: Message timestamps set successfully")
        
        logger.info(f"Call {call_id}: About to record success...")
        await performance_monitor.record_success(total_calls)
        logger.info(f"Call {call_id}: Success recorded, incrementing counter...")
        await success_counter.increment()
        logger.info(f"Call {call_id}: Counter incremented, returning result...")
        
        return {
            'call_id': str(voice_record['_id']),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Call {call_id} internal processing failed: {str(e)[:100]}")
        raise  # Re-raise to be caught by the outer timeout handler

async def save_batch_to_database(batch_updates):
    """Save batch updates to database with optimized bulk operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} voice updates to database...")
        
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
    """Main optimized processing function"""
    logger.info("Starting Optimized EU Banking Voice Transcript Content Generation...")
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
    
    # Get voice calls to process - only those that have NEVER been processed by LLM
    try:
        # Simple and accurate query: voice calls that are missing ALL LLM fields
        query = {
            "$and": [
                # Must have basic voice call structure
                {"_id": {"$exists": True}},
                {"thread": {"$exists": True}},
                # Must be missing ALL core LLM fields
                {
                    "$and": [
                        {"call_summary": {"$exists": False}},
                        {"resolution_status": {"$exists": False}},
                        {"overall_sentiment": {"$exists": False}},
                        {"call_started": {"$exists": False}},
                        {"action_pending_status": {"$exists": False}},
                        {"action_pending_from": {"$exists": False}},
                        {"next_action_suggestion": {"$exists": False}},
                        {"sentiment": {"$exists": False}}
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
        calls_with_some_llm_fields = voice_col.count_documents({
            "$or": [
                {"call_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"overall_sentiment": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        calls_with_all_llm_fields = voice_col.count_documents({
            "$and": [
                {"call_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"overall_sentiment": {"$exists": True, "$ne": None, "$ne": ""}},
                {"call_started": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_from": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"sentiment": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        
        # Calculate actual calls needing processing using the same query
        calls_needing_processing = voice_col.count_documents(query)
        
        logger.info(f"Database Status:")
        logger.info(f"  Total voice calls in DB: {total_calls_in_db}")
        logger.info(f"  Calls processed by LLM (llm_processed=True): {calls_processed_by_llm}")
        logger.info(f"  Calls with some LLM fields: {calls_with_some_llm_fields}")
        logger.info(f"  Calls with ALL LLM fields: {calls_with_all_llm_fields}")
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
                    task = process_single_voice_call(voice_call, total_calls)
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
                                    checkpoint_manager.mark_processed(result['call_id'], success=True),
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
            processed_so_far = min(i + BATCH_SIZE, total_calls)
            progress_pct = (processed_so_far / total_calls) * 100
            logger.info(f"Overall Progress: {progress_pct:.1f}% ({processed_so_far}/{total_calls})")
            
            # Update performance monitor with actual total
            await performance_monitor.log_progress(total_calls)
            
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
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_call = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per call: {avg_time_per_call:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} calls/hour" if total_time > 0 else "Processing rate: N/A")
        
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
            'X-Title': 'EU Banking Voice Generator'
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
        total_count = voice_col.count_documents({})
        
        with_complete_fields = voice_col.count_documents({
            "call_summary": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "call_started": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_status": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_from": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        urgent_calls = voice_col.count_documents({"urgency": True})
        without_complete_fields = total_count - with_complete_fields
        
        logger.info("Voice Collection Statistics:")
        logger.info(f"  Total voice calls: {total_count}")
        logger.info(f"  With complete fields: {with_complete_fields}")
        logger.info(f"  Without complete fields: {without_complete_fields}")
        logger.info(f"  Urgent calls: {urgent_calls} ({(urgent_calls/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent calls: 0")
        logger.info(f"  Completion rate: {(with_complete_fields/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

async def main():
    """Main async function"""
    logger.info("Optimized EU Banking Voice Transcript Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{VOICE_COLLECTION}")
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

# Run the optimized voice transcript generator
if __name__ == "__main__":
    asyncio.run(main())