# EU Banking Email Thread Generation and Analysis System - OpenRouter Migration
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
EMAIL_COLLECTION = "email"

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
        if self.emails_processed % 100 == 0 and self.emails_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.emails_processed / elapsed if elapsed > 0 else 0
            remaining_emails = (total_emails - self.emails_processed) if total_emails else 0
            eta = remaining_emails / rate if rate > 0 and remaining_emails > 0 else 0
            
            logger.info(f"Performance Stats:")
            if total_emails:
                logger.info(f"  Processed: {self.emails_processed}/{total_emails} emails")
            else:
                logger.info(f"  Processed: {self.emails_processed} emails")
            logger.info(f"  Rate: {rate:.2f} emails/second ({rate*3600:.0f} emails/hour)")
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

def generate_optimized_email_prompt(email_data):
    """Generate optimized prompt for email content and analysis generation"""
    
    # Extract data from email record
    dominant_topic = email_data.get('dominant_topic', 'General Banking')
    subtopics = email_data.get('subtopics', 'General operations')
    participants = email_data.get('thread', {}).get('participants', [])
    message_count = email_data.get('thread', {}).get('message_count', 2)
    urgency = email_data.get('urgency', False)
    stages = email_data.get('stages', 'Receive')
    follow_up_required = email_data.get('follow_up_required', 'no')
    
    # Extract participant details for reference
    sender = next((p for p in participants if p['type'] == 'from'), participants[0] if participants else {'name': 'Customer', 'email': 'customer@example.com'})
    recipient = next((p for p in participants if p['type'] == 'to'), participants[1] if len(participants) > 1 else {'name': 'Support Team', 'email': 'support@eubank.com'})
    
    prompt = f"""
TASK: Generate a realistic EU banking email thread with {message_count} messages focusing ONLY on email content (subject, body, dates).

**CONTEXT:**
- Dominant Topic: {dominant_topic}
- Subtopics: {subtopics}
- Participants: {sender['name']} ({sender['email']}) ↔ {recipient['name']} ({recipient['email']})
- Messages: {message_count}
- Industry: EU Banking Sector
- Urgency: {urgency} (MUST PRESERVE THIS VALUE)
- Current Stage: {stages} (MUST PRESERVE THIS VALUE)
- Follow-up Required: {follow_up_required} (MUST PRESERVE THIS VALUE)

**EMAIL GENERATION REQUIREMENTS:**

1. **AUTHENTIC EU Banking Context:**
   - Generate completely authentic emails relevant to European banking operations
   - Use real European business communication style and terminology
   - Use European date formats (DD/MM/YYYY) and authentic business terminology
   - Include specific banking scenarios, regulations, and compliance requirements

2. **Authentic Email Content Generation:**
   - Create {message_count} completely authentic email messages with real banking scenarios
   - Message 1: Initial email from {sender['name']} with specific banking context
   - Subsequent messages: Natural conversation flow alternating between participants
   - Professional banking tone appropriate to EU standards with real business language
   - Each message: 200-300 words (vary naturally) with specific banking details
   - Include proper email greetings and professional closings
   - Incorporate authentic banking-specific terminology, scenarios, and business processes
   - **CRITICAL: Do NOT include the subject line in the email body content**
   - **CRITICAL: Generate authentic content - NO placeholder text, mock data, or generic examples**

3. **Authentic Subject and Dating:**
   - Generate realistic banking-related subject lines with specific business context
   - Reply messages use "Re: [original subject]" format
   - Generate realistic timestamps showing natural progression (minutes to days apart)
   - **CRITICAL: All dates must be between 2025-01-01 and 2025-06-30 (6 months only)**
   - Ensure meaningful timeframes between messages (minutes for quick replies, hours for business hours, days for complex issues)

**OUTPUT FORMAT:**
Return ONLY a JSON object with this structure:

{{
  "thread_data": {{
    "subject_norm": "[original_subject_line]",
    "first_message_at": "[ISO_timestamp_first_message]",
    "last_message_at": "[ISO_timestamp_last_message]"
  }},
  "messages": [
    {{
      "headers": {{
        "date": "[ISO_timestamp]",
        "subject": "[email_subject]"
      }},
      "body": {{
        "text": {{
          "plain": "[complete_email_body_content_with_banking_context - DO NOT include subject line here]"
        }}
      }}
    }}
  ],
  "analysis": {{
    "stages": "{stages}",
    "email_summary": "[100-150 word comprehensive thread summary explaining full context]",
    "action_pending_status": "[yes/no based on thread analysis]",
    "action_pending_from": "[company/customer if pending yes or null if pending no]",
    "resolution_status": "[open/inprogress/closed based on issue resolution in thread]",
    "follow_up_required": "{follow_up_required}",
    "follow_up_date": "[ISO_timestamp or null - provide realistic date if follow_up_required=yes, null if no]",
    "follow_up_reason": "[2 lines explaining why followup needed or null - provide contextual reason if follow_up_required=yes, null if no]",
    "next_action_suggestion": "[30-50 word AI agent recommendation for customer retention improvement]",
    "urgency": {str(urgency).lower()} (MUST preserve existing urgency value: {urgency}),
    "sentiment": {{
      "0": "[0-5 human emotional state message 1]",
      "1": "[0-5 human emotional state message 2]",
      "[message_index]": "[0-5 human emotional state]"
    }},
    "overall_sentiment": "[0-5 average emotional state across thread]"
  }}
}}

**CRITICAL INSTRUCTIONS:**

1. **AUTHENTIC DATA ONLY:** Generate completely authentic, realistic EU banking email content. NO mock data, placeholder text, or generic examples. Create genuine banking scenarios with specific details, real business contexts, and authentic communication patterns.

2. **EU Banking Focus:** Generate authentic European banking scenarios with relevant regulations, terminology, and business practices. Use real EU banking terminology, compliance requirements, and business processes.

3. **Date Range:** ALL dates must be between 2025-01-01 and 2025-06-30 (6 months only) with meaningful timeframes

4. **Natural Communication:** Generate normal banking emails - avoid defaulting to crisis scenarios. Create realistic business communications that reflect actual banking operations.

5. **Banking Compliance:** Include relevant EU banking regulations and compliance considerations with specific details.

6. **Subject/Body Separation:** NEVER include the subject line in the email body content - they are separate fields

7. **Preserve Values:** MUST preserve urgency={urgency}, stages="{stages}", follow_up_required="{follow_up_required}" exactly as provided

8. **Action Pending From Field:** The action_pending_from field MUST contain ONLY one of these exact values:
   - "company" (if action is pending from the bank/company side)
   - "customer" (if action is pending from the customer side)  
   - null (if action_pending_status is "no")
   DO NOT include any names, departments, or other text in this field.

9. **Sentiment Analysis:** Individual message sentiment analysis using human emotional tone (0-5 scale):
   - 0: Happy (pleased, satisfied, positive)
   - 1: Calm (baseline for professional communication)
   - 2: Bit Irritated (slight annoyance or impatience)
   - 3: Moderately Concerned (growing unease or worry)
   - 4: Anger (clear frustration or anger)
   - 5: Frustrated (extreme frustration, very upset)
   Generate exactly {message_count} sentiment entries

10. **Content Authenticity:** Generate realistic email content that reflects actual banking operations, customer interactions, and business processes. Include specific details, realistic scenarios, and authentic business language.

Generate the EU banking email thread content now.
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
        
        # Validate analysis fields
        analysis_fields = [
            'stages', 'email_summary', 'action_pending_status', 'action_pending_from',
            'resolution_status', 'follow_up_required', 'follow_up_date', 'follow_up_reason',
            'next_action_suggestion', 'urgency', 'sentiment', 'overall_sentiment'
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
        
        # Validate sentiment count matches message count
        if len(result['analysis']['sentiment']) != len(result['messages']):
            logger.warning(f"Thread {thread_id}: Sentiment count mismatch, adjusting...")
            result['analysis']['sentiment'] = {str(i): result['analysis']['sentiment'].get(str(i), 1) for i in range(len(result['messages']))}
        
        # Validate follow_up_required matches existing value
        existing_follow_up = email_data.get('follow_up_required', 'no')
        if result['analysis'].get('follow_up_required') != existing_follow_up:
            logger.warning(f"Thread {thread_id}: LLM generated follow_up_required='{result['analysis'].get('follow_up_required')}' but existing value is '{existing_follow_up}'. Correcting...")
            result['analysis']['follow_up_required'] = existing_follow_up
        
        # Validate urgency matches existing value
        existing_urgency = email_data.get('urgency', False)
        if result['analysis'].get('urgency') != existing_urgency:
            logger.warning(f"Thread {thread_id}: LLM generated urgency='{result['analysis'].get('urgency')}' but existing value is '{existing_urgency}'. Correcting...")
            result['analysis']['urgency'] = existing_urgency
        
        # Validate action_pending_from values - must be only company/customer/null
        valid_action_sources = ['company', 'customer', None, 'null']
        action_pending_from = result['analysis'].get('action_pending_from')
        
        # Clean up any names or extra text that might be in the field
        if action_pending_from and isinstance(action_pending_from, str):
            action_pending_from = action_pending_from.strip().lower()
            # Extract only the valid part if it contains extra text
            if 'company' in action_pending_from:
                action_pending_from = 'company'
            elif 'customer' in action_pending_from:
                action_pending_from = 'customer'
            elif action_pending_from in ['null', 'none', '']:
                action_pending_from = None
        
        if action_pending_from not in valid_action_sources:
            logger.warning(f"Thread {thread_id}: Invalid action_pending_from='{action_pending_from}', correcting to 'company'")
            result['analysis']['action_pending_from'] = 'company'
        elif action_pending_from == 'null':
            result['analysis']['action_pending_from'] = None
        else:
            result['analysis']['action_pending_from'] = action_pending_from
        
        # Clean up any subject lines that might appear in body content
        if 'messages' in result:
            for message in result['messages']:
                if 'body' in message and 'text' in message['body'] and 'plain' in message['body']['text']:
                    body_content = message['body']['text']['plain']
                    
                    # Check for various subject line patterns and remove them
                    subject_patterns = ['Subject:', 'SUBJECT:', 'subject:', 'Re:', 'RE:', 'Fwd:', 'FWD:']
                    
                    cleaned_body = body_content
                    for pattern in subject_patterns:
                        if pattern in cleaned_body:
                            lines = cleaned_body.split('\n')
                            cleaned_lines = []
                            
                            for line in lines:
                                if any(line.strip().startswith(p) for p in subject_patterns):
                                    logger.warning(f"Removed subject line '{line.strip()}' from body content for thread {thread_id}")
                                    continue
                                cleaned_lines.append(line)
                            
                            cleaned_body = '\n'.join(cleaned_lines)
                    
                    message['body']['text']['plain'] = cleaned_body
        
        generation_time = time.time() - start_time
        
        # Log success
        success_info = {
            'thread_id': thread_id,
            'dominant_topic': email_data.get('dominant_topic'),
            'subject': result['thread_data']['subject_norm'],
            'stages': result['analysis']['stages'],
            'urgency': result['analysis']['urgency'],
            'resolution_status': result['analysis']['resolution_status'],
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
        
        # Debug: Log the generated content structure
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
        
        # Update analysis fields from the LLM response
        if 'analysis' in email_content:
            analysis = email_content['analysis']
            update_doc['stages'] = analysis.get('stages')
            update_doc['email_summary'] = analysis.get('email_summary')
            update_doc['action_pending_status'] = analysis.get('action_pending_status')
            update_doc['action_pending_from'] = analysis.get('action_pending_from')
            update_doc['resolution_status'] = analysis.get('resolution_status')
            update_doc['follow_up_required'] = analysis.get('follow_up_required')
            update_doc['follow_up_date'] = analysis.get('follow_up_date')
            update_doc['follow_up_reason'] = analysis.get('follow_up_reason')
            update_doc['next_action_suggestion'] = analysis.get('next_action_suggestion')
            update_doc['urgency'] = analysis.get('urgency')
            update_doc['sentiment'] = analysis.get('sentiment')
            update_doc['overall_sentiment'] = analysis.get('overall_sentiment')
        
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
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create bulk operations
        bulk_operations = []
        thread_ids = []
        
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"thread.thread_id": update_data['thread_id']},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            thread_ids.append(update_data['thread_id'])
        
        if bulk_operations:
            try:
                result = email_col.bulk_write(bulk_operations, ordered=False)
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
                        result = email_col.update_one(
                            {"thread.thread_id": update_data['thread_id']},
                            {"$set": update_data['update_doc']}
                        )
                        if result.modified_count > 0:
                            individual_success += 1
                    except Exception as individual_error:
                        logger.error(f"Individual update failed for {update_data['thread_id']}: {individual_error}")
                
                logger.info(f"Fallback: {individual_success} records saved individually")
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

async def process_emails_optimized():
    """Main optimized processing function for email generation"""
    logger.info("Starting Optimized EU Banking Email Content Generation...")
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
                # Must have the basic fields that were set by previous scripts
                {"stages": {"$exists": True}},
                {"urgency": {"$exists": True}},
                {"follow_up_required": {"$exists": True}},
                # Must be missing LLM-generated content fields
                {
                    "$and": [
                        {"email_summary": {"$exists": False}},
                        {"action_pending_status": {"$exists": False}},
                        {"action_pending_from": {"$exists": False}},
                        {"resolution_status": {"$exists": False}},
                        {"next_action_suggestion": {"$exists": False}},
                        {"sentiment": {"$exists": False}},
                        {"overall_sentiment": {"$exists": False}}
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
                {"urgency": {"$exists": True}},
                {"follow_up_required": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        emails_with_some_llm_fields = email_col.count_documents({
            "$or": [
                {"email_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_from": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"sentiment": {"$exists": True, "$ne": None, "$ne": ""}},
                {"overall_sentiment": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        emails_with_all_llm_fields = email_col.count_documents({
            "$and": [
                {"stages": {"$exists": True, "$ne": None, "$ne": ""}},
                {"email_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"follow_up_required": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"sentiment": {"$exists": True, "$ne": None, "$ne": ""}},
                {"overall_sentiment": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        
        # Calculate actual emails needing processing
        emails_needing_processing = email_col.count_documents(query)
        
        # Debug: Let's also check what fields actually exist
        logger.info("Debug - Checking what fields exist in emails:")
        for field in ["stages", "email_summary", "action_pending_status", "resolution_status", "follow_up_required", "urgency"]:
            count = email_col.count_documents({field: {"$exists": True}})
            logger.info(f"  {field}: {count} emails have this field")
        
        logger.info(f"Database Status:")
        logger.info(f"  Total emails in DB: {total_emails_in_db}")
        logger.info(f"  Emails with basic fields (stages, urgency, follow_up_required): {emails_with_basic_fields}")
        logger.info(f"  Emails processed by LLM (llm_processed=True): {emails_processed_by_llm}")
        logger.info(f"  Emails with some LLM fields: {emails_with_some_llm_fields}")
        logger.info(f"  Emails with ALL LLM fields: {emails_with_all_llm_fields}")
        logger.info(f"  Emails needing processing: {emails_needing_processing}")
        
        email_records = list(email_col.find(query))
        total_emails = len(email_records)
        
        if total_emails == 0:
            logger.info("No emails found that need processing!")
            logger.info("All emails appear to have been processed by LLM already.")
            return
        
        logger.info(f"Found {total_emails} emails that need LLM processing")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_emails)} emails")
        progress_logger.info(f"BATCH_START: total_emails={total_emails}")
        
    except Exception as e:
        logger.error(f"Error fetching email records: {e}")
        return
    
    # Process emails in optimized batches
    total_updated = 0
    batch_updates = []
    
    try:
        # Process emails in concurrent batches
        for i in range(0, total_emails, BATCH_SIZE):
            if shutdown_flag.is_set():
                logger.info("Shutdown requested, stopping processing")
                break
            
            batch = email_records[i:i + BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (total_emails + BATCH_SIZE - 1)//BATCH_SIZE
            logger.info(f"Processing batch {batch_num}/{total_batches} (emails {i+1}-{min(i+BATCH_SIZE, total_emails)})")
            
            # Process batch concurrently
            batch_tasks = []
            for email in batch:
                if not checkpoint_manager.is_processed(email.get('thread', {}).get('thread_id')):
                    task = process_single_email(email, total_emails)
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
                                    checkpoint_manager.mark_processed(result['thread_id'], success=True),
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
            processed_so_far = min(i + BATCH_SIZE, total_emails)
            progress_pct = (processed_so_far / total_emails) * 100
            logger.info(f"Overall Progress: {progress_pct:.1f}% ({processed_so_far}/{total_emails})")
            
            # Update performance monitor with actual total
            await performance_monitor.log_progress(total_emails)
            
            # Brief delay between batches to manage rate limits
            if i + BATCH_SIZE < total_emails and not shutdown_flag.is_set():
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
        
        logger.info(f"Final Results:")
        logger.info(f"  Total emails updated: {total_updated}")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_email = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per email: {avg_time_per_email:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} emails/hour" if total_time > 0 else "Processing rate: N/A")
        
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
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "email_summary": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_status": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "follow_up_required": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None}
        })
        
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
        logger.info(f"  With complete analysis: {with_complete_analysis}")
        logger.info(f"  Without complete analysis: {without_complete_analysis}")
        logger.info(f"  Urgent emails: {urgent_emails} ({(urgent_emails/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent emails: 0")
        logger.info(f"  Completion rate: {(with_complete_analysis/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"  {i}. {topic['_id']}: {topic['count']} emails")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_emails(limit=3):
    """Get sample emails with generated analysis"""
    try:
        samples = list(email_col.find({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "email_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Email Analysis:")
        for i, email in enumerate(samples, 1):
            logger.info(f"--- Sample Email {i} ---")
            logger.info(f"Thread ID: {email.get('thread', {}).get('thread_id', 'N/A')}")
            logger.info(f"Subject: {email.get('thread', {}).get('subject_norm', 'N/A')}")
            logger.info(f"Dominant Topic: {email.get('dominant_topic', 'N/A')}")
            logger.info(f"Stages: {email.get('stages', 'N/A')}")
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