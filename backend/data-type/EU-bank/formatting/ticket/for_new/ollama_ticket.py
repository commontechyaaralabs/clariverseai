# EU Banking Trouble Ticket Content Generator - Optimized Version
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
TICKET_COLLECTION = "tickets"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"optimized_ticket_generator_{timestamp}.log"
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
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Global variables for graceful shutdown
shutdown_flag = asyncio.Event()
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
        self.tickets_processed = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self._lock = asyncio.Lock()
    
    async def record_success(self, total_tickets=None):
        async with self._lock:
            self.successful_requests += 1
            self.tickets_processed += 1
            await self.log_progress(total_tickets)
    
    async def record_failure(self, total_tickets=None):
        async with self._lock:
            self.failed_requests += 1
            await self.log_progress(total_tickets)
    
    async def log_progress(self, total_tickets=None):
        if self.tickets_processed % 100 == 0 and self.tickets_processed > 0:
            elapsed = time.time() - self.start_time
            rate = self.tickets_processed / elapsed if elapsed > 0 else 0
            remaining_tickets = (total_tickets - self.tickets_processed) if total_tickets else 0
            eta = remaining_tickets / rate if rate > 0 and remaining_tickets > 0 else 0
            
            logger.info(f"Performance Stats:")
            if total_tickets:
                logger.info(f"  Processed: {self.tickets_processed}/{total_tickets} tickets")
            else:
                logger.info(f"  Processed: {self.tickets_processed} tickets")
            logger.info(f"  Rate: {rate:.2f} tickets/second ({rate*3600:.0f} tickets/hour)")
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
        self.processed_tickets = set()
        self.failed_tickets = set()
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
                    self.processed_tickets = set(data.get('processed_tickets', []))
                    self.failed_tickets = set(data.get('failed_tickets', []))
                    self.stats.update(data.get('stats', {}))
                logger.info(f"Loaded checkpoint: {len(self.processed_tickets)} processed, {len(self.failed_tickets)} failed")
        except Exception as e:
            logger.warning(f"Could not load checkpoint: {e}")
    
    async def save_checkpoint(self):
        async with self._lock:
            try:
                checkpoint_data = {
                    'processed_tickets': list(self.processed_tickets),
                    'failed_tickets': list(self.failed_tickets),
                    'stats': self.stats,
                    'timestamp': datetime.now().isoformat()
                }
                with open(self.checkpoint_file, 'w') as f:
                    json.dump(checkpoint_data, f, indent=2)
            except Exception as e:
                logger.error(f"Could not save checkpoint: {e}")
    
    def is_processed(self, ticket_id):
        return str(ticket_id) in self.processed_tickets
    
    async def mark_processed(self, ticket_id, success=True):
        async with self._lock:
            ticket_id_str = str(ticket_id)
            self.processed_tickets.add(ticket_id_str)
            self.stats['processed_count'] += 1
            
            if success:
                self.stats['success_count'] += 1
                self.failed_tickets.discard(ticket_id_str)
            else:
                self.stats['failure_count'] += 1
                self.failed_tickets.add(ticket_id_str)
            
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
        ticket_col.create_index("title")
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

def generate_optimized_prompt(ticket_data):
    """Generate highly optimized, shorter prompt while maintaining quality output"""
    dominant_topic = ticket_data.get('dominant_topic', 'General Banking System Issue')
    subtopics = ticket_data.get('subtopics', 'System malfunction')
    message_count = ticket_data.get('thread', {}).get('message_count', 2)
    existing_urgency = ticket_data.get('urgency', False)
    existing_follow_up = ticket_data.get('follow_up_required', 'no')
    
    banking_details = generate_realistic_banking_details(ticket_data)
    
    urgency_context = "URGENT" if existing_urgency else "NON-URGENT"
    
    prompt = f"""Generate EU banking trouble ticket JSON. CRITICAL: Return ONLY valid JSON, no other text.

CONTEXT:
Topic: {dominant_topic} | Subtopic: {subtopics}
Messages: {message_count} | Urgency: {urgency_context} ({existing_urgency})
Follow-up Required: {existing_follow_up} (MUST PRESERVE THIS VALUE)
Customer: {banking_details['customer_name']} | Account: {banking_details['account_number']}
Ticket: {banking_details['ticket_number']} | System: {banking_details['system_name']}

TEAMS AVAILABLE:
- customerservice@eubank.com (General support)
- financialcrime@eubank.com (Fraud, suspicious activity)
- loansupport@eubank.com (Loans, credit cards)
- kyc@eubank.com (Identity, compliance)
- cardoperations@eubank.com (Card issues)
- digitalbanking@eubank.com (App, online banking)
- internalithelpdesk@eubank.com (Internal IT)

OUTPUT FORMAT REQUIRED - EXACTLY THIS STRUCTURE:
{{
  "title": "Professional ticket title 80-120 characters - match {urgency_context} tone",
  "priority": "P1-Critical|P2-High|P3-Medium|P4-Low|P5-Very Low - match urgency={existing_urgency}",
  "assigned_team_email": "exact_email@eubank.com",
  "ticket_summary": "Business summary 150-200 words describing issue, impact, customer details",
  "action_pending_status": "yes|no",
  "action_pending_from": "company|customer|null (null if action_pending_status=no)",
  "resolution_status": "open|inprogress|closed",
  "follow_up_required": "yes|no (MUST match existing value: {existing_follow_up})",
  "follow_up_date": "2025-MM-DDTHH:MM:SS or null (provide date if follow_up_required=yes, can be null if no)",
  "follow_up_reason": "specific reason WHY follow-up is needed or null (examples: 'To verify the issue is resolved after system update', 'To confirm customer satisfaction with resolution', 'To check if additional support is required', 'To monitor for recurring problems', 'To gather feedback on implemented solution', 'To ensure transaction processing is working correctly', 'To validate that account access is restored', 'To confirm fraud protection measures are effective', 'To verify mobile app functionality after update' - provide contextual reason if follow_up_required=yes, null if no)",
  "next_action_suggestion": "Next step recommendation 50-80 words",
  "messages": [
    {{
      "content": "Customer message 300-400 words in flexible, conversational format. Use natural customer communication style with proper line breaks (\\n) for formatting:\n\n[Choose appropriate customer message style based on situation]:\\n\\n1. **Initial Complaint**: 'Dear Support Team,\\n\\nI am writing to report an issue with [specific problem]. [Detailed description 250-350 words with relevant details such as: specific dates/times, amounts, account details {banking_details['account_number']}, customer name {banking_details['customer_name']}, exact error messages, transaction IDs, what was attempted, what happened, context and background]. Include device/technical details ONLY if the issue is related to mobile app, online banking, ATM, or card terminal problems. For general banking issues like account questions, loan inquiries, or fraud reports, focus on the banking context rather than technical details.\\n\\n[Add specific sections only when genuinely needed]:\\nExpected Outcome: [Only if customer has clear expectations]\\nImpact: [Only if there's significant business or personal impact]\\nService & Device Details: [Only for technical issues involving apps, websites, ATMs, or card terminals]\\nAttachment: [Only if customer mentions screenshots or documents]'\\n\\n2. **Acknowledgment Response**: 'Hi [NAME],\\n\\nThank you for reaching out and looking into my issue. I appreciate the quick response...'\\n\\n3. **Update/Clarification**: 'Hello,\\n\\nI wanted to provide some additional information about my case. [Details and updates]...'\\n\\n4. **Confirmation/Satisfaction**: 'Dear Support,\\n\\nThank you for resolving this issue so quickly. Everything is working perfectly now...'\\n\\n5. **Escalation Request**: 'Hello,\\n\\nI need this matter escalated to a supervisor. The issue is still not resolved and [reason]...'\\n\\n6. **Follow-up/Concern**: 'Hi,\\n\\nIt's been a while since we last spoke about [issue]. I'm still experiencing problems and wanted to check the status...'\\n\\n7. **Information Request**: 'Dear Team,\\n\\nCould you please provide more details about [specific question]? I need clarification on [topic]...'\\n\\n8. **Urgent Follow-up**: 'URGENT - Hello,\\n\\nI need immediate assistance with [issue]. This is affecting [impact] and requires urgent attention...'\\n\\nUse natural paragraph breaks (\\n\\n) and only include subheadings when they genuinely add clarity. Most customer messages should flow naturally without forced structure.",
      "sender_type": "customer",
      "headers": {{
        "date": "2025-MM-DD HH:MM:SS (Generate date between 2025-01-01 and 2025-06-30, use realistic business hours 08:00-18:00 for routine issues, 00:00-23:59 for urgent issues)"
      }}
    }}{"," if message_count > 1 else ""}
    {"{"}"content": "Company response 300-400 words in flexible, professional format with proper line breaks (\\n) for formatting. Use varied response styles:\n\n[Always start with Ticket Reference: {banking_details['ticket_number']}]\\n\\n[Choose appropriate response style based on situation]:\\n\\n1. **Acknowledgment Response**: 'Dear [Customer],\\n\\nThank you for contacting us regarding [issue]. We have received your message and are looking into this matter...'\\n\\n2. **Update Response**: 'Hello [Customer],\\n\\nI wanted to provide you with an update on your case [Ticket ID]. Currently, we are [status] and expect to have this resolved by [timeframe]...'\\n\\n3. **Resolution Response**: 'Dear [Customer],\\n\\nGood news! We have successfully resolved the issue you reported. [Explanation of what was fixed]...'\\n\\n4. **Information Request**: 'Hello [Customer],\\n\\nTo better assist you with [issue], we need some additional information. Could you please provide [specific details]...'\\n\\n5. **Apology Response**: 'Dear [Customer],\\n\\nWe sincerely apologize for the inconvenience caused by [issue]. We understand how frustrating this must be...'\\n\\n6. **Follow-up Response**: 'Hello [Customer],\\n\\nWe wanted to follow up on your recent inquiry about [issue]. [Current status and next steps]...'\\n\\n7. **Escalation Response**: 'Dear [Customer],\\n\\nThank you for your patience. Your case has been escalated to our specialized team who will [action]...'\\n\\nUse natural paragraph breaks (\\n\\n) and only include subheadings like 'Investigation Status:', 'Resolution Steps:', 'Next Actions:' when they genuinely add clarity to the response. Most responses should flow naturally without forced structure.",
    "sender_type": "company",
    "headers": {{
      "date": "2025-MM-DD HH:MM:SS (Generate date between 2025-01-01 and 2025-06-30, use realistic business hours 08:00-18:00 for routine issues, 00:00-23:59 for urgent issues)"
    }}{"}"}{"" if message_count <= 2 else "... continue alternating pattern for " + str(message_count) + " total messages with same detailed format"}
  ],
  "sentiment": {{"0": sentiment_score_message_1, "1": sentiment_score_message_2{"..." if message_count > 2 else ""}}} (Individual message sentiment analysis using human emotional tone 0-5 scale. Generate sentiment for each message based on message_count:
- 0: Happy (pleased, satisfied, positive)
- 1: Calm (baseline for professional communication)  
- 2: Bit Irritated (slight annoyance or impatience)
- 3: Moderately Concerned (growing unease or worry)
- 4: Anger (clear frustration or anger)
- 5: Frustrated (extreme frustration, very upset)

CRITICAL: If message_count is 1, only generate sentiment for message "0". If message_count is 2, generate sentiment for "0" and "1", etc.),
  "overall_sentiment": 0.0-5.0 (overall ticket sentiment based on issue severity and resolution quality),
  "ticket_raised": "2025-01-01T08:00:00 to 2025-06-30T18:00:00 (business hours for routine, after-hours for urgent)",
  "thread_dates": {{
    "first_message_at": "2025-MM-DD HH:MM:SS (Use the earliest date from messages.headers.date)",
    "last_message_at": "2025-MM-DD HH:MM:SS (Use the latest date from messages.headers.date)"
  }}
}}

VALIDATION REQUIREMENTS:
✓ Generate exactly {message_count} messages alternating customer/company
✓ Match urgency={existing_urgency} in title, priority, content tone
✓ Use realistic banking language and account details
✓ Include specific error codes, amounts, system names, transaction IDs when relevant
✓ Each message must be 300-400 words with realistic ticket structure and proper line breaks (\\n) for formatting
✓ Customer messages: Use flexible conversational format with varied styles (initial complaint, acknowledgment, update/clarification, confirmation/satisfaction, escalation request, follow-up/concern, information request, urgent follow-up). Only use subheadings when they genuinely add clarity - most messages should flow naturally without forced structure
✓ Company messages: Always include "Ticket Reference" at the start. Use flexible response styles (acknowledgment, update, resolution, information request, apology, follow-up, escalation) with natural flow. Only use subheadings when they genuinely add clarity - most responses should flow naturally without forced structure
✓ Use subheadings flexibly - only include "Impact", "Service & Device Details", "Attachment" etc. when they make sense for the specific issue
✓ Device/technical details ONLY for app, online banking, ATM, or card terminal issues - NOT for general banking inquiries
✓ For general banking issues (account questions, loans, fraud), focus on banking context rather than technical details
✓ Sentiment matches message count: {message_count} entries (CRITICAL: If message_count=1, only generate sentiment for "0". If message_count=2, generate for "0" and "1", etc.)
✓ Sentiment analysis: Use human emotional tone scale (0: Happy/pleased/satisfied, 1: Calm/baseline professional, 2: Bit Irritated/slight annoyance, 3: Moderately Concerned/growing unease, 4: Anger/clear frustration, 5: Frustrated/extreme frustration)
✓ Overall sentiment: Consider issue severity and resolution quality - critical issues=1-2, resolved issues=4-5, ongoing issues=2-3
✓ Follow-up fields: CRITICAL - follow_up_required MUST match existing value "{existing_follow_up}". If existing value is "no", set follow_up_required="no" and leave date/reason as null. If existing value is "yes", set follow_up_required="yes" and provide meaningful follow_up_date and specific follow_up_reason explaining WHY follow-up is needed (e.g., "To verify resolution after system update", "To confirm customer satisfaction", "To monitor for recurring issues")
✓ Date generation: CRITICAL - All dates must be between 2025-01-01 and 2025-06-30. Use format "2025-MM-DD HH:MM:SS". Generate realistic chronological order with customer messages first, then company responses. Use business hours (08:00-18:00) for routine issues, any time (00:00-23:59) for urgent issues. Set thread_dates.first_message_at to the earliest message date and thread_dates.last_message_at to the latest message date.
✓ Include realistic banking terminology appropriate to the specific issue type
✓ CRITICAL: Use \\n\\n between paragraphs and \\n after subheadings for proper formatting - do not generate text as one continuous paragraph

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
                'X-Title': 'EU Banking Ticket Generator'
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

async def generate_ticket_content(ticket_data):
    """Generate ticket content with optimized processing"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    ticket_id = str(ticket_data.get('_id', 'unknown'))
    
    try:
        prompt = generate_optimized_prompt(ticket_data)
        
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
            logger.info(f"Ticket {ticket_id}: JSON parsing successful. Keys: {list(result.keys())}")
        except json.JSONDecodeError as json_err:
            logger.error(f"JSON parsing failed for ticket {ticket_id}. Raw response: {reply[:300]}...")
            logger.error(f"Ticket {ticket_id}: Full LLM response: {response[:500]}...")
            raise ValueError(f"Invalid JSON response from LLM: {json_err}")
        
        # Validate required fields
        required_fields = [
            'title', 'priority', 'assigned_team_email', 'ticket_summary',
            'action_pending_status', 'action_pending_from', 'resolution_status',
            'follow_up_date', 'follow_up_reason', 'next_action_suggestion', 
            'sentiment', 'overall_sentiment', 'ticket_raised', 'messages'
        ]
        
        missing_fields = [field for field in required_fields if field not in result]
        if missing_fields:
            logger.error(f"Ticket {ticket_id}: Missing required fields: {missing_fields}")
            logger.error(f"Ticket {ticket_id}: Generated result keys: {list(result.keys())}")
            logger.error(f"Ticket {ticket_id}: Raw LLM response: {response[:500]}...")
            raise ValueError(f"Missing required fields: {missing_fields}")
        
        # Validate messages count
        message_count = ticket_data.get('thread', {}).get('message_count', 2)
        if len(result['messages']) != message_count:
            logger.warning(f"Ticket {ticket_id}: Expected {message_count} messages, got {len(result['messages'])}")
            # Adjust to correct count
            if len(result['messages']) > message_count:
                result['messages'] = result['messages'][:message_count]
            
        # Validate sentiment count matches message count
        if len(result['sentiment']) != len(result['messages']):
            logger.warning(f"Ticket {ticket_id}: Sentiment count mismatch, adjusting...")
            result['sentiment'] = {str(i): result['sentiment'].get(str(i), 1) for i in range(len(result['messages']))}
        
        # Validate follow_up_required matches existing value
        existing_follow_up = ticket_data.get('follow_up_required', 'no')
        if result.get('follow_up_required') != existing_follow_up:
            logger.warning(f"Ticket {ticket_id}: LLM generated follow_up_required='{result.get('follow_up_required')}' but existing value is '{existing_follow_up}'. Correcting...")
            result['follow_up_required'] = existing_follow_up
        
        # Validate date format and range
        try:
            ticket_date = datetime.fromisoformat(result['ticket_raised'].replace('Z', ''))
            start_date = datetime(2025, 1, 1)
            end_date = datetime(2025, 6, 30, 23, 59, 59)
            
            if not (start_date <= ticket_date <= end_date):
                if ticket_date > end_date:
                    ticket_date = end_date
                elif ticket_date < start_date:
                    ticket_date = start_date
                result['ticket_raised'] = ticket_date.strftime('%Y-%m-%dT%H:%M:%S')
        except:
            # Default date if parsing fails
            result['ticket_raised'] = '2025-03-15T12:00:00'
        
        generation_time = time.time() - start_time
        
        # Log success
        success_info = {
            'ticket_id': ticket_id,
            'dominant_topic': ticket_data.get('dominant_topic'),
            'title': result['title'],
            'priority': result['priority'],
            'urgency': ticket_data.get('urgency'),
            'resolution_status': result['resolution_status'],
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
                
                # Update message content (title will be set programmatically later)
                updates[f'messages.{msg_idx}.body.text.plain'] = generated_msg['content']
                
                # Update email addresses based on sender type
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

async def process_single_ticket(ticket_record, total_tickets=None):
    """Process a single ticket with all optimizations"""
    if shutdown_flag.is_set():
        return None
    
    ticket_id = str(ticket_record.get('_id', 'unknown'))
    
    try:
        # NO TIMEOUT - let the task complete naturally
        return await _process_single_ticket_internal(ticket_record, total_tickets)
    except Exception as e:
        logger.error(f"Ticket {ticket_id} processing failed with error: {str(e)[:100]}")
        await performance_monitor.record_failure(total_tickets)
        await failure_counter.increment()
        await checkpoint_manager.mark_processed(ticket_id, success=False)
        return None

async def _process_single_ticket_internal(ticket_record, total_tickets=None):
    """Internal ticket processing logic"""
    ticket_id = str(ticket_record.get('_id', 'unknown'))
    
    try:
        # Generate content
        ticket_content = await generate_ticket_content(ticket_record)
        
        if not ticket_content:
            await performance_monitor.record_failure(total_tickets)
            return None
        
        # Debug: Log the generated content structure
        logger.info(f"Ticket {ticket_id}: Generated content keys: {list(ticket_content.keys()) if isinstance(ticket_content, dict) else 'Not a dict'}")
        if isinstance(ticket_content, dict) and 'title' in ticket_content:
            logger.info(f"Ticket {ticket_id}: Title field found: {ticket_content['title'][:50]}...")
        else:
            logger.error(f"Ticket {ticket_id}: Title field missing from generated content")
            logger.error(f"Ticket {ticket_id}: Full content structure: {ticket_content}")
            await performance_monitor.record_failure(total_tickets)
            return None
        
        # Debug: Check messages structure
        if 'messages' in ticket_content and ticket_content['messages']:
            logger.info(f"Ticket {ticket_id}: Messages count: {len(ticket_content['messages'])}")
            for i, msg in enumerate(ticket_content['messages']):
                logger.info(f"Ticket {ticket_id}: Message {i} keys: {list(msg.keys()) if isinstance(msg, dict) else 'Not a dict'}")
                if isinstance(msg, dict) and 'sender_type' in msg:
                    logger.info(f"Ticket {ticket_id}: Message {i} sender_type: {msg['sender_type']}")
        else:
            logger.error(f"Ticket {ticket_id}: No messages or empty messages array")
        
        # Handle follow_up fields logic programmatically - RESPECT EXISTING DB VALUES
        existing_follow_up_required = ticket_record.get('follow_up_required', 'no')
        
        if existing_follow_up_required == 'no':
            # If DB has follow_up_required='no', keep it as 'no' and set date/reason to null
            follow_up_required = 'no'
            follow_up_date = None
            follow_up_reason = None
            logger.info(f"Ticket {ticket_id}: DB has follow_up_required='no', keeping as 'no' and setting date/reason=null")
        else:
            # If DB has follow_up_required='yes', use LLM generated values but validate
            llm_follow_up = ticket_content.get('follow_up_required', 'no')
            if llm_follow_up != 'yes':
                logger.warning(f"Ticket {ticket_id}: LLM generated follow_up_required='{llm_follow_up}' but DB has 'yes'. Forcing to 'yes'.")
                follow_up_required = 'yes'
            else:
                follow_up_required = 'yes'
            
            follow_up_date = ticket_content.get('follow_up_date')
            follow_up_reason = ticket_content.get('follow_up_reason')
            logger.info(f"Ticket {ticket_id}: DB has follow_up_required='{existing_follow_up_required}', using LLM values: required={follow_up_required}, date={follow_up_date}, reason={follow_up_reason}")
        
        # Prepare update document
        update_doc = {
            "title": ticket_content['title'],
            "priority": ticket_content['priority'],
            "assigned_team_email": ticket_content['assigned_team_email'],
            "ticket_summary": ticket_content['ticket_summary'],
            "action_pending_status": ticket_content['action_pending_status'],
            "action_pending_from": ticket_content['action_pending_from'],
            "resolution_status": ticket_content['resolution_status'],
            "follow_up_required": follow_up_required,
            "follow_up_date": follow_up_date,
            "follow_up_reason": follow_up_reason,
            "next_action_suggestion": ticket_content['next_action_suggestion'],
            "sentiment": ticket_content['sentiment'],
            "overall_sentiment": ticket_content['overall_sentiment'],
            "ticket_raised": ticket_content['ticket_raised'],
            # Add LLM processing tracking
            "llm_processed": True,
            "llm_processed_at": datetime.now().isoformat(),
            "llm_model_used": OPENROUTER_MODEL
        }
        
        # Add email and message updates
        logger.info(f"Ticket {ticket_id}: About to populate email addresses...")
        try:
            email_updates = populate_email_addresses(ticket_record, ticket_content['assigned_team_email'], ticket_content['messages'])
            update_doc.update(email_updates)
            logger.info(f"Ticket {ticket_id}: Email updates completed successfully")
        except Exception as email_err:
            logger.error(f"Ticket {ticket_id}: Error in populate_email_addresses: {email_err}")
            raise
        
        logger.info(f"Ticket {ticket_id}: About to set message titles...")
        
        # Programmatically set titles for consistency
        if 'title' not in ticket_content:
            logger.error(f"Ticket {ticket_id}: No 'title' field in generated content. Keys: {list(ticket_content.keys())}")
            raise KeyError("Missing 'title' field in generated content")
        
        main_title = ticket_content['title']
        
        # Set thread title to match main title
        update_doc['thread.ticket_title'] = main_title
        
        # Update message titles programmatically
        if ticket_content['messages']:
            logger.info(f"Ticket {ticket_id}: Setting titles for {len(ticket_content['messages'])} messages...")
            for i, message in enumerate(ticket_content['messages']):
                if message['sender_type'] == 'customer':
                    # Customer messages use the main title
                    update_doc[f'messages.{i}.title'] = main_title
                    update_doc[f'messages.{i}.headers.ticket_title'] = main_title
                else:
                    # Company messages use RE: prefix
                    update_doc[f'messages.{i}.title'] = f"RE: {main_title}"
                    update_doc[f'messages.{i}.headers.ticket_title'] = f"RE: {main_title}"
            logger.info(f"Ticket {ticket_id}: Message titles set successfully")
        
        # Add thread dates from LLM generated content
        if 'thread_dates' in ticket_content:
            thread_dates = ticket_content['thread_dates']
            if 'first_message_at' in thread_dates:
                update_doc['thread.first_message_at'] = thread_dates['first_message_at']
            if 'last_message_at' in thread_dates:
                update_doc['thread.last_message_at'] = thread_dates['last_message_at']
            logger.info(f"Ticket {ticket_id}: Thread dates set successfully")
        
        # Add message dates from LLM generated content
        if ticket_content.get('messages'):
            logger.info(f"Ticket {ticket_id}: Setting dates for {len(ticket_content['messages'])} messages...")
            for i, message in enumerate(ticket_content['messages']):
                if message.get('headers', {}).get('date'):
                    update_doc[f'messages.{i}.headers.date'] = message['headers']['date']
            logger.info(f"Ticket {ticket_id}: Message dates set successfully")
        
        logger.info(f"Ticket {ticket_id}: About to record success...")
        await performance_monitor.record_success(total_tickets)
        logger.info(f"Ticket {ticket_id}: Success recorded, incrementing counter...")
        await success_counter.increment()
        logger.info(f"Ticket {ticket_id}: Counter incremented, returning result...")
        
        return {
            'ticket_id': str(ticket_record['_id']),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Ticket {ticket_id} internal processing failed: {str(e)[:100]}")
        raise  # Re-raise to be caught by the outer timeout handler

async def save_batch_to_database(batch_updates):
    """Save batch updates to database with optimized bulk operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create bulk operations
        bulk_operations = []
        ticket_ids = []
        
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"_id": ObjectId(update_data['ticket_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            ticket_ids.append(update_data['ticket_id'])
        
        if bulk_operations:
            try:
                result = ticket_col.bulk_write(bulk_operations, ordered=False)
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
                        result = ticket_col.update_one(
                            {"_id": ObjectId(update_data['ticket_id'])},
                            {"$set": update_data['update_doc']}
                        )
                        if result.modified_count > 0:
                            individual_success += 1
                    except Exception as individual_error:
                        logger.error(f"Individual update failed for {update_data['ticket_id']}: {individual_error}")
                
                logger.info(f"Fallback: {individual_success} records saved individually")
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

async def process_tickets_optimized():
    """Main optimized processing function"""
    logger.info("Starting Optimized EU Banking Trouble Ticket Content Generation...")
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
    
    # Get tickets to process - only those that have NEVER been processed by LLM
    try:
        # Simple and accurate query: tickets that are missing ALL LLM fields
        # This ensures we only process tickets that haven't been touched by the LLM
        query = {
            "$and": [
                # Must have basic ticket structure
                {"_id": {"$exists": True}},
                {"thread": {"$exists": True}},
                # Must be missing ALL core LLM fields
                {
                    "$and": [
                        {"title": {"$exists": False}},
                        {"priority": {"$exists": False}},
                        {"assigned_team_email": {"$exists": False}},
                        {"ticket_summary": {"$exists": False}},
                        {"resolution_status": {"$exists": False}},
                        {"overall_sentiment": {"$exists": False}},
                        {"ticket_raised": {"$exists": False}},
                        {"action_pending_status": {"$exists": False}},
                        {"action_pending_from": {"$exists": False}},
                        {"next_action_suggestion": {"$exists": False}},
                        {"sentiment": {"$exists": False}}
                    ]
                }
            ]
        }
        
        # Exclude already processed tickets
        if checkpoint_manager.processed_tickets:
            processed_ids = [ObjectId(tid) for tid in checkpoint_manager.processed_tickets if ObjectId.is_valid(tid)]
            query["_id"] = {"$nin": processed_ids}
        
        # First, let's check what tickets exist and their status
        total_tickets_in_db = ticket_col.count_documents({})
        tickets_processed_by_llm = ticket_col.count_documents({"llm_processed": True})
        tickets_with_some_llm_fields = ticket_col.count_documents({
            "$or": [
                {"title": {"$exists": True, "$ne": None, "$ne": ""}},
                {"priority": {"$exists": True, "$ne": None, "$ne": ""}},
                {"assigned_team_email": {"$exists": True, "$ne": None, "$ne": ""}},
                {"ticket_summary": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        tickets_with_all_llm_fields = ticket_col.count_documents({
            "$and": [
                {"title": {"$exists": True, "$ne": None, "$ne": ""}},
                {"priority": {"$exists": True, "$ne": None, "$ne": ""}},
                {"assigned_team_email": {"$exists": True, "$ne": None, "$ne": ""}},
                {"ticket_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"overall_sentiment": {"$exists": True, "$ne": None, "$ne": ""}},
                {"ticket_raised": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_from": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"sentiment": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        
        # Calculate actual tickets needing processing using the same query
        tickets_needing_processing = ticket_col.count_documents(query)
        
        logger.info(f"Database Status:")
        logger.info(f"  Total tickets in DB: {total_tickets_in_db}")
        logger.info(f"  Tickets processed by LLM (llm_processed=True): {tickets_processed_by_llm}")
        logger.info(f"  Tickets with some LLM fields: {tickets_with_some_llm_fields}")
        logger.info(f"  Tickets with ALL LLM fields: {tickets_with_all_llm_fields}")
        logger.info(f"  Tickets needing processing: {tickets_needing_processing}")
        
        ticket_records = list(ticket_col.find(query))
        total_tickets = len(ticket_records)
        
        if total_tickets == 0:
            logger.info("No tickets found that need processing!")
            logger.info("All tickets appear to have been processed by LLM already.")
            return
        
        logger.info(f"Found {total_tickets} tickets that need LLM processing")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_tickets)} tickets")
        progress_logger.info(f"BATCH_START: total_tickets={total_tickets}")
        
    except Exception as e:
        logger.error(f"Error fetching ticket records: {e}")
        return
    
    # Process tickets in optimized batches
    total_updated = 0
    batch_updates = []
    
    try:
        # Process tickets in concurrent batches
        for i in range(0, total_tickets, BATCH_SIZE):
            if shutdown_flag.is_set():
                logger.info("Shutdown requested, stopping processing")
                break
            
            batch = ticket_records[i:i + BATCH_SIZE]
            batch_num = i//BATCH_SIZE + 1
            total_batches = (total_tickets + BATCH_SIZE - 1)//BATCH_SIZE
            logger.info(f"Processing batch {batch_num}/{total_batches} (tickets {i+1}-{min(i+BATCH_SIZE, total_tickets)})")
            
            # Process batch concurrently
            batch_tasks = []
            for ticket in batch:
                if not checkpoint_manager.is_processed(ticket['_id']):
                    task = process_single_ticket(ticket, total_tickets)
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
                                    checkpoint_manager.mark_processed(result['ticket_id'], success=True),
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
            processed_so_far = min(i + BATCH_SIZE, total_tickets)
            progress_pct = (processed_so_far / total_tickets) * 100
            logger.info(f"Overall Progress: {progress_pct:.1f}% ({processed_so_far}/{total_tickets})")
            
            # Update performance monitor with actual total
            await performance_monitor.log_progress(total_tickets)
            
            # Brief delay between batches to manage rate limits
            if i + BATCH_SIZE < total_tickets and not shutdown_flag.is_set():
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
            logger.info("Optimized ticket content generation complete!")
        
        logger.info(f"Final Results:")
        logger.info(f"  Total tickets updated: {total_updated}")
        logger.info(f"  Successful generations: {success_counter.value}")
        logger.info(f"  Failed generations: {failure_counter.value}")
        logger.info(f"  Success rate: {(success_counter.value/(success_counter.value + failure_counter.value))*100:.1f}%" if (success_counter.value + failure_counter.value) > 0 else "Success rate: N/A")
        
        # Performance summary
        total_time = time.time() - performance_monitor.start_time
        avg_time_per_ticket = total_time / success_counter.value if success_counter.value > 0 else 0
        logger.info(f"  Total processing time: {total_time/3600:.2f} hours")
        logger.info(f"  Average time per ticket: {avg_time_per_ticket:.1f} seconds")
        logger.info(f"  Processing rate: {success_counter.value/(total_time/3600):.0f} tickets/hour" if total_time > 0 else "Processing rate: N/A")
        
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
            'X-Title': 'EU Banking Ticket Generator'
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
        total_count = ticket_col.count_documents({})
        
        with_complete_fields = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "assigned_team_email": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_raised": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_status": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_from": {"$exists": True, "$ne": "", "$ne": None},
            "follow_up_required": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        urgent_tickets = ticket_col.count_documents({"urgency": True})
        without_complete_fields = total_count - with_complete_fields
        
        logger.info("Collection Statistics:")
        logger.info(f"  Total tickets: {total_count}")
        logger.info(f"  With complete fields: {with_complete_fields}")
        logger.info(f"  Without complete fields: {without_complete_fields}")
        logger.info(f"  Urgent tickets: {urgent_tickets} ({(urgent_tickets/total_count)*100:.1f}%)" if total_count > 0 else "  Urgent tickets: 0")
        logger.info(f"  Completion rate: {(with_complete_fields/total_count)*100:.1f}%" if total_count > 0 else "  Completion rate: 0%")
        
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

async def main():
    """Main async function"""
    logger.info("Optimized EU Banking Trouble Ticket Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{TICKET_COLLECTION}")
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
        await process_tickets_optimized()
        
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