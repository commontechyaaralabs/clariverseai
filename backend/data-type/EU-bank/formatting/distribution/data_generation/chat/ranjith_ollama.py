# EU Banking Chat Content Generator - Ollama Version (Modified for chat_new)
import os
import random
import time
import json
import requests
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
MAIN_LOG_FILE = LOG_DIR / f"chat_generator_ollama_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"progress_{timestamp}.log"
CHECKPOINT_FILE = LOG_DIR / f"checkpoint_{timestamp}.json"
INTERMEDIATE_RESULTS_FILE = LOG_DIR / f"intermediate_results_{timestamp}.json"

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

# Configuration values for Ollama
OLLAMA_MODEL = "gemma3:27b"
BATCH_SIZE = 3
MAX_WORKERS = multiprocessing.cpu_count()  # Use all available CPU cores
REQUEST_TIMEOUT = 300  # 5 minutes for slow responses
MAX_RETRIES = 5
RETRY_DELAY = 3
BATCH_DELAY = 2.0
API_CALL_DELAY = 0.5
CHECKPOINT_SAVE_INTERVAL = 5  # Very frequent checkpoints
MAX_RETRY_ATTEMPTS = 3  # Maximum retry attempts for failed records

# Ollama setup
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "7eb2c60fcd3740cea657c8d109ff9016af894d2a2c112954bc3aff033c117736")
OLLAMA_URL = "http://34.147.17.26:16637/api/chat"

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
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

# Thread-safe counters with logging
class LoggingCounter:
    def __init__(self, name):
        self._value = 0
        self._lock = threading.Lock()
        self._name = name
    
    def increment(self):
        with self._lock:
            self._value += 1
            # Reduce logging frequency for performance
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
retry_counter = LoggingCounter("RETRY_COUNT")

# Failed records tracking
failed_records = []  # Store failed records for retry

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
        self._lock = threading.Lock()
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
    
    def save_checkpoint(self):
        with self._lock:
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
    
    def increment_retry(self, chat_id):
        """Track retry attempts for a chat"""
        with self._lock:
            chat_id_str = str(chat_id)
            if chat_id_str not in self.retry_attempts:
                self.retry_attempts[chat_id_str] = 0
            self.retry_attempts[chat_id_str] += 1
            self.stats['retry_count'] += 1
            return self.retry_attempts[chat_id_str]
    
    def get_retry_count(self, chat_id):
        """Get number of retry attempts for a chat"""
        return self.retry_attempts.get(str(chat_id), 0)
    
    def mark_processed(self, chat_id, success=True):
        with self._lock:
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
            
            # Auto-save checkpoint
            if self.stats['processed_count'] % CHECKPOINT_SAVE_INTERVAL == 0:
                self.save_checkpoint()

checkpoint_manager = CheckpointManager(CHECKPOINT_FILE)

# Intermediate Results Manager
class IntermediateResultsManager:
    """Manages saving and loading of intermediate results"""
    
    def __init__(self, filename):
        self.filename = filename
        self.results = []
        self._lock = threading.Lock()
        self.load_existing_results()
    
    def load_existing_results(self):
        """Load existing intermediate results if file exists"""
        try:
            if self.filename.exists():
                with open(self.filename, 'r') as f:
                    self.results = json.load(f)
                logger.info(f"Loaded {len(self.results)} existing intermediate results")
        except Exception as e:
            logger.error(f"Error loading intermediate results: {e}")
            self.results = []
    
    def add_result(self, result):
        """Add a result to intermediate storage"""
        with self._lock:
            result['timestamp'] = datetime.now().isoformat()
            self.results.append(result)
            self.save_to_file()
    
    def save_to_file(self):
        """Save current results to file"""
        try:
            with open(self.filename, 'w') as f:
                json.dump(self.results, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving intermediate results: {e}")
    
    def get_pending_updates(self):
        """Get results that haven't been saved to database yet"""
        return [r for r in self.results if not r.get('saved_to_db', False)]
    
    def mark_as_saved(self, chat_ids):
        """Mark results as saved to database"""
        with self._lock:
            for result in self.results:
                if result.get('chat_id') in chat_ids:
                    result['saved_to_db'] = True
            self.save_to_file()

# Initialize intermediate results manager
results_manager = IntermediateResultsManager(INTERMEDIATE_RESULTS_FILE)

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
- **External (Bank → Customer):** "Thanks for your patience! I've reset your login You should be able to access it now." ← BANK (emoji for completion)
- **Internal (Employee → Employee):** "Hey Sarah, just got a call from Maria Garcia about her loan application. She's asking about the status 😅" ← EMPLOYEES discussing customer's case (emoji for stress)
- **Internal (Employee → Employee):** "Oh really? What's the issue with Maria's application? I thought we processed it yesterday" ← COLLEAGUES chatting (no emoji - straightforward)
- **Internal (Employee → Employee):** "Can you help me with the Johnson account? They're asking about wire transfer limits 🤔" ← STAFF helping each other (emoji for uncertainty)
- **Internal (Employee → Employee):** "Found it! I'll send you the file now" ← COLLEAGUES (emoji for file reference)
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

def generate_chat_content(chat_data):
    """Generate chat content and analysis with Ollama"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    chat_id = chat_data.get('_id', 'unknown')
    
    try:
        prompt = generate_optimized_chat_prompt(chat_data)
        
        response = call_ollama_with_backoff(prompt)
        
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

def process_single_chat_update(chat_record, retry_attempt=0):
    """Process a single chat record to generate content and analysis"""
    if shutdown_flag.is_set():
        return None
        
    # Extract chat_id for logging
    chat_id = str(chat_record.get('_id', 'unknown'))
        
    try:
        # Generate chat content based on existing data
        chat_content = generate_chat_content(chat_record)
        
        if not chat_content:
            if retry_attempt < MAX_RETRY_ATTEMPTS:
                logger.warning(f"Generation failed for {chat_id}, will retry (attempt {retry_attempt + 1}/{MAX_RETRY_ATTEMPTS})")
                return None  # Will be retried
            else:
                failure_counter.increment()
                logger.error(f"Final failure for {chat_id} after {MAX_RETRY_ATTEMPTS} attempts")
                return None
        
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
                else:
                    update_doc['follow_up_date'] = None
                
                # Handle next_action_suggestion based on follow_up_required and action_pending_status
                if chat_follow_up_required == "yes" and chat_action_pending_status == "yes":
                    update_doc['next_action_suggestion'] = analysis.get('next_action_suggestion')
                else:
                    update_doc['next_action_suggestion'] = None
        
        # Ensure follow_up_date field exists in database (create with null if not exists)
        if 'follow_up_date' not in update_doc and 'follow_up_date' not in chat_record:
            update_doc['follow_up_date'] = None
        
        # Add LLM processing tracking
        update_doc['llm_processed'] = True
        update_doc['llm_processed_at'] = datetime.now().isoformat()
        update_doc['llm_model_used'] = OLLAMA_MODEL
        
        success_counter.increment()
        checkpoint_manager.mark_processed(chat_id, success=True)
        
        # Create intermediate result
        intermediate_result = {
            'chat_id': str(chat_id),
            'update_doc': update_doc,
            'original_data': {
                'dominant_topic': chat_record.get('dominant_topic'),
                'priority': chat_record.get('priority')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'chat_id': str(chat_id),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {chat_id}: {str(e)[:100]}")
        if retry_attempt < MAX_RETRY_ATTEMPTS:
            logger.warning(f"Will retry {chat_id} due to error (attempt {retry_attempt + 1}/{MAX_RETRY_ATTEMPTS})")
            checkpoint_manager.increment_retry(chat_id)
            return None  # Will be retried
        else:
            failure_counter.increment()
            checkpoint_manager.mark_processed(chat_id, success=False)
            logger.error(f"Final failure for {chat_id} after {MAX_RETRY_ATTEMPTS} attempts")
            return None

def retry_failed_records(failed_records_list):
    """Retry processing failed records"""
    if not failed_records_list:
        return []
    
    logger.info(f"Retrying {len(failed_records_list)} failed records...")
    retry_counter.increment()
    
    successful_retries = []
    
    for record_data in failed_records_list:
        if shutdown_flag.is_set():
            break
            
        chat_record = record_data['record']
        retry_attempt = record_data.get('retry_attempt', 0) + 1
        
        logger.info(f"Retrying {chat_record.get('_id', 'unknown')} (attempt {retry_attempt}/{MAX_RETRY_ATTEMPTS})")
        
        # Add delay before retry
        time.sleep(RETRY_DELAY)
        
        result = process_single_chat_update(chat_record, retry_attempt)
        
        if result:
            successful_retries.append(result)
            logger.info(f"Retry successful for {chat_record.get('_id', 'unknown')}")
        else:
            if retry_attempt < MAX_RETRY_ATTEMPTS:
                # Add back to failed records for another retry
                failed_records.append({
                    'record': chat_record,
                    'retry_attempt': retry_attempt
                })
            else:
                logger.error(f"Final retry failure for {chat_record.get('_id', 'unknown')}")
    
    return successful_retries

def save_batch_to_database(batch_updates):
    """Save a batch of updates to the database using proper bulk write operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create proper UpdateOne operations
        bulk_operations = []
        chat_ids = []
        
        for update_data in batch_updates:
            # Create UpdateOne operation properly
            operation = UpdateOne(
                filter={"_id": ObjectId(update_data['chat_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            chat_ids.append(update_data['chat_id'])
        
        # Execute bulk write with proper error handling
        if bulk_operations:
            try:
                result = chat_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Mark intermediate results as saved
                results_manager.mark_as_saved(chat_ids)
                
                # Update counter
                update_counter._value += updated_count
                
                logger.info(f"Successfully saved {updated_count} records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved, total_updates: {update_counter.value}")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Try individual updates as fallback
                logger.info("Attempting individual updates as fallback...")
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
                
                if individual_success > 0:
                    results_manager.mark_as_saved([up['chat_id'] for up in batch_updates[:individual_success]])
                    update_counter._value += individual_success
                    logger.info(f"Fallback: {individual_success} records saved individually")
                
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def update_chats_with_content_parallel():
    """Update existing chats with generated content and analysis using optimized batch processing"""
    
    logger.info("Starting EU Banking Chat Content Generation (Ollama)...")
    logger.info(f"System Info: {CPU_COUNT} CPU cores detected")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Request timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"Max retries per request: {MAX_RETRIES}")
    
    # Test Ollama connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        return
    
    # Get all chat records that need content generation
    logger.info("Fetching chat records from database...")
    try:
        # Query for chats that have null/empty message content
        query = {
            "$and": [
                # Must have basic chat structure
                {"_id": {"$exists": True}},
                # Must have messages array
                {"messages": {"$exists": True, "$ne": None, "$ne": []}},
                # Must have at least one message with null/empty body content
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
        
        # Exclude already processed chats from checkpoint
        if checkpoint_manager.processed_chats:
            processed_ids = [ObjectId(cid) for cid in checkpoint_manager.processed_chats if ObjectId.is_valid(cid)]
            if processed_ids:
                if "_id" in query:
                    if isinstance(query["_id"], dict):
                        query["_id"]["$nin"] = processed_ids
                    else:
                        query = {"$and": [query, {"_id": {"$nin": processed_ids}}]}
                else:
                    query["_id"] = {"$nin": processed_ids}
        
        chat_records = list(chat_col.find(query))
        total_chats = len(chat_records)
        
        if total_chats == 0:
            logger.info("No chats found that need processing!")
            logger.info("All chats appear to have been processed already.")
            return
            
        logger.info(f"Found {total_chats} chats needing content generation")
        logger.info(f"Previously processed (checkpoint): {len(checkpoint_manager.processed_chats)} chats")
        progress_logger.info(f"BATCH_START: total_chats={total_chats}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching chat records: {e}")
        return
    
    # Process in batches
    total_batches = (total_chats + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []
    
    logger.info(f"Processing in {total_batches} batches of {BATCH_SIZE} chats each")
    logger.info(f"Using {MAX_WORKERS} workers for parallel processing")
    
    try:
        for batch_num in range(1, total_batches + 1):
            if shutdown_flag.is_set():
                logger.info(f"Shutdown requested. Stopping at batch {batch_num-1}/{total_batches}")
                break
                
            batch_start = (batch_num - 1) * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_chats)
            batch_records = chat_records[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_records)} chats)...")
            progress_logger.info(f"BATCH_START: batch={batch_num}/{total_batches}, records={len(batch_records)}")
            
            # Process batch with optimized parallelization
            successful_updates = []
            batch_start_time = time.time()
            
            # Use ThreadPoolExecutor for I/O bound operations (API calls)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks for this batch
                futures = {
                    executor.submit(process_single_chat_update, record): record 
                    for record in batch_records
                }
                
                # Collect results with progress tracking
                completed = 0
                batch_failed_records = []
                
                try:
                    for future in as_completed(futures, timeout=REQUEST_TIMEOUT * 3):
                        if shutdown_flag.is_set():
                            logger.warning("Cancelling remaining tasks...")
                            for f in futures:
                                f.cancel()
                            break
                            
                        try:
                            result = future.result(timeout=60)
                            completed += 1
                            
                            if result:
                                successful_updates.append(result)
                                # Save immediately when we have enough for a batch
                                if len(successful_updates) >= BATCH_SIZE:
                                    saved_count = save_batch_to_database(successful_updates)
                                    total_updated += saved_count
                                    successful_updates = []
                                    logger.info(f"Immediate save: {saved_count} records")
                            
                            # Progress indicator
                            progress = (completed / len(batch_records)) * 100
                            logger.info(f"Task completed: {progress:.1f}% ({completed}/{len(batch_records)})")
                                
                        except Exception as e:
                            logger.error(f"Error processing future result: {e}")
                            completed += 1
                            
                except Exception as e:
                    logger.error(f"Error collecting batch results: {e}")
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # Save any remaining updates from this batch
            if successful_updates and not shutdown_flag.is_set():
                saved_count = save_batch_to_database(successful_updates)
                total_updated += saved_count
                logger.info(f"Final batch save: {saved_count} records")
            
            # Retry failed records from this batch
            if failed_records and not shutdown_flag.is_set():
                logger.info(f"Retrying {len(failed_records)} failed records from batch {batch_num}")
                retry_successful = retry_failed_records(failed_records.copy())
                
                if retry_successful:
                    retry_saved_count = save_batch_to_database(retry_successful)
                    total_updated += retry_saved_count
                    logger.info(f"Retry save: {retry_saved_count} records")
                
                failed_records.clear()
            
            logger.info(f"Batch {batch_num} processing complete: {len(successful_updates)}/{len(batch_records)} successful")
            logger.info(f"Batch duration: {batch_duration:.2f}s")
            progress_logger.info(f"BATCH_COMPLETE: batch={batch_num}, successful={len(successful_updates)}, duration={batch_duration:.2f}s")
            
            # Progress summary every 3 batches
            if batch_num % 3 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_chats) * 100
                logger.info(f"Overall Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value} | Retries: {retry_counter.value} | DB Updates: {total_updated}")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, success={success_counter.value}, failures={failure_counter.value}, retries={retry_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches
            if not shutdown_flag.is_set() and batch_num < total_batches:
                time.sleep(BATCH_DELAY)
        
        # Final retry phase for any remaining failed records
        if failed_records and not shutdown_flag.is_set():
            logger.info(f"Final retry phase: {len(failed_records)} records remaining")
            final_retry_successful = retry_failed_records(failed_records.copy())
            
            if final_retry_successful:
                final_retry_saved = save_batch_to_database(final_retry_successful)
                total_updated += final_retry_saved
                logger.info(f"Final retry save: {final_retry_saved} records")
            
            failed_records.clear()
        
        # Save final checkpoint
        checkpoint_manager.save_checkpoint()
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking chat content generation complete!")
            
        logger.info(f"Total chats updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        logger.info(f"Retry attempts: {retry_counter.value}")
        logger.info(f"Data updated in MongoDB: {DB_NAME}.{CHAT_COLLECTION}")
        
        # Save list of permanently failed chats
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
                
                logger.warning(f"ATTENTION: {len(checkpoint_manager.failed_chats)} chats could not be processed")
                logger.warning(f"Failed chat IDs saved to: {failed_chats_file}")
            except Exception as e:
                logger.error(f"Could not save failed chats list: {e}")
        
        # Final progress log
        progress_logger.info(f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        shutdown_flag.set()

def test_ollama_connection():
    """Test if Ollama is accessible and model is available"""
    try:
        logger.info(f"Testing connection to Ollama: {OLLAMA_URL}")
        
        headers = {
            'Authorization': f'Bearer {OLLAMA_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        # Test basic connection with simple generation
        logger.info("Testing simple generation...")
        
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
        
        logger.info(f"Generation test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from generation endpoint")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "message" in result and "content" in result["message"]:
                logger.info("Ollama connection test successful")
                logger.info(f"Test response: {result['message']['content'][:100]}...")
                return True
            else:
                logger.error(f"No 'message.content' field in test. Fields: {list(result.keys())}")
                return False
        except json.JSONDecodeError as e:
            logger.error(f"Generation test returned invalid JSON: {e}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection test failed: {e}")
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
        
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_analysis={with_complete_analysis}, without_analysis={without_complete_analysis}")
            
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

def recover_from_intermediate_results():
    """Recover and process any pending intermediate results"""
    try:
        pending_results = results_manager.get_pending_updates()
        
        if not pending_results:
            logger.info("No pending intermediate results to recover")
            return 0
        
        logger.info(f"Recovering {len(pending_results)} pending intermediate results...")
        
        # Convert to database update format
        batch_updates = []
        for result in pending_results:
            if 'chat_id' in result and 'update_doc' in result:
                batch_updates.append({
                    'chat_id': result['chat_id'],
                    'update_doc': result['update_doc']
                })
        
        if batch_updates:
            # Process in batches
            total_recovered = 0
            for i in range(0, len(batch_updates), BATCH_SIZE):
                batch = batch_updates[i:i + BATCH_SIZE]
                saved_count = save_batch_to_database(batch)
                total_recovered += saved_count
                logger.info(f"Recovered batch: {saved_count} records")
            
            logger.info(f"Successfully recovered {total_recovered} records from intermediate results")
            return total_recovered
        
        return 0
        
    except Exception as e:
        logger.error(f"Error recovering intermediate results: {e}")
        return 0

# Main execution function
def main():
    """Main function to initialize and run the chat content generator"""
    logger.info("EU Banking Chat Content Generator Starting (Ollama)...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {CHAT_COLLECTION}")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_URL}")
    logger.info(f"Max Workers: {MAX_WORKERS}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    logger.info(f"Log Directory: {LOG_DIR}")
    
    # Setup signal handlers and cleanup
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    # Initialize database
    if not init_database():
        logger.error("Cannot proceed without database connection")
        return
    
    try:
        # Show current collection stats
        get_collection_stats()
        
        # Try to recover any pending intermediate results first
        recovered_count = recover_from_intermediate_results()
        if recovered_count > 0:
            logger.info(f"Recovered {recovered_count} records from previous session")
        
        # Run the chat content generation
        update_chats_with_content_parallel()
        
        # Show final statistics
        get_collection_stats()
        
        # Show sample generated content
        get_sample_generated_chats()
        
    except KeyboardInterrupt:
        logger.info("Content generation interrupted by user!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        # Save final intermediate results and checkpoint
        results_manager.save_to_file()
        checkpoint_manager.save_checkpoint()
        cleanup_resources()
        
        logger.info("Session complete. Check log files for detailed information:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")
        logger.info(f"Progress Log: {PROGRESS_LOG_FILE}")
        logger.info(f"Checkpoint: {CHECKPOINT_FILE}")
        logger.info(f"Intermediate Results: {INTERMEDIATE_RESULTS_FILE}")

# Run the content generator
if __name__ == "__main__":
    main()

