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

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
client = None
db = None
chat_col = None

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Configuration values - Ultra-conservative for Ollama
OLLAMA_MODEL = "gemma3:27b"
BATCH_SIZE = 1  # Process only 1 chat at a time
MAX_WORKERS = 1  # Single worker to avoid rate limits
REQUEST_TIMEOUT = 720  # Increased timeout to 12 minutes
MAX_RETRIES = 3  # Fewer retries to avoid long waits
RETRY_DELAY = 30  # 30 second retry delay for rate limits
BATCH_DELAY = 10.0  # 10 second delay between batches
API_CALL_DELAY = 5.0  # 5 second delay between API calls

# Ollama setup
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "fbedba0c1d929cbd26609896a3c9c186a1edc55e753ccc16d4e44de403c297ef")
OLLAMA_URL = "http://34.147.17.26:20855/api/chat"
# Retry configuration
MAX_RETRY_ATTEMPTS = 3  # Maximum retry attempts for failed records
RETRY_DELAY_SECONDS = 5  # Delay between retry attempts

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
    
    def add_batch_results(self, results_batch):
        """Add multiple results to intermediate storage"""
        with self._lock:
            for result in results_batch:
                result['timestamp'] = datetime.now().isoformat()
            self.results.extend(results_batch)
            self.save_to_file()
            logger.info(f"Added {len(results_batch)} results to intermediate storage")
    
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
    max_time=600,  # Increased max time for rate limiting and longer processing
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
        # Add delay to help with rate limiting
        time.sleep(API_CALL_DELAY)
        
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
    """Generate optimized prompt for chat content and analysis generation - EXACT REPLICA from v2openrouter.py"""
    
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
    overall_sentiment = chat_data.get('overall_sentiment') or chat_data.get('sentiment')
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
    
    # Build sentiment description dynamically with specific tone guidance
    sentiment_desc = ""
    if overall_sentiment is not None:
        if overall_sentiment == 5:
            sentiment_desc = "Frustrated (extreme frustration, very upset) - Use strong emotional language, complaints, exclamations, urgent demands"
        elif overall_sentiment == 4:
            sentiment_desc = "Anger (clear frustration or anger) - Use frustrated language, raised concerns, demanding tone, clear dissatisfaction"
        elif overall_sentiment == 3:
            sentiment_desc = "Moderately Concerned (growing unease or worry) - Use worried language, concerned questions, seeking reassurance"
        elif overall_sentiment == 2:
            sentiment_desc = "Bit Irritated (slight annoyance or impatience) - Use slightly impatient language, mild complaints, subtle frustration"
        elif overall_sentiment == 1:
            sentiment_desc = "Calm (baseline for professional communication) - Use professional, calm, and polite language"
        else:
            sentiment_desc = "Positive satisfied communication - Use positive, satisfied, and appreciative language"
    
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
        
        # Determine conversation context based on category
        if category == 'external':
            if i == 0:
                # First message from customer to bank
                message_instructions.append(f'{{"content": "Realistic chat message 10-100 words from customer to bank. Customer is reaching out about banking issue. Use natural language, contractions, informal tone, emojis when appropriate. Base content on {dominant_topic if dominant_topic else "banking business"}. Sound like a real customer with genuine concerns. Include realistic details, questions, and natural flow. Vary length: short responses (10-30 words) for quick replies, medium (40-70 words) for explanations, longer (80-100 words) for detailed discussions. When mentioning client names or issues, use realistic names like John Smith, Maria Garcia, etc. - NOT participant names.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
            else:
                # Bank employee response
                message_instructions.append(f'{{"content": "Realistic chat message 10-100 words from bank employee to customer. Professional but friendly response. Use natural language, contractions, informal tone, emojis when appropriate. Base content on {dominant_topic if dominant_topic else "banking business"}. Sound like helpful bank staff. Include realistic details, solutions, and natural conversation flow. Vary length: short responses (10-30 words) for quick replies, medium (40-70 words) for explanations, longer (80-100 words) for detailed discussions. When mentioning client names or issues, use realistic names like John Smith, Maria Garcia, etc. - NOT participant names.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
        else:
            # Internal conversation between bank employees
            if i == 0:
                message_instructions.append(f'{{"content": "Realistic chat message 10-100 words from bank employee to colleague. Internal discussion about banking operations. Use natural language, contractions, informal tone, emojis when appropriate. Base content on {dominant_topic if dominant_topic else "banking business"}. Sound like colleagues discussing work. Include realistic details, questions, reactions, and natural flow. Vary length: short responses (10-30 words) for quick replies, medium (40-70 words) for explanations, longer (80-100 words) for detailed discussions. When mentioning client names or issues, use realistic names like John Smith, Maria Garcia, etc. - NOT participant names.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
            else:
                message_instructions.append(f'{{"content": "Realistic chat message 10-100 words from bank employee to colleague. Continue internal discussion naturally. Use natural language, contractions, informal tone, emojis when appropriate. Base content on {dominant_topic if dominant_topic else "banking business"}. Sound like colleagues having a real conversation. Include realistic details, emotions, and natural conversation flow. Vary length: short responses (10-30 words) for quick replies, medium (40-70 words) for explanations, longer (80-100 words) for detailed discussions. When mentioning client names or issues, use realistic names like John Smith, Maria Garcia, etc. - NOT participant names.{day_shift_instruction}", "from_user": "{user_name}", "timestamp": "{message_date}"}}')
    
    messages_json = ",\n  ".join(message_instructions)
    
    # No sentiment generation needed - use existing overall_sentiment
    
    # Determine conversation type and ending requirements
    conversation_type = "EXTERNAL (Customer ↔ Bank)" if category == 'external' else "INTERNAL (Bank Employee ↔ Bank Employee)"
    
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

**ACTION PENDING CONTEXT:** {action_pending_context}

**PARTICIPANTS:** {participant_str}

**RULES:** 
- Sentiment {overall_sentiment}/5: {"Extreme frustration throughout ALL messages" if overall_sentiment == 5 else "Clear anger/frustration" if overall_sentiment == 4 else "Moderate concern/unease" if overall_sentiment == 3 else "Slight irritation/impatience" if overall_sentiment == 2 else "Calm professional baseline" if overall_sentiment == 1 else "Positive satisfied communication"}
- Bank employees: ALWAYS calm, professional, helpful
- Follow-up {follow_up_required}: {"End with open-ended scenarios" if follow_up_required == "yes" else "End with complete resolution"}
- Action {action_pending_status}: {"Show waiting scenarios" if action_pending_status == "yes" else "Show completed processes"}
- Action Pending From {action_pending_from}: {action_pending_from_desc}

**CONVERSATION STRUCTURE:**
- External: First message from customer, then bank employee responses
- Internal: All messages between bank employees discussing work
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
- Use emojis appropriately (😅, 👍, 🤔, etc.)
- Ask follow-up questions naturally
- Show emotions and personality
- Use informal language while staying professional
- Include realistic banking scenarios and problems

**SENTIMENT-BASED TONE INSTRUCTIONS:**
- Sentiment 1 (Calm): Professional, calm, polite language - "I would like to inquire about...", "Could you please help me with...", "Thank you for your assistance"
- Sentiment 2 (Bit Irritated): Slightly impatient, mild complaints - "I've been waiting for...", "This is taking longer than expected", "I'm a bit concerned about..."
- Sentiment 3 (Moderately Concerned): Worried language, seeking reassurance - "I'm worried about...", "Can you confirm that...", "This doesn't seem right to me"
- Sentiment 4 (Anger): Frustrated language, demanding tone - "This is unacceptable!", "I need this resolved immediately", "I'm very frustrated with..."
- Sentiment 5 (Frustrated): Extreme frustration, strong emotional language - "This is ridiculous!", "I'm extremely upset!", "I demand immediate action!", "This is the worst service!"

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

**EXAMPLES OF GOOD CHAT MESSAGES:**
- External: "Hi, I'm having trouble with my online banking. Can someone help me check my account balance?"
- External: "Of course! I can help you with that. Let me look up your account details right away."
- Internal: "Hey Sarah, just got a call about that loan application. The customer's asking about the status 😅"
- Internal: "Oh really? What's the issue? I thought we processed it yesterday"
- Day shift: "Good morning! Following up on our discussion about the Smith application..."
- Day shift: "Hey, got an update on that compliance issue we were working on"
- Same day gap: "Quick follow-up - did you hear back from the client?"
- Same day gap: "Just checking in on the status of that transaction"

**OUTPUT:** {{
  "messages": [
    {messages_json}
  ],
  "analysis": {{
    "chat_summary": "Business summary 150-200 words describing discussion topic, participants, key points, and context",
    "follow_up_reason": {"[WHY follow-up is needed - the trigger/justification. Focus on the REASON/CAUSE that necessitates follow-up. Provide detailed explanation with specific context, background information, and comprehensive reasoning. Examples: 'Customer requested status update on loan application submitted last week', 'Documentation incomplete - missing income verification and employment history', 'Compliance deadline approaching for regulatory audit requiring immediate attention', 'Issue unresolved after multiple escalation attempts and customer dissatisfaction', 'Waiting for external approval from regulatory body with pending timeline', 'System error occurred during transaction processing causing customer inconvenience', 'Client response required for account verification process', 'Regulatory requirement pending with specific deadline constraints'. Be specific about what triggered the need for follow-up with full context and background.]" if follow_up_required == "yes" else "null"},
    "next_action_suggestion": {"[WHAT specific step to take - the actionable recommendation. Focus on the CONCRETE ACTION to be performed with detailed steps, timeline, and responsible parties. Examples: 'Contact client to request missing documents including bank statements and employment verification within 48 hours', 'Schedule compliance review meeting with senior management and legal team for next business day', 'Escalate to senior management with detailed case summary and customer impact assessment', 'Update system with new information and coordinate with IT team for immediate resolution', 'Send follow-up email to customer with status update and next steps timeline', 'Review and approve pending application with complete documentation verification', 'Coordinate with IT team for system resolution and customer communication strategy', 'Prepare comprehensive documentation for audit including all supporting materials'. Be specific about what needs to be done with detailed implementation steps.]" if follow_up_required == "yes" and action_pending_status == "yes" else "null"},
    "follow_up_date": {"[Generate meaningful follow-up date based on last message date and issue type. Consider urgency, business days, and typical resolution times for the topic. For urgent issues (P1-Critical), suggest 1-2 business days. For high priority (P2-High), suggest 3-5 business days. For medium priority (P3-Medium), suggest 1-2 weeks. Format: YYYY-MM-DDTHH:MM:SSZ]" if follow_up_required == "yes" else "null"}
  }}
}}

Use EXACT metadata values and EXACT dates provided. Implement concepts through natural scenarios, NOT explicit mentions. Generate authentic banking content with specific details.

**CRITICAL:** 
- **SENTIMENT-BASED TONE**: ALL messages must reflect the exact sentiment level (1-5) with appropriate emotional language and tone
- Follow-up reason = "WHY" (the trigger/justification for follow-up) - ONLY if follow_up_required="yes", otherwise "null" 
- Next-action suggestion = "WHAT" (the step you advise taking) - ONLY generate if follow_up_required="yes" AND action_pending_status="yes":
  * If action_pending_from="Customer": Suggest what the customer needs to do with specific details and timeline
  * If action_pending_from="Bank": Suggest what the bank needs to do with specific details and timeline
  * If both follow_up_required="no" AND action_pending_status="no": Set to "null"
- Follow-up date = "WHEN" (meaningful date based on last message and issue type) - ONLY if follow_up_required="yes", otherwise "null"
- Chat summary should reflect the conversation type (external vs internal) and include all relevant context
- All analysis fields should be meaningful and based on the actual conversation content
- Use the EXACT dates provided for message timestamps - do not generate new dates

**IMPORTANT MESSAGE GENERATION RULES:**
- Generate EXACTLY {message_count} messages - no more, no less
- Each message MUST have actual conversational content (10-100 words)
- Content should be realistic banking conversation text
- Do NOT generate empty, null, or placeholder content
- Each message should sound like a real person talking
- Use "content" field name (NOT "text") for message content in JSON structure
- ALL {message_count} messages must be included in the output array

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
        logger.info(f"Chat {chat_id}: Starting prompt generation...")
        prompt = generate_optimized_chat_prompt(chat_data)
        logger.info(f"Chat {chat_id}: Prompt generated successfully, length: {len(prompt)}")
        
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
            # Debug logging for message content
            if 'messages' in result and result['messages']:
                sample_message = result['messages'][0] if result['messages'] else {}
                logger.info(f"Chat {chat_id}: JSON parsing successful. Keys: {list(result.keys())}")
                logger.info(f"Chat {chat_id}: Sample message structure: {sample_message}")
                
                # Check for both 'content' and 'text' fields
                if 'content' in sample_message:
                    logger.info(f"Chat {chat_id}: Sample message has 'content' field, length: {len(str(sample_message['content']))}")
                elif 'text' in sample_message:
                    logger.info(f"Chat {chat_id}: Sample message has 'text' field, length: {len(str(sample_message['text']))}")
                else:
                    logger.warning(f"Chat {chat_id}: Sample message missing both 'content' and 'text' fields!")
            else:
                logger.warning(f"Chat {chat_id}: No messages in LLM response!")
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
        logger.info(f"Chat {chat_id}: Expected {message_count} messages, got {len(result['messages'])} from LLM")
        
        if len(result['messages']) != message_count:
            logger.warning(f"Chat {chat_id}: Message count mismatch - expected {message_count}, got {len(result['messages'])}")
            
            # If LLM generated fewer messages, we need to handle this
            if len(result['messages']) < message_count:
                logger.warning(f"Chat {chat_id}: LLM generated only {len(result['messages'])} messages, need {message_count}")
                # For now, we'll process what we have and log the issue
            elif len(result['messages']) > message_count:
                logger.warning(f"Chat {chat_id}: LLM generated {len(result['messages'])} messages, truncating to {message_count}")
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
        
        # Validate message content and word counts for realism
        for i, message in enumerate(result.get('messages', [])):
            if isinstance(message, dict) and ('content' in message or 'text' in message):
                # Handle both 'content' and 'text' field names from LLM
                content = message.get('content') or message.get('text')
                
                # Check if content is empty or None
                if not content or content.strip() == "":
                    logger.error(f"Chat {chat_id}: Message {i} has empty content! Message structure: {message}")
                    # Generate a fallback message
                    fallback_content = f"This is message {i+1} in the conversation about {chat_data.get('dominant_topic', 'banking matters')}."
                    message['content'] = fallback_content
                    content = fallback_content
                
                word_count = len(content.split())
                if word_count < 10:
                    logger.warning(f"Chat {chat_id}: Message {i} too short ({word_count} words), expanding...")
                    # Add some context to make it more realistic
                    expanded_content = f"{content} Let me give you more details about this."
                    message['content'] = expanded_content
                elif word_count > 100:
                    logger.warning(f"Chat {chat_id}: Message {i} too long ({word_count} words), truncating...")
                    # Truncate to 100 words
                    words = content.split()
                    truncated_content = ' '.join(words[:100])
                    message['content'] = truncated_content
        
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
        success_logger.info(json.dumps(success_info))
        
        return result
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'chat_id': str(chat_id),
            'dominant_topic': chat_data.get('dominant_topic', 'Unknown'),
            'error': str(e)[:200],
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
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
                expected_message_count = len(chat_data.get('messages', []))
                logger.info(f"Chat {chat_id}: Processing {len(messages)} generated messages for {expected_message_count} expected messages")
                
                for i, message in enumerate(messages):
                    if isinstance(message, dict):
                        # Handle both 'content' and 'text' field names from LLM
                        content = message.get('content') or message.get('text')
                        timestamp = message.get('timestamp')
                        
                        # Debug logging for message content
                        if not content or content.strip() == "":
                            logger.warning(f"Chat {chat_id}: Message {i} has empty content. Message structure: {message}")
                        else:
                            logger.info(f"Chat {chat_id}: Message {i} content length: {len(content)} chars")
                        
                        update_doc[f'messages.{i}.body.content'] = content
                        update_doc[f'messages.{i}.createdDateTime'] = timestamp
                
                # Log if we're missing messages
                if len(messages) < expected_message_count:
                    missing_count = expected_message_count - len(messages)
                    logger.warning(f"Chat {chat_id}: Missing {missing_count} messages - only updating first {len(messages)} messages")
                    
                    # For missing messages, set content to null to avoid database errors
                    for i in range(len(messages), expected_message_count):
                        update_doc[f'messages.{i}.body.content'] = None
                        logger.warning(f"Chat {chat_id}: Setting message {i} content to null (missing from LLM response)")
        
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
        update_doc['llm_model_used'] = OLLAMA_MODEL
        
        # Debug: Log what's being saved to database
        logger.info(f"Chat {chat_id}: Update document keys: {list(update_doc.keys())}")
        if 'follow_up_date' in update_doc:
            logger.info(f"Chat {chat_id}: follow_up_date in update_doc: {update_doc['follow_up_date']}")
        else:
            logger.warning(f"Chat {chat_id}: follow_up_date NOT in update_doc")
        
        success_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'chat_id': chat_id,
            'update_doc': update_doc,
            'original_data': {
                'dominant_topic': chat_record.get('dominant_topic'),
                'subtopics': chat_record.get('subtopics', '')[:100] + '...' if len(str(chat_record.get('subtopics', ''))) > 100 else chat_record.get('subtopics', '')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'chat_id': chat_id,
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {chat_id}: {str(e)[:100]}")
        if retry_attempt < MAX_RETRY_ATTEMPTS:
            logger.warning(f"Will retry {chat_id} due to error (attempt {retry_attempt + 1}/{MAX_RETRY_ATTEMPTS})")
            return None  # Will be retried
        else:
            failure_counter.increment()
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
        time.sleep(RETRY_DELAY_SECONDS)
        
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
                
                # Log some details of what was saved
                if updated_count > 0:
                    sample_update = batch_updates[0]
                    logger.info(f"Sample update - Chat ID: {sample_update['chat_id']}")
                
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

def process_chats_optimized():
    """Main optimized processing function for chat generation - regenerates BOTH message content AND analysis fields for records with null/empty message content"""
    logger.info("Starting Optimized EU Banking Chat Content Generation...")
    logger.info("Focus: Processing records with NULL/empty message content")
    logger.info("Action: Will regenerate BOTH message content AND analysis fields")
    logger.info(f"Collection: {CHAT_COLLECTION}")
    logger.info(f"Optimized Configuration:")
    logger.info(f"  Max Workers: {MAX_WORKERS}")
    logger.info(f"  Batch Size: {BATCH_SIZE}")
    logger.info(f"  API Delay: {API_CALL_DELAY}s")
    logger.info(f"  Request Timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"  Model: {OLLAMA_MODEL}")
    
    # Test connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
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
                      "follow_up_date", "overall_sentiment", "sentiment", "subtopics", "category"]:
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
        # Process chats in batches
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
            
            # Process batch with single worker
            batch_tasks = []
            for chat in batch:
                chat_id = str(chat.get('_id'))
                task = process_single_chat_update(chat)
                batch_tasks.append(task)
            
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
                    task_timeout = REQUEST_TIMEOUT + 30  # 12.5 minutes per task
                    logger.info(f"Starting single task with {task_timeout}s timeout")
                    
                    result = task  # Direct call since it's not async
                    
                    if result:
                        successful_results.append(result)
                        logger.info(f"Single task completed successfully")
                    else:
                        failed_count += 1
                        logger.warning(f"Single task returned no result")
                        
                except Exception as e:
                    failed_count += 1
                    error_msg = str(e).lower() if e else "unknown error"
                    if "rate limit" in error_msg or "429" in error_msg:
                        logger.warning(f"Rate limit detected, pausing for 30 seconds...")
                        time.sleep(30)  # 30 second pause for rate limits
                    logger.error(f"Single task failed with error: {e}")
                
                if successful_results:
                    batch_updates.extend(successful_results)
                
                batch_elapsed = time.time() - batch_start_time
                logger.info(f"Batch {batch_num} completed in {batch_elapsed:.1f}s: {len(successful_results)}/1 successful, {failed_count} failed")
            
            # Save to database when we have enough updates
            if len(batch_updates) >= BATCH_SIZE:
                saved_count = save_batch_to_database(batch_updates)
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
            
            # Longer delay between batches to manage rate limits
            if processed_count < total_chats and not shutdown_flag.is_set():
                logger.info(f"Waiting {BATCH_DELAY}s before next batch to avoid rate limits...")
                time.sleep(BATCH_DELAY)
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
        
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
        
        progress_logger.info(f"FINAL_SUMMARY: session_updated={total_updated}, total_completed={final_total_completed}, pending={final_pending}, completion_rate={final_completion_percentage:.1f}%, success={success_counter.value}, failures={failure_counter.value}")
        
    except Exception as e:
        logger.error(f"Unexpected error in main processing: {e}")
        import traceback
        logger.error(traceback.format_exc())

def test_ollama_connection():
    """Test Ollama connection with simple generation"""
    try:
        logger.info("Testing Ollama connection...")
        
        headers = {
            'Authorization': f'Bearer {OLLAMA_API_KEY}',
            'Content-Type': 'application/json'
        }
        
        test_payload = {
            "model": OLLAMA_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": 'Generate JSON: {"test": "success"}'
                }
            ],
            "stream": False,
            "options": {
                "temperature": 0.4,
                "num_predict": 50
            }
        }
        
        test_response = requests.post(
            OLLAMA_URL, 
            json=test_payload,
            headers=headers,
            timeout=60
        )
        
        logger.info(f"Test response status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from Ollama API")
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
            logger.error(f"Test returned invalid JSON: {e}")
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
            # Process in batches of BATCH_SIZE
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
    logger.info("Optimized EU Banking Chat Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}.{CHAT_COLLECTION}")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Configuration: {MAX_WORKERS} workers, {BATCH_SIZE} batch size")
    logger.info(f"Ollama URL: {OLLAMA_URL}")
    logger.info(f"Log Directory: {LOG_DIR}")
    
    # Setup signal handlers and cleanup
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    # Initialize database
    if not init_database():
        logger.error("Cannot proceed without database connection")
        return
    
    try:
        # Show initial stats
        get_collection_stats()
        
        # Try to recover any pending intermediate results first
        recovered_count = recover_from_intermediate_results()
        if recovered_count > 0:
            logger.info(f"Recovered {recovered_count} records from previous session")
        
        # Run optimized processing
        process_chats_optimized()
        
        # Show final stats
        logger.info("="*60)
        logger.info("FINAL STATISTICS")
        logger.info("="*60)
        get_collection_stats()
        get_sample_generated_chats(3)
        
    except KeyboardInterrupt:
        logger.info("Processing interrupted by user")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Save final intermediate results
        results_manager.save_to_file()
        cleanup_resources()
        logger.info("Session complete. Check log files for details:")
        logger.info(f"  Main: {MAIN_LOG_FILE}")
        logger.info(f"  Success: {SUCCESS_LOG_FILE}")
        logger.info(f"  Failures: {FAILURE_LOG_FILE}")
        logger.info(f"  Progress: {PROGRESS_LOG_FILE}")
        logger.info(f"  Intermediate Results: {INTERMEDIATE_RESULTS_FILE}")

# Run the optimized generator
if __name__ == "__main__":
    main()
     