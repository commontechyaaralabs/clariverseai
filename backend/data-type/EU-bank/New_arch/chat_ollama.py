# EU Banking Chat Messages Generation and Analysis System - Ollama Version
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
CHAT_COLLECTION = "chatmessages"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"chat_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"progress_{timestamp}.log"

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

# Import configuration
try:
    from config import (
        OLLAMA_MODEL, BATCH_SIZE, MAX_WORKERS, REQUEST_TIMEOUT, 
        MAX_RETRIES, RETRY_DELAY, BATCH_DELAY, API_CALL_DELAY,
        get_rate_limit_config
    )
    # Apply rate limiting configuration
    rate_config = get_rate_limit_config()
    BATCH_SIZE = rate_config["batch_size"]
    MAX_WORKERS = rate_config["max_workers"]
    BATCH_DELAY = rate_config["batch_delay"]
    API_CALL_DELAY = rate_config["api_call_delay"]
except ImportError:
    # Fallback configuration if config.py doesn't exist
    OLLAMA_MODEL = "gemma3:27b"
    BATCH_SIZE = 3
    MAX_WORKERS = CPU_COUNT  # Use all available CPU cores
    REQUEST_TIMEOUT = 300  # Increased from 120 to 300 seconds (5 minutes)
    MAX_RETRIES = 5
    RETRY_DELAY = 3
    BATCH_DELAY = 2.0  # Reduced delay for faster processing
    API_CALL_DELAY = 0.5  # Reduced API call delay

# Ollama setup
OLLAMA_API_KEY = os.getenv("OLLAMA_API_KEY", "3f2766cd30b00bbb7dc9a3915f7c3c01ddbf18dfbaec26ba6fb4cd0c2cae515a")
OLLAMA_URL = "http://20.66.111.167:7651/api/chat"


# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Retry configuration
MAX_RETRY_ATTEMPTS = 3  # Maximum retry attempts for failed records
RETRY_DELAY_SECONDS = 5  # Delay between retry attempts

# Intermediate results storage
INTERMEDIATE_RESULTS_FILE = LOG_DIR / f"intermediate_results_{timestamp}.json"

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
        # Test connection
        client.admin.command('ping')
        db = client[DB_NAME]
        chat_col = db[CHAT_COLLECTION]
        
        # Create indexes for better performance
        chat_col.create_index("chat.chat_id")
        chat_col.create_index("dominant_topic")
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

def generate_eu_banking_chat_prompt(dominant_topic, subtopics, participants, message_count):
    """
    Generate EU banking chat conversation content and semantic analysis
    """
    
    # Extract participant details for reference
    sender = next((p for p in participants if p['id'] == participants[0]['id']), participants[0])
    recipient = next((p for p in participants if p['id'] != sender['id']), participants[1] if len(participants) > 1 else participants[0])
    
    prompt = f"""
TASK: Generate a realistic EU banking chat conversation with {message_count} messages and provide comprehensive semantic analysis.

**CONTEXT:**
- Dominant Topic: {dominant_topic}
- Subtopics: {subtopics}
- Participants: {sender['displayName']} ({sender['id']}) ↔ {recipient['displayName']} ({recipient['id']})
- Messages: {message_count}
- Industry: EU Banking Sector

**CONVERSATION TYPE DETERMINATION:**
Based on the dominant topic and subtopics, determine the conversation type:
- **Customer-Bank Staff**: If topic involves customer service, account issues, complaints, inquiries, or customer-facing banking services
- **Inter-Company**: If topic involves internal operations, compliance, risk management, or staff-to-staff communication

**CHAT GENERATION REQUIREMENTS:**

1. **EU Banking Context:**
   - Generate chat messages relevant to European banking operations
   - Use European business communication style mixed with casual chat tone
   - Use European date formats (DD/MM/YYYY) and business terminology
   - Balance professional banking language with natural chat conversation flow

2. **Conversation Type & Content Generation:**
   - **Customer-Bank Staff Conversations:**
     * Customer: {sender['displayName']} - seeking help with banking issues, complaints, or inquiries
     * Bank Staff: {recipient['displayName']} - customer service representative providing support
     * Focus on: Account issues, payment problems, card issues, loan inquiries, technical problems, complaints
     * Emotional progression: Start with customer concern/frustration, show resolution process, end with satisfaction or escalation
   
   - **Inter-Company Conversations:**
     * Staff Member 1: {sender['displayName']} - internal banking staff
     * Staff Member 2: {recipient['displayName']} - colleague or department representative
     * Focus on: Internal operations, compliance discussions, risk management, process improvements, policy updates
     * Professional tone with occasional casual elements, collaborative problem-solving

3. **Chat Content Generation:**
   - Create {message_count} realistic chat messages
   - Message 1: Initial message from {sender['displayName']}
   - Subsequent messages: Natural conversation flow alternating between participants
   - Mix of professional and conversational tone appropriate for the conversation type
   - Each message: 50-150 words (vary naturally - some short, some longer)
   - Use natural chat language (shorter sentences, occasional abbreviations, but maintain professionalism)
   - Incorporate banking-specific terminology and scenarios
   - Include realistic chat elements like quick confirmations, follow-up questions, etc.

4. **Mixed Emotions and Sentiments:**
   - Create realistic emotional progression throughout the conversation
   - **Customer-Bank Staff**: Start with various customer emotions (calm, concerned, irritated, happy), show emotional changes as conversation progresses
   - **Inter-Company**: Professional tone with occasional frustration, satisfaction, or concern about work matters
   - Use appropriate language that reflects different sentiment levels (0-5 scale)
   - Show realistic reactions to banking problems, solutions, and internal challenges

5. **Chat Timing and Flow:**
   - Generate realistic timestamps showing natural chat progression (seconds to minutes apart typically)
   - **CRITICAL: All dates must be between 2025-01-01 and 2025-06-30 (6 months only)**
   - Chat messages should show realistic timing patterns:
     - Quick replies: 30 seconds to 2 minutes apart
     - Normal responses: 2-10 minutes apart
     - Delayed responses: 30 minutes to few hours (with context for delay)
   - Show natural conversation flow with appropriate response times

**SEMANTIC ANALYSIS FIELD DEFINITIONS:**

**stages**: Based on reading the ENTIRE chat thread, determine at which customer service stage the conversation concludes. Read all messages and assess where the process ended:
- "Receive": Customer inquiry just received, no response yet
- "Authenticate": Verifying customer identity/credentials
- "Categorize": Understanding and classifying the issue/request
- "Attempt Resolution": Actively working to solve the problem
- "Escalate/Investigate": Issue requires higher-level attention or investigation
- "Update Customer": Providing progress updates or additional information
- "Resolve": Issue has been successfully resolved
- "Confirm/Close": Final confirmation and case closure
- "Report/Analyze": Post-resolution analysis or reporting phase

**chat_summary**: Based on the ENTIRE chat thread, provide a comprehensive summary (100-150 words) that explains what the entire conversation was about. Even for single messages, explain the full context and meaning. The summary should convey the complete story of the chat exchange and its purpose.

**action_pending_status**: After reading all messages, determine if there are any pending actions required: "yes" or "no"

**action_pending_from**: If action_pending_status is "yes", specify who needs to act next: "company" or "customer". If action_pending_status is "no", this field should be null.

**resolution_status**: Based on the complete thread, determine if the main issue/request has been resolved: "open" (unresolved), "inprogress" (work is actively being processed), or "closed" (resolved)

**follow_up_required**: Based on the conversation flow, determine if follow-up communication is needed: "yes" or "no"

**follow_up_date**: If follow-up is required, provide realistic ISO timestamp, otherwise null

**follow_up_reason**: If follow-up is required, explain why and what needs to be followed up in 2 lines maximum

**next_action_suggestion**: Provide AI-agent style recommendation (30-50 words) for the next best action to take. Focus on:
- Customer retention strategies
- Company operational improvements  
- Internal staff satisfaction
- Service quality enhancement
- Compliance requirements
- Relationship building opportunities

**urgency**: Semantic analysis of the chat content to determine if IMMEDIATE action is required. Only mark as "true" for chats that genuinely need urgent attention. Target: Only ~7-8% should be urgent. Base decision on actual semantic content, not default assumptions.

**sentiment**: MANDATORY - You MUST analyze each individual message and assign sentiment scores. This is REQUIRED and cannot be omitted.

For each message, assign a sentiment score (0-5):
- 0: Happy (pleased, satisfied, positive)
- 1: Calm (baseline for professional communication)  
- 2: Bit Irritated (slight annoyance or impatience)
- 3: Moderately Concerned (growing unease or worry)
- 4: Anger (clear frustration or anger)
- 5: Frustrated (extreme frustration, very upset)

**SENTIMENT ANALYSIS RULES:**
1. Analyze EVERY single message individually
2. Use VARIED sentiment scores (not all 1s) - create emotional progression
3. Customer messages: Can range from 0-5 based on satisfaction/frustration
4. Staff messages: Usually 1-3 (calm to concerned), occasionally 0 (happy) or 4-5 (frustrated)
5. Create realistic emotional journey with score variation throughout conversation

**EXAMPLE SENTIMENT STRUCTURE:**
If you have 3 messages, your sentiment should look like:
"sentiment": {{
  "0": 2,
  "1": 1, 
  "2": 0
}}

**overall_sentiment**: Calculate the arithmetic mean of ALL individual message sentiments, rounded to 1 decimal place (0.0-5.0 scale). This represents the overall emotional tone of the entire chat conversation.

**CRITICAL: SENTIMENT IS MANDATORY - You MUST include sentiment analysis for every message. Do not omit this field.**

**OUTPUT FORMAT:**
Return ONLY a JSON object with this structure:

{{
  "messages": [
    {{
      "body": {{
        "text": {{
          "plain": "[complete_chat_message_content_with_banking_context]"
        }}
      }}
    }}
  ],
  "analysis": {{
    "stages": "[single_final_stage_based_on_complete_thread_analysis]",
    "chat_summary": "[100-150_word_comprehensive_thread_summary_explaining_full_context]",
    "action_pending_status": "[yes/no_based_on_thread_analysis]",
    "action_pending_from": "[company/customer_if_pending_yes_or_null_if_pending_no]",
    "resolution_status": "[open/inprogress/closed_based_on_issue_resolution_in_thread]",
    "follow_up_required": "[yes/no_based_on_conversation_needs]",
    "follow_up_date": "[ISO_timestamp_or_null]",
    "follow_up_reason": "[2_lines_explaining_why_followup_needed_or_null]",
    "next_action_suggestion": "[30-50_word_AI_agent_recommendation_for_customer_retention_improvement]",
    "urgency": [true/false_based_on_semantic_urgent_need_analysis],
    "sentiment": {{
      "0": [0-5_sentiment_score_for_message_1],
      "1": [0-5_sentiment_score_for_message_2],
      "2": [0-5_sentiment_score_for_message_3],
      "[message_index]": [0-5_sentiment_score_for_each_message]
    }},
    "overall_sentiment": [0.0-5.0_calculated_average_of_all_message_sentiments]
  }}
}}

**CRITICAL INSTRUCTIONS:**

1. **Conversation Type Selection:** Based on dominant topic and subtopics, intelligently choose between Customer-Bank Staff or Inter-Company conversation types
2. **EU Banking Focus:** Generate authentic European banking scenarios with relevant regulations, terminology, and business practices
3. **Date Range:** ALL dates must be between 2025-01-01 and 2025-06-30 (6 months only) with realistic chat timing patterns
4. **Semantic Analysis:** Read and analyze the ENTIRE chat thread before determining each analysis field
5. **Natural Chat Communication:** Generate normal business chat messages - balance professional banking content with conversational chat style
6. **SENTIMENT PRECISION:** MANDATORY - You MUST include sentiment analysis for EVERY message. Analyze each individual chat message for emotional tone using the 0-5 scale (0=Happy, 1=Calm, 2=Bit Irritated, 3=Moderately Concerned, 4=Anger, 5=Frustrated). MUST use VARIED scores - not all 1s. Consider emotional progression, punctuation, word choice, and informal expressions. Customer messages can be 0-5, staff messages usually 1-3. THIS FIELD CANNOT BE OMITTED.
7. **Mixed Emotions:** Create realistic emotional progression - customers can start frustrated and become satisfied, or staff can show professional concern about internal issues
8. **Urgency Assessment:** Only mark as urgent if content semantically requires immediate action (target: ~7-8%)
9. **Human Sentiment:** Analyze the emotional tone of human communication in each message with careful attention to chat conversation dynamics
10. **Comprehensive Summary:** Chat summary must explain the complete story of the thread
11. **Action-Oriented:** Next action suggestions should focus on business improvement and customer retention
12. **Stage Analysis:** Determine where in the customer service process the chat actually ended
13. **Banking Compliance:** Include relevant EU banking regulations and compliance considerations
14. **Action Pending Logic:** If action_pending_status is "no", then action_pending_from must be null
15. **Chat Timing:** Use realistic chat response times (seconds to minutes, not hours like email)
16. **Message Length:** Keep individual messages chat-appropriate (50-150 words, with natural variation)
17. **EMOTION DETECTION:** Pay special attention to banking-related emotional triggers (account problems, security concerns, successful transactions) and how they manifest in chat conversations. Use the 0-5 sentiment scale: 0=Happy, 1=Calm, 2=Bit Irritated, 3=Moderately Concerned, 4=Anger, 5=Frustrated

**FINAL REMINDER: SENTIMENT ANALYSIS IS MANDATORY - Include sentiment scores for every message and calculate overall_sentiment. Do not omit this field.**

Generate the EU banking chat conversation and comprehensive analysis now.
""".strip()
    
    return prompt

def validate_and_fix_sentiment_data(result):
    """Validate and fix sentiment data to ensure proper format and values"""
    try:
        if 'analysis' not in result:
            return result
            
        analysis = result['analysis']
        
        # Check if sentiment exists and is valid
        if 'sentiment' not in analysis:
            logger.warning("Missing sentiment data, creating default")
            analysis['sentiment'] = {}
        
        if 'overall_sentiment' not in analysis:
            logger.warning("Missing overall_sentiment, calculating from individual sentiments")
            analysis['overall_sentiment'] = 1.0
        
        # Validate sentiment scores
        sentiment_scores = []
        for key, value in analysis['sentiment'].items():
            try:
                score = float(value)
                if 0 <= score <= 5:
                    sentiment_scores.append(score)
                else:
                    logger.warning(f"Invalid sentiment score {score} for message {key}, setting to 1")
                    analysis['sentiment'][key] = 1
                    sentiment_scores.append(1)
            except (ValueError, TypeError):
                logger.warning(f"Invalid sentiment value {value} for message {key}, setting to 1")
                analysis['sentiment'][key] = 1
                sentiment_scores.append(1)
        
        # Calculate overall_sentiment if missing or invalid
        if sentiment_scores:
            overall_sentiment = round(sum(sentiment_scores) / len(sentiment_scores), 1)
            analysis['overall_sentiment'] = overall_sentiment
        else:
            analysis['overall_sentiment'] = 1.0
        
        # Ensure we have sentiment for each message
        if 'messages' in result:
            message_count = len(result['messages'])
            for i in range(message_count):
                if str(i) not in analysis['sentiment']:
                    logger.warning(f"Missing sentiment for message {i}, setting to 1")
                    analysis['sentiment'][str(i)] = 1
        
        return result
        
    except Exception as e:
        logger.error(f"Error validating sentiment data: {e}")
        return result

def generate_eu_banking_chat_content(chat_data):
    """Generate EU banking chat content based on dominant topic and subtopics"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    chat_id = chat_data.get('chat', {}).get('chat_id', 'unknown')
    
    try:
        # Extract data from chat record
        dominant_topic = chat_data.get('dominant_topic', 'General Banking')
        subtopics = chat_data.get('subtopics', 'General operations')
        participants = chat_data.get('chat', {}).get('members', [])
        message_count = chat_data.get('chat', {}).get('message_count', 2)
        
        # Generate the prompt using the existing function
        prompt = generate_eu_banking_chat_prompt(dominant_topic, subtopics, participants, message_count)
        
        response = call_ollama_with_backoff(prompt)
        
        if not response or not response.strip():
            raise ValueError("Empty response from LLM")
        
        # Clean response
        reply = response.strip()
        
        # Remove markdown formatting
        if "```" in reply:
            reply = reply.replace("```json", "").replace("```", "")
        
        # Find JSON object
        json_start = reply.find('{')
        json_end = reply.rfind('}') + 1
        
        if json_start == -1 or json_end <= json_start:
            raise ValueError("No valid JSON found in LLM response")
        
        reply = reply[json_start:json_end]
        
        try:
            result = json.loads(reply)
            
            # Validate required fields
            required_fields = ['messages', 'analysis']
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            # Validate and fix sentiment data
            result = validate_and_fix_sentiment_data(result)
            
            generation_time = time.time() - start_time
            
            # Log successful generation
            success_info = {
                'chat_id': chat_id,
                'dominant_topic': dominant_topic,
                'generation_time': generation_time,
                'message_count': len(result.get('messages', []))
            }
            success_logger.info(json.dumps(success_info))
            
            return result
            
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'chat_id': chat_id,
            'dominant_topic': chat_data.get('dominant_topic', 'Unknown'),
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_chat_update(chat_record, retry_attempt=0):
    """Process a single chat record to generate content and analysis"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate chat content based on existing data
        chat_content = generate_eu_banking_chat_content(chat_record)
        
        if not chat_content:
            if retry_attempt < MAX_RETRY_ATTEMPTS:
                logger.warning(f"Generation failed for {chat_record.get('chat', {}).get('chat_id', 'unknown')}, will retry (attempt {retry_attempt + 1}/{MAX_RETRY_ATTEMPTS})")
                return None  # Will be retried
            else:
                failure_counter.increment()
                logger.error(f"Final failure for {chat_record.get('chat', {}).get('chat_id', 'unknown')} after {MAX_RETRY_ATTEMPTS} attempts")
                return None
        
        # Prepare update document with the new structure
        update_doc = {}
        
        # Update messages with generated content
        if 'messages' in chat_content:
            messages = chat_content['messages']
            for i, message in enumerate(messages):
                if i < len(chat_record.get('messages', [])):
                    update_doc[f'messages.{i}.body.content'] = message.get('body', {}).get('text', {}).get('plain')
        
        # Update analysis fields
        if 'analysis' in chat_content:
            analysis = chat_content['analysis']
            update_doc['stages'] = analysis.get('stages')
            update_doc['chat_summary'] = analysis.get('chat_summary')
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
        
        success_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'chat_id': chat_record['chat']['chat_id'],
            'update_doc': update_doc,
            'original_data': {
                'dominant_topic': chat_record.get('dominant_topic'),
                'subtopics': chat_record.get('subtopics', '')[:100] + '...' if len(str(chat_record.get('subtopics', ''))) > 100 else chat_record.get('subtopics', '')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'chat_id': chat_record['chat']['chat_id'],
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {chat_record.get('chat', {}).get('chat_id', 'unknown')}: {str(e)[:100]}")
        if retry_attempt < MAX_RETRY_ATTEMPTS:
            logger.warning(f"Will retry {chat_record.get('chat', {}).get('chat_id', 'unknown')} due to error (attempt {retry_attempt + 1}/{MAX_RETRY_ATTEMPTS})")
            return None  # Will be retried
        else:
            failure_counter.increment()
            logger.error(f"Final failure for {chat_record.get('chat', {}).get('chat_id', 'unknown')} after {MAX_RETRY_ATTEMPTS} attempts")
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
        
        logger.info(f"Retrying {chat_record.get('chat', {}).get('chat_id', 'unknown')} (attempt {retry_attempt}/{MAX_RETRY_ATTEMPTS})")
        
        # Add delay before retry
        time.sleep(RETRY_DELAY_SECONDS)
        
        result = process_single_chat_update(chat_record, retry_attempt)
        
        if result:
            successful_retries.append(result)
            logger.info(f"Retry successful for {chat_record.get('chat', {}).get('chat_id', 'unknown')}")
        else:
            if retry_attempt < MAX_RETRY_ATTEMPTS:
                # Add back to failed records for another retry
                failed_records.append({
                    'record': chat_record,
                    'retry_attempt': retry_attempt
                })
            else:
                logger.error(f"Final retry failure for {chat_record.get('chat', {}).get('chat_id', 'unknown')}")
    
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
                filter={"chat.chat_id": update_data['chat_id']},
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
                            {"chat.chat_id": update_data['chat_id']},
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
    
    logger.info("Starting EU Banking Chat Content Generation...")
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
        # Query for chats that don't have the analysis fields
        query = {
            "$or": [
                {"stages": {"$exists": False}},
                {"chat_summary": {"$exists": False}},
                {"action_pending_status": {"$exists": False}},
                {"action_pending_from": {"$exists": False}},
                {"resolution_status": {"$exists": False}},
                {"follow_up_required": {"$exists": False}},
                {"next_action_suggestion": {"$exists": False}},
                {"sentiment": {"$exists": False}},
                {"overall_sentiment": {"$exists": False}}
            ]
        }
        
        chat_records = list(chat_col.find(query))
        total_chats = len(chat_records)
        
        if total_chats == 0:
            logger.info("All chats already have analysis fields!")
            return
            
        logger.info(f"Found {total_chats} chats needing content generation")
        progress_logger.info(f"BATCH_START: total_chats={total_chats}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching chat records: {e}")
        return
    
    # Process in batches of BATCH_SIZE (3)
    total_batches = (total_chats + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []  # Accumulate updates for batch saving
    
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
                
                # Collect results with progress tracking - process as they complete
                completed = 0
                batch_failed_records = []  # Track failed records for this batch
                
                try:
                    for future in as_completed(futures, timeout=REQUEST_TIMEOUT * 3):  # Increased timeout multiplier
                        if shutdown_flag.is_set():
                            logger.warning("Cancelling remaining tasks...")
                            for f in futures:
                                f.cancel()
                            break
                            
                        try:
                            result = future.result(timeout=60)  # Increased from 30 to 60 seconds
                            completed += 1
                            
                            if result:
                                successful_updates.append(result)
                                # Save immediately when we have enough for a batch
                                if len(successful_updates) >= BATCH_SIZE:
                                    saved_count = save_batch_to_database(successful_updates)
                                    total_updated += saved_count
                                    successful_updates = []  # Clear after saving
                                    logger.info(f"Immediate save: {saved_count} records")
                            
                            # Progress indicator for each completion
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
                    # Save retry results
                    retry_saved_count = save_batch_to_database(retry_successful)
                    total_updated += retry_saved_count
                    logger.info(f"Retry save: {retry_saved_count} records")
                
                # Clear failed records after retry attempt
                failed_records.clear()
            
            logger.info(f"Batch {batch_num} processing complete: {len(successful_updates)}/{len(batch_records)} successful")
            logger.info(f"Batch duration: {batch_duration:.2f}s")
            progress_logger.info(f"BATCH_COMPLETE: batch={batch_num}, successful={len(successful_updates)}, duration={batch_duration:.2f}s")
            
            # Progress summary every 3 batches (more frequent due to smaller batches)
            if batch_num % 3 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_chats) * 100
                logger.info(f"Overall Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value} | Retries: {retry_counter.value} | DB Updates: {total_updated}")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, success={success_counter.value}, failures={failure_counter.value}, retries={retry_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches to help with rate limiting (reduced)
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
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking chat content generation complete!")
            
        logger.info(f"Total chats updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        logger.info(f"Retry attempts: {retry_counter.value}")
        logger.info(f"Data updated in MongoDB: {DB_NAME}.{CHAT_COLLECTION}")
        
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
            timeout=60  # Increased from 30 to 60 seconds
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
    """Get collection statistics"""
    try:
        total_count = chat_col.count_documents({})
        
        # Count records with and without analysis fields
        with_analysis = chat_col.count_documents({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "chat_summary": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_analysis = total_count - with_analysis
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(chat_col.aggregate(pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"Total chats: {total_count}")
        logger.info(f"With analysis fields: {with_analysis}")
        logger.info(f"Without analysis fields: {without_analysis}")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"{i}. {topic['_id']}: {topic['count']} chats")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_analysis={with_analysis}, without_analysis={without_analysis}")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_chats(limit=3):
    """Get sample chats with generated content"""
    try:
        samples = list(chat_col.find({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "chat_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Chat Content:")
        for i, chat in enumerate(samples, 1):
            logger.info(f"--- Sample Chat {i} ---")
            logger.info(f"Chat ID: {chat.get('chat', {}).get('chat_id', 'N/A')}")
            logger.info(f"Dominant Topic: {chat.get('dominant_topic', 'N/A')}")
            logger.info(f"Subtopics: {str(chat.get('subtopics', 'N/A'))[:100]}...")
            logger.info(f"Stages: {chat.get('stages', 'N/A')}")
            logger.info(f"Chat Summary: {str(chat.get('chat_summary', 'N/A'))[:200]}...")
            if 'urgency' in chat:
                logger.info(f"Urgent: {chat['urgency']}")
            
    except Exception as e:
        logger.error(f"Error getting sample chats: {e}")

def generate_status_report():
    """Generate comprehensive status report"""
    try:
        report_file = LOG_DIR / f"status_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Get intermediate results stats
        pending_results = results_manager.get_pending_updates()
        total_intermediate = len(results_manager.results)
        
        # Get database stats
        total_count = chat_col.count_documents({})
        with_analysis = chat_col.count_documents({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "chat_summary": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Generate report
        status_report = {
            "timestamp": datetime.now().isoformat(),
            "session_stats": {
                "successful_generations": success_counter.value,
                "failed_generations": failure_counter.value,
                "database_updates": update_counter.value,
                "success_rate": (success_counter.value / (success_counter.value + failure_counter.value)) * 100 if (success_counter.value + failure_counter.value) > 0 else 0
            },
            "intermediate_results": {
                "total_results": total_intermediate,
                "pending_database_saves": len(pending_results),
                "intermediate_file": str(INTERMEDIATE_RESULTS_FILE)
            },
            "database_stats": {
                "total_chats": total_count,
                "chats_with_analysis": with_analysis,
                "chats_without_analysis": total_count - with_analysis,
                "completion_percentage": (with_analysis / total_count) * 100 if total_count > 0 else 0
            },
            "system_info": {
                "cpu_cores": CPU_COUNT,
                "max_workers": MAX_WORKERS,
                "batch_size": BATCH_SIZE,
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent
            },
            "log_files": {
                "main_log": str(MAIN_LOG_FILE),
                "success_log": str(SUCCESS_LOG_FILE),
                "failure_log": str(FAILURE_LOG_FILE),
                "progress_log": str(PROGRESS_LOG_FILE)
            }
        }
        
        # Save report
        with open(report_file, 'w') as f:
            json.dump(status_report, f, indent=2)
        
        logger.info(f"Status report generated: {report_file}")
        return status_report
        
    except Exception as e:
        logger.error(f"Error generating status report: {e}")
        return None

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

def validate_generated_content():
    """Validate recently generated content quality"""
    try:
        # Get recent generated chats
        recent_chats = list(chat_col.find({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "chat_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(20))
        
        validation_results = {
            "total_validated": len(recent_chats),
            "validation_issues": [],
            "quality_metrics": {
                "avg_summary_length": 0,
                "chats_with_urgency": 0,
                "chats_with_sentiment": 0
            }
        }
        
        summary_lengths = []
        urgency_count = 0
        sentiment_count = 0
        
        for chat in recent_chats:
            summary = chat.get('chat_summary', '')
            
            # Validate summary
            if len(summary) < 50 or len(summary) > 300:
                validation_results["validation_issues"].append({
                    "chat_id": chat.get('chat', {}).get('chat_id'),
                    "issue": f"Summary length {len(summary)} outside optimal range (50-300)"
                })
            
            # Check for required elements
            if not chat.get('stages'):
                validation_results["validation_issues"].append({
                    "chat_id": chat.get('chat', {}).get('chat_id'),
                    "issue": "Missing stages field"
                })
            
            summary_lengths.append(len(summary))
            
            if chat.get('urgency', False):
                urgency_count += 1
            
            if chat.get('sentiment'):
                sentiment_count += 1
        
        # Calculate metrics
        if summary_lengths:
            validation_results["quality_metrics"]["avg_summary_length"] = sum(summary_lengths) / len(summary_lengths)
        validation_results["quality_metrics"]["chats_with_urgency"] = urgency_count
        validation_results["quality_metrics"]["chats_with_sentiment"] = sentiment_count
        
        logger.info(f"Content validation complete: {len(validation_results['validation_issues'])} issues found")
        
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating content: {e}")
        return None

def cleanup_old_logs(days_to_keep=7):
    """Clean up old log files"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        cleaned_count = 0
        
        for log_file in LOG_DIR.glob("*.log"):
            if log_file.stat().st_mtime < cutoff_date.timestamp():
                log_file.unlink()
                cleaned_count += 1
        
        for json_file in LOG_DIR.glob("*.json"):
            if json_file.stat().st_mtime < cutoff_date.timestamp():
                json_file.unlink()
                cleaned_count += 1
        
        if cleaned_count > 0:
            logger.info(f"Cleaned up {cleaned_count} old log files")
        
    except Exception as e:
        logger.error(f"Error cleaning up old logs: {e}")

# Main execution function
def main():
    """Main function to initialize and run the chat content generator"""
    logger.info("EU Banking Chat Content Generator Starting...")
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
    
    # Clean up old logs
    cleanup_old_logs()
    
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
        
        # Validate content quality
        validation_results = validate_generated_content()
        if validation_results and validation_results["validation_issues"]:
            logger.warning(f"Found {len(validation_results['validation_issues'])} content validation issues")
        
        # Generate final status report
        status_report = generate_status_report()
        if status_report:
            logger.info("Final session report:")
            logger.info(f"Success Rate: {status_report['session_stats']['success_rate']:.2f}%")
            logger.info(f"Database Completion: {status_report['database_stats']['completion_percentage']:.2f}%")
        
    except KeyboardInterrupt:
        logger.info("Content generation interrupted by user!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        # Save final intermediate results
        results_manager.save_to_file()
        cleanup_resources()
        
        logger.info("Session complete. Check log files for detailed information:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")
        logger.info(f"Progress Log: {PROGRESS_LOG_FILE}")
        logger.info(f"Intermediate Results: {INTERMEDIATE_RESULTS_FILE}")

# Run the content generator
if __name__ == "__main__":
    main()