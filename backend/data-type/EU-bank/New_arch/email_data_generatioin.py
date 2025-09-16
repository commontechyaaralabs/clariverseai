# EU Banking Email Thread Generation and Analysis System
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
EMAIL_COLLECTION = "email"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"email_generator_{timestamp}.log"
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
email_col = None

# Import configuration
try:
    from config import (
        OPENROUTER_MODEL, BATCH_SIZE, MAX_WORKERS, REQUEST_TIMEOUT, 
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
    OPENROUTER_MODEL = "google/gemma-3-27b-it:free"
    BATCH_SIZE = 3
    MAX_WORKERS = 2
    REQUEST_TIMEOUT = 120
    MAX_RETRIES = 5
    RETRY_DELAY = 3
    BATCH_DELAY = 5.0
    API_CALL_DELAY = 1.0

# OpenRouter setup
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

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
    
    def mark_as_saved(self, message_ids):
        """Mark results as saved to database"""
        with self._lock:
            for result in self.results:
                if result.get('thread_id') in message_ids:
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
    global client, db, email_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        db = client[DB_NAME]
        email_col = db[EMAIL_COLLECTION]
        
        # Create indexes for better performance
        email_col.create_index("thread.thread_id")
        email_col.create_index("dominant_topic")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=300,  # Increased max time for rate limiting
    base=RETRY_DELAY,
    on_backoff=lambda details: logger.warning(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_openrouter_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call OpenRouter API with exponential backoff and better error handling"""
    if shutdown_flag.is_set():
        raise KeyboardInterrupt("Shutdown requested")
    
    headers = {
        'Authorization': f'Bearer {OPENROUTER_API_KEY}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'http://localhost:3000',
        'X-Title': 'EU Banking Email Generator'
    }
    
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {
                "role": "user",
                "content": prompt
            }
        ],
        "max_tokens": 4000,
        "temperature": 0.4
    }
    
    try:
        response = requests.post(
            OPENROUTER_URL, 
            json=payload, 
            headers=headers,
            timeout=timeout
        )
        
        # Check if response is empty
        if not response.text.strip():
            raise ValueError("Empty response from OpenRouter API")
        
        response.raise_for_status()
        
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error. Response text: {response.text[:200]}...")
            raise
        
        if "choices" not in result or not result["choices"]:
            logger.error(f"No 'choices' field. Available fields: {list(result.keys())}")
            raise KeyError("No 'choices' field in OpenRouter response")
            
        # Add delay to help with rate limiting
        time.sleep(API_CALL_DELAY)
        return result["choices"][0]["message"]["content"]
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out after {timeout} seconds")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - check OpenRouter endpoint")
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.warning(f"Rate limited (429) - will retry with backoff")
        else:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text[:200]}")
        raise
    except requests.exceptions.RequestException as e:
        logger.error(f"OpenRouter API error: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON response: {e}")
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"API response error: {e}")
        raise

def generate_eu_banking_email_prompt(dominant_topic, subtopics, participants, message_count):
    """
    Generate EU banking email thread content and semantic analysis
    """
    
    # Extract participant details for reference
    sender = next((p for p in participants if p['type'] == 'from'), participants[0])
    recipient = next((p for p in participants if p['type'] == 'to'), participants[1] if len(participants) > 1 else participants[0])
    
    prompt = f"""
TASK: Generate a realistic EU banking email thread with {message_count} messages and provide comprehensive semantic analysis.

**CONTEXT:**
- Dominant Topic: {dominant_topic}
- Subtopics: {subtopics}
- Participants: {sender['name']} ({sender['email']}) ↔ {recipient['name']} ({recipient['email']})
- Messages: {message_count}
- Industry: EU Banking Sector

**EMAIL GENERATION REQUIREMENTS:**

1. **EU Banking Context:**
   - Generate emails relevant to European banking operations
   - Use European business communication style
   - Use European date formats (DD/MM/YYYY) and business terminology

2. **Email Content Generation:**
   - Create {message_count} realistic email messages
   - Message 1: Initial email from {sender['name']}
   - Subsequent messages: Natural conversation flow alternating between participants
   - Professional banking tone appropriate to EU standards
   - Each message: 200-300 words (vary naturally)
   - Include proper email greetings and professional closings
   - Incorporate banking-specific terminology and scenarios

3. **Subject and Dating:**
   - Generate realistic banking-related subject lines
   - Reply messages use "Re: [original subject]" format
   - Generate realistic timestamps showing natural progression (minutes to days apart)
   - Use recent dates (within last 6 months)

**SEMANTIC ANALYSIS FIELD DEFINITIONS:**

**stages**: Based on reading the ENTIRE email thread, determine at which customer service stage the conversation concludes. Read all messages and assess where the process ended:
- "Receive": Customer inquiry just received, no response yet
- "Authenticate": Verifying customer identity/credentials
- "Categorize": Understanding and classifying the issue/request
- "Attempt Resolution": Actively working to solve the problem
- "Escalate/Investigate": Issue requires higher-level attention or investigation
- "Update Customer": Providing progress updates or additional information
- "Resolve": Issue has been successfully resolved
- "Confirm/Close": Final confirmation and case closure
- "Report/Analyze": Post-resolution analysis or reporting phase

**email_summary**: Based on the ENTIRE thread, provide a comprehensive summary (100-150 words) that explains what the entire conversation was about. Even for single messages, explain the full context and meaning. The summary should convey the complete story of the email exchange and its purpose.

**action_pending_status**: After reading all messages, determine if there are any pending actions required: "yes" or "no"

**action_pending_from**: If action is pending, specify who needs to act next: "company" or "customer"

**resolution_status**: Based on the complete thread, determine if the main issue/request has been resolved: "open" (unresolved) or "closed" (resolved)

**follow_up_required**: Based on the conversation flow, determine if follow-up communication is needed: "yes" or "no"

**follow_up_date**: If follow-up is required, provide realistic ISO timestamp, otherwise null

**follow_up_reason**: If follow-up is required, explain why and what needs to be followed up in 2 lines maximum

**next_action_suggestion**: Provide AI-agent style recommendation (150-200 words) for the next best action to take. Focus on:
- Customer retention strategies
- Company operational improvements  
- Internal staff satisfaction
- Service quality enhancement
- Compliance requirements
- Relationship building opportunities

**urgency**: Semantic analysis of the email content to determine if IMMEDIATE action is required. Only mark as "true" for emails that genuinely need urgent attention. Target: Only ~7-8% should be urgent. Base decision on actual semantic content, not default assumptions.

**sentiment**: Individual message sentiment analysis using human emotional tone:
- 0: Neutral/Calm (baseline for professional communication)
- 1: Slightly Concerned/Mildly Positive  
- 2: Moderately Concerned/Happy
- 3: Worried/Excited
- 4: Very Concerned/Very Happy
- 5: Extremely Distressed/Extremely Pleased

**overall_sentiment**: Average sentiment across the entire thread (0-5 scale)

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
          "plain": "[complete_email_body_content_with_banking_context]"
        }}
      }}
    }}
  ],
  "analysis": {{
    "stages": "[single_final_stage_based_on_complete_thread_analysis]",
    "email_summary": "[100-150_word_comprehensive_thread_summary_explaining_full_context]",
    "action_pending_status": "[yes/no_based_on_thread_analysis]",
    "action_pending_from": "[company/customer_based_on_who_needs_to_act]",
    "resolution_status": "[open/closed_based_on_issue_resolution_in_thread]",
    "follow_up_required": "[yes/no_based_on_conversation_needs]",
    "follow_up_date": "[ISO_timestamp_or_null]",
    "follow_up_reason": "[2_lines_explaining_why_followup_needed_or_null]",
    "next_action_suggestion": "[150-200_word_AI_agent_recommendation_for_customer_retention_improvement]",
    "urgency": [true/false_based_on_semantic_urgent_need_analysis],
    "sentiment": {{
      "0": [0-5_human_sentiment_score_message_1],
      "1": [0-5_human_sentiment_score_message_2],
      "[message_index]": [0-5_human_sentiment_score]
    }},
    "overall_sentiment": [0-5_average_sentiment_across_thread]
  }}
}}

**CRITICAL INSTRUCTIONS:**

1. **EU Banking Focus:** Generate authentic European banking scenarios with relevant regulations, terminology, and business practices
2. **Semantic Analysis:** Read and analyze the ENTIRE thread before determining each analysis field
3. **Natural Communication:** Generate normal banking emails - avoid defaulting to crisis scenarios
4. **Urgency Assessment:** Only mark as urgent if content semantically requires immediate action (target: ~7-8%)
5. **Human Sentiment:** Analyze the emotional tone of human communication in each message
6. **Comprehensive Summary:** Email summary must explain the complete story of the thread
7. **Action-Oriented:** Next action suggestions should focus on business improvement and customer retention
8. **Stage Analysis:** Determine where in the customer service process the thread actually ended
9. **Banking Compliance:** Include relevant EU banking regulations and compliance considerations

Generate the EU banking email thread and comprehensive analysis now.
""".strip()
    
    return prompt


def generate_eu_banking_email_content(email_data):
    """Generate EU banking email content based on dominant topic and subtopics"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    thread_id = email_data.get('thread', {}).get('thread_id', 'unknown')
    
    try:
        # Extract data from email record
        dominant_topic = email_data.get('dominant_topic', 'General Banking')
        subtopics = email_data.get('subtopics', 'General operations')
        participants = email_data.get('thread', {}).get('participants', [])
        message_count = email_data.get('thread', {}).get('message_count', 2)
        
        # Generate the prompt using the existing function
        prompt = generate_eu_banking_email_prompt(dominant_topic, subtopics, participants, message_count)
        
        response = call_openrouter_with_backoff(prompt)
        
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
            required_fields = ['thread_data', 'messages', 'analysis']
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            generation_time = time.time() - start_time
            
            # Log successful generation
            success_info = {
                'thread_id': thread_id,
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
            'thread_id': thread_id,
            'dominant_topic': email_data.get('dominant_topic', 'Unknown'),
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_email_update(email_record):
    """Process a single email record to generate content and analysis"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate email content based on existing data
        email_content = generate_eu_banking_email_content(email_record)
        
        if not email_content:
            failure_counter.increment()
            return None
        
        # Prepare update document with the new structure
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
        
        # Update analysis fields
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
        
        success_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'thread_id': email_record['thread']['thread_id'],
            'update_doc': update_doc,
            'original_data': {
                'dominant_topic': email_record.get('dominant_topic'),
                'subtopics': email_record.get('subtopics', '')[:100] + '...' if len(str(email_record.get('subtopics', ''))) > 100 else email_record.get('subtopics', '')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'thread_id': email_record['thread']['thread_id'],
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {email_record.get('thread', {}).get('thread_id', 'unknown')}: {str(e)[:100]}")
        failure_counter.increment()
        return None

def save_batch_to_database(batch_updates):
    """Save a batch of updates to the database using proper bulk write operations"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        # Create proper UpdateOne operations
        bulk_operations = []
        thread_ids = []
        
        for update_data in batch_updates:
            # Create UpdateOne operation properly
            operation = UpdateOne(
                filter={"thread.thread_id": update_data['thread_id']},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            thread_ids.append(update_data['thread_id'])
        
        # Execute bulk write with proper error handling
        if bulk_operations:
            try:
                result = email_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Mark intermediate results as saved
                results_manager.mark_as_saved(thread_ids)
                
                # Update counter
                update_counter._value += updated_count
                
                logger.info(f"Successfully saved {updated_count} records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved, total_updates: {update_counter.value}")
                
                # Log some details of what was saved
                if updated_count > 0:
                    sample_update = batch_updates[0]
                    logger.info(f"Sample update - Thread ID: {sample_update['thread_id']}")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Try individual updates as fallback
                logger.info("Attempting individual updates as fallback...")
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
                
                if individual_success > 0:
                    results_manager.mark_as_saved([up['thread_id'] for up in batch_updates[:individual_success]])
                    update_counter._value += individual_success
                    logger.info(f"Fallback: {individual_success} records saved individually")
                
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def update_emails_with_content_parallel():
    """Update existing emails with generated content and analysis using optimized batch processing"""
    
    logger.info("Starting EU Banking Email Content Generation...")
    logger.info(f"System Info: {CPU_COUNT} CPU cores detected")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Request timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"Max retries per request: {MAX_RETRIES}")
    
    # Test OpenRouter connection
    if not test_openrouter_connection():
        logger.error("Cannot proceed without OpenRouter connection")
        return
    
    # Get all email records that need content generation
    logger.info("Fetching email records from database...")
    try:
        # Query for emails that don't have the analysis fields
        query = {
            "$or": [
                {"stages": {"$exists": False}},
                {"email_summary": {"$exists": False}},
                {"action_pending_status": {"$exists": False}},
                {"action_pending_from": {"$exists": False}},
                {"resolution_status": {"$exists": False}},
                {"follow_up_required": {"$exists": False}},
                {"next_action_suggestion": {"$exists": False}},
                {"sentiment": {"$exists": False}},
                {"overall_sentiment": {"$exists": False}}
            ]
        }
        
        email_records = list(email_col.find(query))
        total_emails = len(email_records)
        
        if total_emails == 0:
            logger.info("All emails already have analysis fields!")
            return
            
        logger.info(f"Found {total_emails} emails needing content generation")
        progress_logger.info(f"BATCH_START: total_emails={total_emails}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching email records: {e}")
        return
    
    # Process in batches of BATCH_SIZE (10)
    total_batches = (total_emails + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []  # Accumulate updates for batch saving
    
    logger.info(f"Processing in {total_batches} batches of {BATCH_SIZE} emails each")
    
    try:
        for batch_num in range(1, total_batches + 1):
            if shutdown_flag.is_set():
                logger.info(f"Shutdown requested. Stopping at batch {batch_num-1}/{total_batches}")
                break
                
            batch_start = (batch_num - 1) * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_emails)
            batch_records = email_records[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_records)} emails)...")
            progress_logger.info(f"BATCH_START: batch={batch_num}/{total_batches}, records={len(batch_records)}")
            
            # Process batch with parallelization
            successful_updates = []
            batch_start_time = time.time()
            
            # Use ThreadPoolExecutor for I/O bound operations (API calls)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks for this batch
                futures = {
                    executor.submit(process_single_email_update, record): record 
                    for record in batch_records
                }
                
                # Collect results with progress tracking
                completed = 0
                try:
                    for future in as_completed(futures, timeout=REQUEST_TIMEOUT * 2):
                        if shutdown_flag.is_set():
                            logger.warning("Cancelling remaining tasks...")
                            for f in futures:
                                f.cancel()
                            break
                            
                        try:
                            result = future.result(timeout=30)
                            completed += 1
                            
                            if result:
                                successful_updates.append(result)
                            
                            # Progress indicator
                            if completed % 5 == 0:
                                progress = (completed / len(batch_records)) * 100
                                logger.info(f"Batch progress: {progress:.1f}% ({completed}/{len(batch_records)})")
                                
                        except Exception as e:
                            logger.error(f"Error processing future result: {e}")
                            completed += 1
                            
                except Exception as e:
                    logger.error(f"Error collecting batch results: {e}")
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # Add successful updates to accumulator
            batch_updates.extend(successful_updates)
            
            logger.info(f"Batch {batch_num} processing complete: {len(successful_updates)}/{len(batch_records)} successful")
            logger.info(f"Batch duration: {batch_duration:.2f}s")
            progress_logger.info(f"BATCH_COMPLETE: batch={batch_num}, successful={len(successful_updates)}, duration={batch_duration:.2f}s")
            
            # Save to database every 10 records (one batch)
            if len(batch_updates) >= BATCH_SIZE and not shutdown_flag.is_set():
                saved_count = save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear the accumulator
                
                logger.info(f"Database update complete: {saved_count} records saved")
            
            # Progress summary every 5 batches
            if batch_num % 5 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_emails) * 100
                logger.info(f"Overall Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value} | DB Updates: {total_updated}")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, success={success_counter.value}, failures={failure_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches to help with rate limiting
            if not shutdown_flag.is_set() and batch_num < total_batches:
                time.sleep(BATCH_DELAY)
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
            logger.info(f"Final batch saved: {saved_count} records")
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking email content generation complete!")
            
        logger.info(f"Total emails updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        logger.info(f"Data updated in MongoDB: {DB_NAME}.{EMAIL_COLLECTION}")
        
        # Final progress log
        progress_logger.info(f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        shutdown_flag.set()

def test_openrouter_connection():
    """Test if OpenRouter is accessible and model is available"""
    try:
        logger.info(f"Testing connection to OpenRouter: {OPENROUTER_URL}")
        
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'http://localhost:3000',
            'X-Title': 'EU Banking Email Generator'
        }
        
        # Test basic connection with simple generation
        logger.info("Testing simple generation...")
        
        test_payload = {
            "model": OPENROUTER_MODEL,
            "messages": [
                {
                    "role": "user",
                    "content": "Generate a JSON object with 'test': 'success'"
                }
            ],
            "max_tokens": 20
        }
        
        test_response = requests.post(
            OPENROUTER_URL, 
            json=test_payload,
            headers=headers,
            timeout=30
        )
        
        logger.info(f"Generation test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from generation endpoint")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "choices" in result and result["choices"]:
                logger.info("OpenRouter connection test successful")
                logger.info(f"Test response: {result['choices'][0]['message']['content'][:100]}...")
                return True
            else:
                logger.error(f"No 'choices' field in test. Fields: {list(result.keys())}")
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
        total_count = email_col.count_documents({})
        
        # Count records with and without analysis fields
        with_analysis = email_col.count_documents({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "email_summary": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_analysis = total_count - with_analysis
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(email_col.aggregate(pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"Total emails: {total_count}")
        logger.info(f"With analysis fields: {with_analysis}")
        logger.info(f"Without analysis fields: {without_analysis}")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"{i}. {topic['_id']}: {topic['count']} emails")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_analysis={with_analysis}, without_analysis={without_analysis}")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_emails(limit=3):
    """Get sample emails with generated content"""
    try:
        samples = list(email_col.find({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "email_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Email Content:")
        for i, email in enumerate(samples, 1):
            logger.info(f"--- Sample Email {i} ---")
            logger.info(f"Thread ID: {email.get('thread', {}).get('thread_id', 'N/A')}")
            logger.info(f"Dominant Topic: {email.get('dominant_topic', 'N/A')}")
            logger.info(f"Subtopics: {str(email.get('subtopics', 'N/A'))[:100]}...")
            logger.info(f"Stages: {email.get('stages', 'N/A')}")
            logger.info(f"Email Summary: {str(email.get('email_summary', 'N/A'))[:200]}...")
            if 'urgency' in email:
                logger.info(f"Urgent: {email['urgency']}")
            
    except Exception as e:
        logger.error(f"Error getting sample emails: {e}")

def generate_status_report():
    """Generate comprehensive status report"""
    try:
        report_file = LOG_DIR / f"status_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Get intermediate results stats
        pending_results = results_manager.get_pending_updates()
        total_intermediate = len(results_manager.results)
        
        # Get database stats
        total_count = email_col.count_documents({})
        with_analysis = email_col.count_documents({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "email_summary": {"$exists": True, "$ne": "", "$ne": None}
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
                "total_emails": total_count,
                "emails_with_analysis": with_analysis,
                "emails_without_analysis": total_count - with_analysis,
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
            if 'thread_id' in result and 'update_doc' in result:
                batch_updates.append({
                    'thread_id': result['thread_id'],
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
        # Get recent generated emails
        recent_emails = list(email_col.find({
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "email_summary": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(20))
        
        validation_results = {
            "total_validated": len(recent_emails),
            "validation_issues": [],
            "quality_metrics": {
                "avg_summary_length": 0,
                "emails_with_urgency": 0,
                "emails_with_sentiment": 0
            }
        }
        
        summary_lengths = []
        urgency_count = 0
        sentiment_count = 0
        
        for email in recent_emails:
            summary = email.get('email_summary', '')
            
            # Validate summary
            if len(summary) < 50 or len(summary) > 300:
                validation_results["validation_issues"].append({
                    "thread_id": email.get('thread', {}).get('thread_id'),
                    "issue": f"Summary length {len(summary)} outside optimal range (50-300)"
                })
            
            # Check for required elements
            if not email.get('stages'):
                validation_results["validation_issues"].append({
                    "thread_id": email.get('thread', {}).get('thread_id'),
                    "issue": "Missing stages field"
                })
            
            summary_lengths.append(len(summary))
            
            if email.get('urgency', False):
                urgency_count += 1
            
            if email.get('sentiment'):
                sentiment_count += 1
        
        # Calculate metrics
        if summary_lengths:
            validation_results["quality_metrics"]["avg_summary_length"] = sum(summary_lengths) / len(summary_lengths)
        validation_results["quality_metrics"]["emails_with_urgency"] = urgency_count
        validation_results["quality_metrics"]["emails_with_sentiment"] = sentiment_count
        
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
    """Main function to initialize and run the email content generator"""
    logger.info("EU Banking Email Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {EMAIL_COLLECTION}")
    logger.info(f"Model: {OPENROUTER_MODEL}")
    logger.info(f"OpenRouter URL: {OPENROUTER_URL}")
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
        
        # Run the email content generation
        update_emails_with_content_parallel()
        
        # Show final statistics
        get_collection_stats()
        
        # Show sample generated content
        get_sample_generated_emails()
        
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