# EU Banking Trouble Ticket Content Generator - Ollama Version
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
TICKET_COLLECTION = "tickets"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"ticket_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_generations_{timestamp}.log"
PROGRESS_LOG_FILE = LOG_DIR / f"progress_{timestamp}.log"

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

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
client = None
db = None
ticket_col = None

# Import configuration - Based on working example
try:
    from config import (
        OLLAMA_BASE_URL, OLLAMA_TOKEN, OLLAMA_MODEL, BATCH_SIZE, MAX_WORKERS, REQUEST_TIMEOUT, 
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
    # Fallback configuration if config.py doesn't exist - matching your exact URL format
    OLLAMA_BASE_URL = "http://80.188.223.202:13267"
    OLLAMA_TOKEN = "3812231de835b2593fa5bd9ea0b41d49929a03103dcab7e687ba674fe4707fbd"
    OLLAMA_MODEL = "gemma3:27b"
    BATCH_SIZE = 3
    MAX_WORKERS = 3
    REQUEST_TIMEOUT = 300
    MAX_RETRIES = 8
    RETRY_DELAY = 5
    BATCH_DELAY = 10.0
    API_CALL_DELAY = 3.0

# Ollama setup - Optimized for Ollama with token authentication
# Check if base URL already includes /api/generate
if OLLAMA_BASE_URL.endswith('/api/generate'):
    OLLAMA_URL = OLLAMA_BASE_URL
else:
    OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"

# Additional configuration
CPU_COUNT = multiprocessing.cpu_count()

# Intermediate results storage
INTERMEDIATE_RESULTS_FILE = LOG_DIR / f"intermediate_results_{timestamp}.json"

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
            progress_logger.info(f"{self._name}: {self._value}")
            return self._value
    
    @property
    def value(self):
        with self._lock:
            return self._value

success_counter = LoggingCounter("SUCCESS_COUNT")
failure_counter = LoggingCounter("FAILURE_COUNT")
update_counter = LoggingCounter("UPDATE_COUNT")
title_counter = LoggingCounter("TITLE_COUNT")

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
                json.dump(self.results, f, indent=2, cls=ObjectIdEncoder)
        except Exception as e:
            logger.error(f"Error saving intermediate results: {e}")
    
    def get_pending_updates(self):
        """Get results that haven't been saved to database yet"""
        return [r for r in self.results if not r.get('saved_to_db', False)]
    
    def mark_as_saved(self, ticket_ids):
        """Mark results as saved to database"""
        with self._lock:
            for result in self.results:
                if result.get('ticket_id') in ticket_ids:
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
    global client, db, ticket_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        db = client[DB_NAME]
        ticket_col = db[TICKET_COLLECTION]
        
        # Create indexes for better performance
        ticket_col.create_index("_id")
        ticket_col.create_index("dominant_topic")
        ticket_col.create_index("priority") 
        ticket_col.create_index("urgency")
        ticket_col.create_index("title")
        ticket_col.create_index("stages")
        ticket_col.create_index("resolution_status")
        ticket_col.create_index("ticket_raised")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def generate_realistic_banking_details():
    """Generate realistic banking details for use in trouble tickets"""
    details = {
        'ticket_number': f"TKT-{random.randint(100000, 999999)}",
        'incident_id': f"INC{random.randint(10000000, 99999999)}",
        'account_number': f"{random.randint(100000000000, 999999999999)}",
        'sort_code': f"{random.randint(10, 99)}-{random.randint(10, 99)}-{random.randint(10, 99)}",
        'swift_code': f"{random.choice(['ABNA', 'DEUT', 'BNPA', 'CITI', 'HSBC', 'BARC'])}{random.choice(['GB', 'DE', 'FR', 'NL', 'IT'])}2{random.choice(['L', 'X'])}{random.randint(100, 999)}",
        'iban': f"{random.choice(['GB', 'DE', 'FR', 'NL', 'IT'])}{random.randint(10, 99)} {random.choice(['ABNA', 'DEUT', 'BNPA'])} {random.randint(1000, 9999)} {random.randint(1000, 9999)} {random.randint(10, 99)}",
        'reference_number': f"REF{random.randint(100000, 999999)}",
        'transaction_id': f"TXN{random.randint(10000000, 99999999)}",
        'amount': f"{random.randint(100, 50000)}.{random.randint(10, 99)}",
        'currency': random.choice(['EUR', 'GBP', 'USD', 'CHF']),
        'branch_code': f"BR{random.randint(1000, 9999)}",
        'customer_id': f"CID{random.randint(100000, 999999)}",
        'system_name': random.choice(['CoreBanking', 'PaymentHub', 'ATMNetwork', 'OnlineBanking', 'MobileApp', 'SwiftGateway']),
        'server_name': f"SRV-{random.choice(['PROD', 'UAT', 'TEST'])}-{random.randint(100, 999)}",
        'error_code': f"ERR_{random.randint(1000, 9999)}",
        'ip_address': f"{random.randint(10, 192)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
        'atm_id': f"ATM-{random.randint(1000, 9999)}",
        'terminal_id': f"TERM{random.randint(100000, 999999)}",
        'date': fake.date_between(start_date='-7d', end_date='today').strftime('%d/%m/%Y'),
        'time': f"{random.randint(0, 23):02d}:{random.randint(0, 59):02d}"
    }
    return details

def generate_random_ticket_raised_date():
    """Generate a random date between 2025-01-01 and 2025-06-30"""
    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 6, 30)
    
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randint(0, days_between)
    
    random_date = start_date + timedelta(days=random_days)
    
    # Add random time component
    random_hour = random.randint(0, 23)
    random_minute = random.randint(0, 59)
    random_second = random.randint(0, 59)
    
    random_datetime = random_date.replace(hour=random_hour, minute=random_minute, second=random_second)
    
    return random_datetime.isoformat()

def determine_urgency_distribution():
    """Determine urgency with proper distribution - only 10-15% as urgent"""
    rand = random.random()
    if rand < 0.12:  # 12% urgent
        return True
    else:
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
    """Call Ollama API with exponential backoff and better error handling - Token as URL parameter"""
    if shutdown_flag.is_set():
        raise KeyboardInterrupt("Shutdown requested")
    
    # Simple headers - no Bearer token
    headers = {
        'Content-Type': 'application/json'
    }
        
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.4,
            "num_predict": 4000,
            "top_k": 30,
            "top_p": 0.9,
            "num_ctx": 6144
        }
    }
    
    # Add token as URL query parameter (matching your working URL format)
    url_with_token = f"{OLLAMA_URL}?token={OLLAMA_TOKEN}"
    
    try:
        response = requests.post(
            url_with_token, 
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
        
        if "response" not in result:
            logger.error(f"No 'response' field. Available fields: {list(result.keys())}")
            raise KeyError("No 'response' field in Ollama response")
            
        return result["response"]
        
    except requests.exceptions.Timeout:
        logger.error(f"Request timed out after {timeout} seconds")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - check Ollama endpoint")
        raise
    except requests.exceptions.HTTPError as e:
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

def generate_eu_banking_ticket_content(ticket_data):
    """Generate EU banking trouble ticket content with all required fields"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    ticket_id = str(ticket_data.get('_id', 'unknown'))
    
    try:
        # Extract data from ticket record
        dominant_topic = ticket_data.get('dominant_topic', 'General Banking System Issue')
        subtopics = ticket_data.get('subtopics', 'System malfunction')
        
        # Generate realistic banking details
        banking_details = generate_realistic_banking_details()
        
        # Generate ticket raised date
        ticket_raised = generate_random_ticket_raised_date()
        
        # Let LLM determine urgency based on description content (no pre-determined value)
        
        # Enhanced prompt for EU banking trouble tickets with all new fields
        prompt = f"""
Generate a realistic EU banking trouble ticket. Analyze the topic to determine if this is a CUSTOMER-REPORTED issue or an INTERNAL COMPANY issue.

**Context Analysis:**
Topic: {dominant_topic}
Subtopics: {subtopics}

**Available System Details:**
Ticket: {banking_details['ticket_number']} | System: {banking_details['system_name']} | Server: {banking_details['server_name']}
Account: {banking_details['account_number']} | Customer: {banking_details['customer_id']} | Branch: {banking_details['branch_code']}
Transaction: {banking_details['transaction_id']} | Amount: {banking_details['currency']} {banking_details['amount']}
Error: {banking_details['error_code']} | IP: {banking_details['ip_address']} | Time: {banking_details['date']} {banking_details['time']}

**CUSTOMER-REPORTED EXAMPLE:**
{{
  "title": "Customer Unable to Access Mobile Banking - Login Failure",
  "description": "Customer contacted support reporting inability to access mobile banking application since yesterday. Customer states: 'I'm facing a problem with my banking app. The app is not turning on. It was working fine until yesterday, but now it doesn't respond. I really need to check my account balance urgently.' Customer attempted multiple login attempts using original credentials registered with account {banking_details['account_number']}. Initial troubleshooting revealed no server-side issues with authentication system. Customer device appears to be iOS 16.3 running latest app version 3.2.1. No recent password changes detected in customer profile. Issue may be related to local app cache or device-specific compatibility problems. Customer expressing moderate frustration due to inability to access funds information before important payment deadline.",
  "urgency": false,
  "stages": "Attempt Resolution",
  "ticket_summary": "Customer {banking_details['customer_id']} reports mobile banking app unresponsive since yesterday. App fails to launch on iOS device despite working previously. No server-side authentication issues detected. Customer needs urgent account access for payment verification. Troubleshooting indicates potential local app cache or device compatibility issue. Customer showing moderate frustration but cooperative with support process. Resolution pending further device diagnostics.",
  "overall_sentiment": 2
}}

**INTERNAL COMPANY EXAMPLE:**
{{
  "title": "Payment Processing System - Database Connection Timeout",
  "description": "Internal monitoring detected intermittent database connection timeouts affecting payment processing system {banking_details['system_name']} on server {banking_details['server_name']}. Error code {banking_details['error_code']} logged at {banking_details['time']} indicating connection pool exhaustion. Approximately 15% of payment transactions experiencing 30-second delays during peak processing hours. Database performance metrics show increased response times correlating with high concurrent user load. System automatically implementing connection retry logic but success rate dropping to 85%. No customer data integrity issues detected. Infrastructure team investigating database cluster performance and connection pooling configuration. Potential impact on SLA compliance if issue persists beyond current business day.",
  "urgency": true,
  "stages": "Escalate/Investigate",
  "ticket_summary": "Payment processing system experiencing database connection timeouts during peak hours. Error {banking_details['error_code']} indicates connection pool exhaustion affecting 15% of transactions. 30-second processing delays observed with 85% retry success rate. No data integrity issues but SLA compliance at risk. Infrastructure team investigating database cluster performance and connection pooling. Requires immediate escalation to prevent customer impact.",
  "overall_sentiment": 1
}}

**INSTRUCTIONS:**
1. **Determine Issue Source**: If topic suggests customer problems (app issues, login problems, card not working, account access, etc.) → Generate CUSTOMER-REPORTED ticket
2. **If Internal Issue**: System failures, server problems, database issues, network outages, compliance violations → Generate INTERNAL COMPANY ticket
3. **Use Realistic Language**: Customer tickets use natural, frustrated language. Internal tickets use technical terminology
4. **Apply Real Details**: Use provided banking details naturally in context
5. **Set Appropriate Urgency**: Only 10-14% should be urgent 

**Field Definitions and Requirements:**

**stages**: Based on reading the ENTIRE chat thread context, determine at which customer service stage the conversation concludes:
- "Receive": Customer inquiry just received, no response yet
- "Authenticate": Verifying customer identity/credentials
- "Categorize": Understanding and classifying the issue/request
- "Attempt Resolution": Actively working to solve the problem
- "Escalate/Investigate": Issue requires higher-level attention or investigation
- "Update Customer": Providing progress updates or additional information
- "Resolve": Issue has been successfully resolved
- "Confirm/Close": Final confirmation and case closure
- "Report/Analyze": Post-resolution analysis or reporting phase

**action_pending_status**: Determine if there are any pending actions required: "yes" or "no"

**action_pending_from**: If action_pending_status is "yes", specify who needs to act next: "company" or "customer". If action_pending_status is "no", this field should be null.

**resolution_status**: Determine if the main issue/request has been resolved: "open" (unresolved), "inprogress" (work is actively being processed), or "closed" (resolved)

**follow_up_required**: Determine if follow-up communication is needed: "yes" or "no"

**follow_up_date**: If follow-up is required, provide realistic ISO timestamp for follow-up, otherwise null

**follow_up_reason**: If follow-up is required, explain why and what needs to be followed up in 2 lines maximum, otherwise null

**next_action_suggestion**: Provide AI-agent style recommendation (30-50 words) for the next best action to take focusing on customer retention, operational improvements, staff satisfaction, service quality, compliance, or relationship building.

**urgency**: CRITICAL - Only mark as TRUE if the description content genuinely requires immediate action. Base this decision SOLELY on the semantic content of the description you generate. Only 10-14% of tickets should be urgent.

**overall_sentiment**: Individual sentiment analysis using human emotional tone (0-5 scale):
- 0: Happy (pleased, satisfied, positive emotional state)
- 1: Calm (baseline for professional communication, neutral tone)
- 2: Bit Irritated (slight annoyance, impatience, minor frustration)
- 3: Moderately Concerned (growing unease, worry, noticeable concern)
- 4: Anger (clear frustration, anger, strong negative emotion)
- 5: Frustrated (extreme frustration, very upset, highly distressed)

**Priority Guidelines:**
- P1 - Critical: Complete system outage, security breach, regulatory compliance failure
- P2 - High: Major functionality impaired, multiple users affected, business operations disrupted
- P3 - Medium: Moderate impact, some users affected, workaround available
- P4 - Low: Minor impact, few users affected, minimal business disruption
- P5 - Very Low: Cosmetic issues, enhancement requests, minimal impact

**CRITICAL INSTRUCTIONS:**
1. Generate realistic technical details using the PROVIDED banking details above
2. NEVER use placeholders like [Account Number] - always use the specific details provided
3. Create content that feels authentic and professional for EU banking operations
4. Reference relevant EU regulations (GDPR, PSD2, CRD IV) where applicable
5. Ensure urgency distribution is realistic (only mark urgent for genuinely severe issues)

**CRITICAL: You MUST return ONLY a valid JSON object with ALL these fields:**

{{
  "title": "Professional title (50-100 chars)",
  "description": "Realistic description (300-400 words)",
  "priority": "P3 - Medium",
  "urgency": false,
  "stages": "Receive",
  "ticket_summary": "Summary (100-110 words)",
  "action_pending_status": "yes",
  "action_pending_from": "company",
  "resolution_status": "open",
  "follow_up_required": "yes",
  "follow_up_date": null,
  "follow_up_reason": null,
  "next_action_suggestion": "Recommendation text",
  "overall_sentiment": 2.0,
  "ticket_raised": "{ticket_raised}"
}}

**IMPORTANT:** Return ONLY the JSON object above with realistic values. No other text.
""".strip()

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
            
            # Debug logging to see what fields the LLM actually returned
            logger.info(f"LLM returned fields for ticket {ticket_id}: {list(result.keys())}")
            
            # Ensure ticket_raised is set first (fallback if LLM didn't generate it)
            if 'ticket_raised' not in result or not result['ticket_raised']:
                result['ticket_raised'] = ticket_raised
            
            # Add fallbacks for critical fields that might be missing
            if 'priority' not in result or not result['priority']:
                result['priority'] = 'P3 - Medium'  # Default to medium priority
                logger.warning(f"Missing priority field for ticket {ticket_id}, defaulting to P3 - Medium")
            
            if 'urgency' not in result:
                result['urgency'] = False  # Default to non-urgent
                logger.warning(f"Missing urgency field for ticket {ticket_id}, defaulting to false")
            
            if 'stages' not in result or not result['stages']:
                result['stages'] = 'Receive'  # Default to initial stage
                logger.warning(f"Missing stages field for ticket {ticket_id}, defaulting to Receive")
            
            if 'overall_sentiment' not in result:
                result['overall_sentiment'] = 2.0  # Default to slightly irritated
                logger.warning(f"Missing overall_sentiment field for ticket {ticket_id}, defaulting to 2.0")
            
            if 'title' not in result or not result['title']:
                result['title'] = f"{dominant_topic} - Support Ticket"
                logger.warning(f"Missing title field for ticket {ticket_id}, using default")
            
            if 'description' not in result or not result['description']:
                result['description'] = f"Support ticket related to {dominant_topic}. Subtopics: {subtopics}. Please review and provide appropriate resolution."
                logger.warning(f"Missing description field for ticket {ticket_id}, using default")
            
            # Validate required fields (after fallbacks)
            required_fields = [
                'title', 'description', 'priority', 'urgency', 'stages', 'ticket_summary',
                'action_pending_status', 'action_pending_from', 'resolution_status',
                'follow_up_required', 'follow_up_date', 'follow_up_reason',
                'next_action_suggestion', 'overall_sentiment', 'ticket_raised'
            ]
            
            for field in required_fields:
                if field not in result:
                    # Add final fallbacks for any remaining missing fields
                    if field == 'ticket_summary':
                        result[field] = f"Summary for {dominant_topic} related issue. Requires further investigation and appropriate resolution based on customer needs and system requirements."
                    elif field == 'action_pending_status':
                        result[field] = 'yes'
                    elif field == 'action_pending_from':
                        result[field] = 'company'
                    elif field == 'resolution_status':
                        result[field] = 'open'
                    elif field == 'follow_up_required':
                        result[field] = 'yes'
                    elif field == 'follow_up_date':
                        result[field] = None
                    elif field == 'follow_up_reason':
                        result[field] = None
                    elif field == 'next_action_suggestion':
                        result[field] = 'Review ticket details and assign to appropriate support team for resolution.'
                    else:
                        raise ValueError(f"Missing required field: {field}")
                    logger.warning(f"Applied fallback for missing field '{field}' in ticket {ticket_id}")
            
            # Validate specific field values
            valid_priorities = ['P1 - Critical', 'P2 - High', 'P3 - Medium', 'P4 - Low', 'P5 - Very Low']
            if result['priority'] not in valid_priorities:
                logger.warning(f"Invalid priority '{result['priority']}' for ticket {ticket_id}, defaulting to P3 - Medium")
                result['priority'] = 'P3 - Medium'
            
            valid_stages = [
                'Receive', 'Authenticate', 'Categorize', 'Attempt Resolution', 
                'Escalate/Investigate', 'Update Customer', 'Resolve', 'Confirm/Close', 'Report/Analyze'
            ]
            if result['stages'] not in valid_stages:
                logger.warning(f"Invalid stage '{result['stages']}' for ticket {ticket_id}, defaulting to Receive")
                result['stages'] = 'Receive'
            
            valid_resolution_status = ['open', 'inprogress', 'closed']
            if result['resolution_status'] not in valid_resolution_status:
                logger.warning(f"Invalid resolution_status '{result['resolution_status']}' for ticket {ticket_id}, defaulting to open")
                result['resolution_status'] = 'open'
            
            # Validate action_pending_from
            if result['action_pending_status'] == 'yes':
                if result['action_pending_from'] not in ['company', 'customer']:
                    result['action_pending_from'] = 'company'
            else:
                result['action_pending_from'] = None
            
            # Validate follow_up fields
            if result['follow_up_required'] == 'no':
                result['follow_up_date'] = None
                result['follow_up_reason'] = None
            
            # Validate urgency as boolean
            if not isinstance(result['urgency'], bool):
                result['urgency'] = False  # Default to non-urgent if not boolean
            
            # Validate overall_sentiment range
            try:
                sentiment = float(result['overall_sentiment'])
                if not (0.0 <= sentiment <= 5.0):
                    result['overall_sentiment'] = 2.0
                else:
                    result['overall_sentiment'] = round(sentiment, 1)
            except (ValueError, TypeError):
                result['overall_sentiment'] = 2.0
            
            # Validate title length
            title = result['title'].strip()
            if len(title) < 50:
                if dominant_topic and len(title) + len(dominant_topic) + 3 <= 100:
                    result['title'] = f"{title} - {dominant_topic}"
            elif len(title) > 100:
                result['title'] = title[:97] + "..."
            
            generation_time = time.time() - start_time
            
            # Log successful generation
            success_info = {
                'ticket_id': ticket_id,
                'dominant_topic': dominant_topic,
                'title': result['title'],
                'priority': result['priority'],
                'urgency': result['urgency'],
                'stages': result['stages'],
                'resolution_status': result['resolution_status'],
                'overall_sentiment': result['overall_sentiment'],
                'generation_time': generation_time,
                'description_length': len(result['description'])
            }
            success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
            
            return result
            
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'ticket_id': str(ticket_id),
            'dominant_topic': ticket_data.get('dominant_topic', 'Unknown'),
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info, cls=ObjectIdEncoder))
        raise

def process_single_ticket_update(ticket_record):
    """Process a single ticket record to generate all content fields"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate ticket content based on existing data
        ticket_content = generate_eu_banking_ticket_content(ticket_record)
        
        if not ticket_content:
            failure_counter.increment()
            return None
        
        # Prepare update document with all new fields
        update_doc = {
            "title": ticket_content['title'],
            "description": ticket_content['description'],
            "priority": ticket_content['priority'],
            "urgency": ticket_content['urgency'],
            "stages": ticket_content['stages'],
            "ticket_summary": ticket_content['ticket_summary'],
            "action_pending_status": ticket_content['action_pending_status'],
            "action_pending_from": ticket_content['action_pending_from'],
            "resolution_status": ticket_content['resolution_status'],
            "follow_up_required": ticket_content['follow_up_required'],
            "follow_up_date": ticket_content['follow_up_date'],
            "follow_up_reason": ticket_content['follow_up_reason'],
            "next_action_suggestion": ticket_content['next_action_suggestion'],
            "overall_sentiment": ticket_content['overall_sentiment'],
            "ticket_raised": ticket_content['ticket_raised']
        }
        
        success_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'ticket_id': str(ticket_record['_id']),
            'update_doc': update_doc,
            'operation_type': 'full_generation',
            'original_data': {
                'dominant_topic': ticket_record.get('dominant_topic'),
                'subtopics': ticket_record.get('subtopics', '')[:100] + '...' if len(str(ticket_record.get('subtopics', ''))) > 100 else ticket_record.get('subtopics', '')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'ticket_id': str(ticket_record['_id']),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {ticket_record.get('_id', 'unknown')}: {str(e)[:100]}")
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
        ticket_ids = []
        
        for update_data in batch_updates:
            # Create UpdateOne operation properly
            operation = UpdateOne(
                filter={"_id": ObjectId(update_data['ticket_id'])},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            ticket_ids.append(update_data['ticket_id'])
        
        # Execute bulk write with proper error handling
        if bulk_operations:
            try:
                result = ticket_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Mark intermediate results as saved
                results_manager.mark_as_saved(ticket_ids)
                
                # Update counter
                update_counter._value += updated_count
                
                logger.info(f"Successfully saved {updated_count} records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved, total_updates: {update_counter.value}")
                
                # Log some details of what was saved
                if updated_count > 0:
                    sample_update = batch_updates[0]
                    operation_type = sample_update.get('operation_type', 'update')
                    if 'title' in sample_update['update_doc']:
                        logger.info(f"Sample update - ID: {sample_update['ticket_id']}, Title: {sample_update['update_doc']['title'][:50]}...")
                    if 'priority' in sample_update['update_doc']:
                        logger.info(f"Priority: {sample_update['update_doc']['priority']}, Urgency: {sample_update['update_doc'].get('urgency', 'N/A')}")
                    if 'stages' in sample_update['update_doc']:
                        logger.info(f"Stage: {sample_update['update_doc']['stages']}, Resolution: {sample_update['update_doc'].get('resolution_status', 'N/A')}")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Try individual updates as fallback
                logger.info("Attempting individual updates as fallback...")
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
                
                if individual_success > 0:
                    results_manager.mark_as_saved([up['ticket_id'] for up in batch_updates[:individual_success]])
                    update_counter._value += individual_success
                    logger.info(f"Fallback: {individual_success} records saved individually")
                
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def update_tickets_with_content_parallel():
    """Update existing tickets with generated content using optimized batch processing"""
    
    logger.info("Starting EU Banking Trouble Ticket Content Generation with OpenRouter...")
    logger.info(f"System Info: {CPU_COUNT} CPU cores detected")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Request timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"Max retries per request: {MAX_RETRIES}")
    logger.info(f"Ollama Model: {OLLAMA_MODEL}")
    
    # Test Ollama connection
    logger.info("Testing Ollama connection...")
    if not test_ollama_connection():
        logger.warning("Ollama connection test failed - this may be due to network restrictions")
        logger.warning("Proceeding anyway since the server may be accessible from your environment")
        logger.info("If generation fails, check Ollama server status and token validity")
    
    # Get all ticket records that need content generation
    logger.info("Fetching ticket records from database...")
    try:
        # Query for tickets that don't have the new fields
        query = {
            "$or": [
                {"description": {"$exists": False}},
                {"priority": {"$exists": False}},
                {"urgency": {"$exists": False}},
                {"stages": {"$exists": False}},
                {"chat_summary": {"$exists": False}},
                {"action_pending_status": {"$exists": False}},
                {"resolution_status": {"$exists": False}},
                {"follow_up_required": {"$exists": False}},
                {"next_action_suggestion": {"$exists": False}},
                {"overall_sentiment": {"$exists": False}},
                {"ticket_raised": {"$exists": False}},
                {"description": {"$in": [None, ""]}},
                {"priority": {"$in": [None, ""]}},
                {"urgency": {"$in": [None, ""]}}
            ]
        }
        
        ticket_records = list(ticket_col.find(query))
        total_tickets = len(ticket_records)
        
        if total_tickets == 0:
            logger.info("All tickets already have complete content!")
            return
        
        # Convert all ObjectId fields to strings to prevent JSON serialization issues
        for record in ticket_records:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
            
        logger.info(f"Found {total_tickets} tickets needing content generation")
        progress_logger.info(f"BATCH_START: total_tickets={total_tickets}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching ticket records: {e}")
        return
    
    # Process in batches of BATCH_SIZE (10)
    total_batches = (total_tickets + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []  # Accumulate updates for batch saving
    
    logger.info(f"Processing in {total_batches} batches of {BATCH_SIZE} tickets each")
    
    try:
        for batch_num in range(1, total_batches + 1):
            if shutdown_flag.is_set():
                logger.info(f"Shutdown requested. Stopping at batch {batch_num-1}/{total_batches}")
                break
                
            batch_start = (batch_num - 1) * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_tickets)
            batch_records = ticket_records[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_records)} tickets)...")
            progress_logger.info(f"BATCH_START: batch={batch_num}/{total_batches}, records={len(batch_records)}")
            
            # Process batch with parallelization
            successful_updates = []
            batch_start_time = time.time()
            
            # Use ThreadPoolExecutor for I/O bound operations (API calls)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks for this batch
                futures = {
                    executor.submit(process_single_ticket_update, record): record 
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
            
            # Save to database every batch
            if len(batch_updates) >= BATCH_SIZE and not shutdown_flag.is_set():
                saved_count = save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear the accumulator
                
                logger.info(f"Database update complete: {saved_count} records saved")
            
            # Progress summary every 5 batches
            if batch_num % 5 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_tickets) * 100
                logger.info(f"Overall Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value} | DB Updates: {total_updated}")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, success={success_counter.value}, failures={failure_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches
            if not shutdown_flag.is_set() and batch_num < total_batches:
                time.sleep(2)  # 2 second delay between batches
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
            logger.info(f"Final batch saved: {saved_count} records")
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking trouble ticket content generation complete!")
            
        logger.info(f"Total tickets updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        logger.info(f"Data updated in MongoDB: {DB_NAME}.{TICKET_COLLECTION}")
        
        # Final progress log
        progress_logger.info(f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        shutdown_flag.set()

def test_ollama_connection():
    """Test if Ollama is accessible and model is available - Token as URL parameter"""
    try:
        logger.info(f"Testing connection to Ollama: {OLLAMA_BASE_URL}")
        
        if not OLLAMA_TOKEN:
            logger.error("Ollama token not found in configuration")
            return False
        
        # Test basic connection with simple generation
        logger.info("Testing simple generation...")
        
        # Simple headers - no Bearer token
        headers = {
            'Content-Type': 'application/json'
        }
        
        test_payload = {
            "model": OLLAMA_MODEL,
            "prompt": "Generate a JSON object with 'test': 'success'",
            "stream": False,
            "options": {"num_predict": 20}
        }
        
        # Add token as URL query parameter (matching your working URL format)
        url_with_token = f"{OLLAMA_URL}?token={OLLAMA_TOKEN}"
        
        test_response = requests.post(
            url_with_token, 
            json=test_payload,
            headers=headers,
            timeout=60
        )
        
        logger.info(f"Test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from generation endpoint")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "response" in result:
                logger.info("Ollama connection test successful")
                logger.info(f"Test response: {result['response'][:100]}...")
                return True
            else:
                logger.error(f"No 'response' field in test. Fields: {list(result.keys())}")
                return False
        except json.JSONDecodeError as e:
            logger.error(f"Generation test returned invalid JSON: {e}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Ollama connection test failed: {e}")
        return False

def get_collection_stats():
    """Get collection statistics"""
    try:
        total_count = ticket_col.count_documents({})
        
        # Count records with complete new fields
        with_all_new_fields = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None},
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_status": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "follow_up_required": {"$exists": True, "$ne": "", "$ne": None},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_raised": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Count urgent tickets
        urgent_tickets = ticket_col.count_documents({
            "urgency": True
        })
        
        without_complete_fields = total_count - with_all_new_fields
        
        # Get stage distribution if available
        stage_pipeline = [
            {"$match": {"stages": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {"_id": "$stages", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        stage_distribution = list(ticket_col.aggregate(stage_pipeline))
        
        # Get resolution status distribution if available
        resolution_pipeline = [
            {"$match": {"resolution_status": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {"_id": "$resolution_status", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        resolution_distribution = list(ticket_col.aggregate(resolution_pipeline))
        
        # Get priority distribution if available
        priority_pipeline = [
            {"$match": {"priority": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        priority_distribution = list(ticket_col.aggregate(priority_pipeline))
        
        # Get sentiment stats if available
        sentiment_pipeline = [
            {"$match": {"overall_sentiment": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {
                "_id": None,
                "avg_sentiment": {"$avg": "$overall_sentiment"},
                "min_sentiment": {"$min": "$overall_sentiment"},
                "max_sentiment": {"$max": "$overall_sentiment"},
                "count": {"$sum": 1}
            }}
        ]
        
        sentiment_stats = list(ticket_col.aggregate(sentiment_pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"Total tickets: {total_count}")
        logger.info(f"With complete new fields: {with_all_new_fields}")
        logger.info(f"Without complete fields: {without_complete_fields}")
        logger.info(f"Urgent tickets: {urgent_tickets} ({(urgent_tickets/total_count)*100:.1f}% of total)" if total_count > 0 else "Urgent tickets: 0")
        logger.info(f"Completion percentage: {(with_all_new_fields/total_count)*100:.1f}%" if total_count > 0 else "Completion percentage: 0%")
        
        if stage_distribution:
            logger.info("Stage Distribution:")
            for stage in stage_distribution:
                logger.info(f"  {stage['_id']}: {stage['count']} tickets")
        
        if resolution_distribution:
            logger.info("Resolution Status Distribution:")
            for resolution in resolution_distribution:
                logger.info(f"  {resolution['_id']}: {resolution['count']} tickets")
        
        if priority_distribution:
            logger.info("Priority Distribution:")
            for priority in priority_distribution:
                logger.info(f"  {priority['_id']}: {priority['count']} tickets")
        
        if sentiment_stats and sentiment_stats[0]['count'] > 0:
            stats = sentiment_stats[0]
            logger.info("Sentiment Statistics:")
            logger.info(f"  Average sentiment: {stats['avg_sentiment']:.2f}")
            logger.info(f"  Min sentiment: {stats['min_sentiment']:.1f}")
            logger.info(f"  Max sentiment: {stats['max_sentiment']:.1f}")
            logger.info(f"  Tickets with sentiment: {stats['count']}")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, complete={with_all_new_fields}, urgent={urgent_tickets}, urgent_percentage={urgent_tickets/total_count*100:.1f}" if total_count > 0 else "COLLECTION_STATS: total=0")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_tickets(limit=3):
    """Get sample tickets with generated content"""
    try:
        samples = list(ticket_col.find({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Ticket Content:")
        for i, ticket in enumerate(samples, 1):
            logger.info(f"--- Sample Ticket {i} ---")
            logger.info(f"Ticket ID: {ticket.get('_id', 'N/A')}")
            logger.info(f"Title: {ticket.get('title', 'N/A')}")
            logger.info(f"Dominant Topic: {ticket.get('dominant_topic', 'N/A')}")
            logger.info(f"Priority: {ticket.get('priority', 'N/A')}")
            logger.info(f"Urgency: {ticket.get('urgency', 'N/A')}")
            logger.info(f"Stage: {ticket.get('stages', 'N/A')}")
            logger.info(f"Resolution Status: {ticket.get('resolution_status', 'N/A')}")
            logger.info(f"Overall Sentiment: {ticket.get('overall_sentiment', 'N/A')}")
            logger.info(f"Follow-up Required: {ticket.get('follow_up_required', 'N/A')}")
            logger.info(f"Ticket Raised: {ticket.get('ticket_raised', 'N/A')}")
            logger.info(f"Description Preview: {str(ticket.get('description', 'N/A'))[:200]}...")
            logger.info(f"Chat Summary Preview: {str(ticket.get('chat_summary', 'N/A'))[:150]}...")
            
    except Exception as e:
        logger.error(f"Error getting sample tickets: {e}")

def generate_status_report():
    """Generate comprehensive status report"""
    try:
        report_file = LOG_DIR / f"status_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Get intermediate results stats
        pending_results = results_manager.get_pending_updates()
        total_intermediate = len(results_manager.results)
        
        # Get database stats
        total_count = ticket_col.count_documents({})
        with_complete_fields = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None},
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_raised": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        urgent_count = ticket_col.count_documents({"urgency": True})
        
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
                "total_tickets": total_count,
                "tickets_with_complete_fields": with_complete_fields,
                "tickets_without_complete_fields": total_count - with_complete_fields,
                "urgent_tickets": urgent_count,
                "completion_percentage": (with_complete_fields / total_count) * 100 if total_count > 0 else 0,
                "urgency_percentage": (urgent_count / total_count) * 100 if total_count > 0 else 0
            },
            "system_info": {
                "cpu_cores": CPU_COUNT,
                "max_workers": MAX_WORKERS,
                "batch_size": BATCH_SIZE,
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent,
                "ollama_model": OLLAMA_MODEL
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
            json.dump(status_report, f, indent=2, cls=ObjectIdEncoder)
        
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
            if 'ticket_id' in result and 'update_doc' in result:
                batch_updates.append({
                    'ticket_id': result['ticket_id'],
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
    """Main function to initialize and run the trouble ticket content generator"""
    logger.info("EU Banking Trouble Ticket Content Generator - Ollama Version Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {TICKET_COLLECTION}")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_BASE_URL}")
    logger.info(f"Using token authentication")
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
        
        # Generate full content with all new fields
        logger.info("=" * 80)
        logger.info("GENERATING COMPLETE TICKET CONTENT WITH ALL FIELDS")
        logger.info("=" * 80)
        update_tickets_with_content_parallel()
        
        # Show final statistics
        logger.info("=" * 80)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 80)
        get_collection_stats()
        
        # Show sample generated content
        get_sample_generated_tickets()
        
        # Generate final status report
        status_report = generate_status_report()
        if status_report:
            logger.info("=" * 80)
            logger.info("FINAL SESSION REPORT")
            logger.info("=" * 80)
            logger.info(f"Success Rate: {status_report['session_stats']['success_rate']:.2f}%")
            logger.info(f"Total Content Generated: {status_report['session_stats']['successful_generations']}")
            logger.info(f"Total Database Updates: {status_report['session_stats']['database_updates']}")
            logger.info(f"Completion Rate: {status_report['database_stats']['completion_percentage']:.2f}%")
            logger.info(f"Urgent Tickets: {status_report['database_stats']['urgency_percentage']:.2f}%")
        
    except KeyboardInterrupt:
        logger.info("Content generation interrupted by user!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        # Save final intermediate results
        results_manager.save_to_file()
        cleanup_resources()
        
        logger.info("=" * 80)
        logger.info("SESSION COMPLETE")
        logger.info("=" * 80)
        logger.info("Check log files for detailed information:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")
        logger.info(f"Progress Log: {PROGRESS_LOG_FILE}")
        logger.info(f"Intermediate Results: {INTERMEDIATE_RESULTS_FILE}")

# Run the content generator
if __name__ == "__main__":
    main()