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
        'X-Title': 'EU Banking Ticket Generator'
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
        message_count = ticket_data.get('thread', {}).get('message_count', 2)
        
        # Generate realistic banking details
        banking_details = generate_realistic_banking_details()
        
        # Generate ticket raised date
        ticket_raised = generate_random_ticket_raised_date()
        
        # Let LLM determine urgency based on description content (no pre-determined value)
        
        # Enhanced prompt for EU banking trouble tickets - generating only remaining missing fields
        prompt = f"""
Generate the remaining fields for an EU banking trouble ticket. The following fields already exist in the database and should NOT be generated:
- stages: {ticket_data.get('stages', 'Not provided')}
- follow_up_required: {ticket_data.get('follow_up_required', 'Not provided')}
- urgency: {ticket_data.get('urgency', 'Not provided')}

**Context Analysis:**
Topic: {dominant_topic}
Subtopics: {subtopics}
Message Count: {message_count}

**Available Teams for Routing:**
- Customer Service Team: customerservice@eubank.com (General customer inquiries, account issues, basic support)
- Financial Crime Investigation Team: financialcrime@eubank.com (Suspicious transactions, fraud reports, AML issues)
- Loan & Credit Card Support Team: loansupport@eubank.com (Loan applications, credit card issues, payment problems)
- Compliance / KYC Team: kyc@eubank.com (Identity verification, compliance issues, regulatory matters)
- Card Operations Team: cardoperations@eubank.com (Card activation, PIN issues, card replacement)
- Digital Banking Support Team: digitalbanking@eubank.com (Online banking, mobile app, digital platform issues)
- Internal IT Helpdesk: internalithelpdesk@eubank.com (Internal system issues, IT infrastructure problems)

**Available System Details:**
Ticket: {banking_details['ticket_number']} | System: {banking_details['system_name']} | Server: {banking_details['server_name']}
Account: {banking_details['account_number']} | Customer: {banking_details['customer_id']} | Branch: {banking_details['branch_code']}
Transaction: {banking_details['transaction_id']} | Amount: {banking_details['currency']} {banking_details['amount']}
Error: {banking_details['error_code']} | IP: {banking_details['ip_address']} | Time: {banking_details['date']} {banking_details['time']}

**CUSTOMER-REPORTED EXAMPLE:**
{{
  "title": "Customer Unable to Access Mobile Banking - Login Failure",
  "description": "Dear Support Team, I am writing to report a critical issue with my mobile banking application. Since yesterday morning, I have been unable to access my account through the mobile app on my iPhone. The application simply will not open - it crashes immediately upon launching. This is extremely concerning as I need to check my account balance and make an urgent payment before the weekend. I have tried restarting my phone, deleting and reinstalling the app, and using different network connections, but the problem persists. My account number is {banking_details['account_number']} and I am a customer at branch {banking_details['branch_code']}. I have been a loyal customer for over 5 years and this is the first time I've experienced such issues. Could you please investigate this matter urgently and provide me with a resolution? I need to access my funds for an important transaction. Thank you for your prompt attention to this matter. Best regards, [Customer Name]",
  "urgency": false,
  "stages": "Attempt Resolution",
  "ticket_summary": "Customer {banking_details['customer_id']} reports mobile banking app unresponsive since yesterday. App fails to launch on iOS device despite working previously. No server-side authentication issues detected. Customer needs urgent account access for payment verification. Troubleshooting indicates potential local app cache or device compatibility issue. Customer showing moderate frustration but cooperative with support process. Resolution pending further device diagnostics.",
  "overall_sentiment": 2
}}

**INTERNAL COMPANY EXAMPLE:**
{{
  "title": "Payment Processing System - Database Connection Timeout",
  "description": "To: IT Operations Team, From: System Monitoring, Subject: Critical Database Performance Issue. We are experiencing intermittent database connection timeouts affecting our payment processing system {banking_details['system_name']} on server {banking_details['server_name']}. The issue was first detected at {banking_details['time']} on {banking_details['date']} when error code {banking_details['error_code']} was logged, indicating connection pool exhaustion. Current impact: Approximately 15% of payment transactions are experiencing 30-second delays during peak processing hours (9:00-17:00 CET). Database performance metrics show response times increased from 50ms to 800ms during high concurrent user load periods. Our automatic retry logic is functioning but success rate has dropped from 99.5% to 85%. Customer transaction {banking_details['transaction_id']} for amount {banking_details['currency']} {banking_details['amount']} was affected. No data integrity issues detected, but this poses significant risk to SLA compliance if unresolved. Immediate investigation required by database administration team. Please escalate to senior infrastructure team for urgent resolution. Regards, Monitoring Team",
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
5. **Realistic Description Requirements**:
   - **Customer tickets**: Start with "Dear Support Team" or similar greeting, include customer's frustration level, specific account details, attempts to resolve, urgency of need, polite closing
   - **Internal tickets**: Include proper addressing (To/From/Subject), technical details, impact assessment, specific metrics, escalation requests, professional closing
   - **Length**: 300-400 words with natural flow and realistic banking context
   - **Include**: Account numbers, transaction IDs, error codes, timestamps, specific system names naturally integrated
   - **Tone**: Customer tickets should sound like real customer complaints, internal tickets should sound like professional incident reports 

**Field Definitions and Requirements (ONLY generate these missing fields):**

**assigned_team_email**: Based on the topic and subtopics, determine which team should handle this ticket. Choose the most appropriate team email from the available teams above. This will be used to populate empty email addresses in the database.

**action_pending_status**: Determine if there are any pending actions required: "yes" or "no"

**action_pending_from**: If action_pending_status is "yes", specify who needs to act next: "company" or "customer". If action_pending_status is "no", this field should be null.

**resolution_status**: Determine if the main issue/request has been resolved: "open" (unresolved), "inprogress" (work is actively being processed), or "closed" (resolved)

**follow_up_date**: ONLY generate this if follow_up_required is "yes". If follow_up_required is "yes", provide realistic ISO timestamp for follow-up. If follow_up_required is "no", this field should be null.

**follow_up_reason**: ONLY generate this if follow_up_required is "yes". If follow_up_required is "yes", explain why and what needs to be followed up in 2 lines maximum. If follow_up_required is "no", this field should be null.

**next_action_suggestion**: Provide AI-agent style recommendation (30-50 words) for the next best action to take focusing on customer retention, operational improvements, staff satisfaction, service quality, compliance, or relationship building.

**sentiment**: Individual message sentiment analysis using human emotional tone (0-5 scale). Generate sentiment for each message in the conversation based on message_count:
- 0: Neutral/Calm (baseline for professional communication)
- 1: Slightly Concerned/Mildly Positive  
- 2: Moderately Concerned/Happy
- 3: Worried/Excited
- 4: Very Concerned/Very Happy
- 5: Extremely Distressed/Extremely Pleased
- Format: {{"0": sentiment_score_message_1, "1": sentiment_score_message_2, ...}}

**overall_sentiment**: Average sentiment across the entire ticket conversation (0-5 scale)

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
5. **DESCRIPTION MUST BE REALISTIC**: Include proper addressing, natural language flow, specific banking details, and realistic customer/internal communication style
6. **Customer descriptions**: Sound like genuine customer complaints with proper greeting, account details, frustration level, and polite closing
7. **Internal descriptions**: Sound like professional incident reports with proper addressing, technical metrics, and escalation language
8. Ensure urgency distribution is realistic (only mark urgent for genuinely severe issues)

**CRITICAL: You MUST return ONLY a valid JSON object with these fields (excluding stages, follow_up_required, urgency which already exist):**

{{
  "title": "Professional title (50-100 chars)",
  "description": "Realistic description with proper addressing - Customer: 'Dear Support Team, I am writing to report...' or Internal: 'To: IT Team, From: Monitoring, Subject: Critical Issue...' (300-400 words)",
  "priority": "P3 - Medium",
  "assigned_team_email": "customerservice@eubank.com",
  "ticket_summary": "Summary (100-110 words)",
  "action_pending_status": "yes",
  "action_pending_from": "company",
  "resolution_status": "open",
  "follow_up_date": null,
  "follow_up_reason": null,
  "next_action_suggestion": "Recommendation text",
  "sentiment": {
    "0": 2,
    "1": 1,
    "2": 3,
    "3": 2,
    "4": 1,
    "5": 0
  },
  "overall_sentiment": 1.5,
  "ticket_raised": "{ticket_raised}"
}}

**IMPORTANT:** Return ONLY the JSON object above with realistic values. No other text.
""".strip()

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
            
            # Debug logging to see what fields the LLM actually returned
            logger.info(f"LLM returned fields for ticket {ticket_id}: {list(result.keys())}")
            
            # Ensure ticket_raised is set (this is generated by our function, not LLM)
            if 'ticket_raised' not in result or not result['ticket_raised']:
                result['ticket_raised'] = ticket_raised
            
            # Validate required fields - excluding stages, follow_up_required, urgency which already exist
            required_fields = [
                'title', 'description', 'priority', 'assigned_team_email', 'ticket_summary',
                'action_pending_status', 'action_pending_from', 'resolution_status',
                'follow_up_date', 'follow_up_reason', 'next_action_suggestion', 
                'sentiment', 'overall_sentiment', 'ticket_raised'
            ]
            
            # Check for missing required fields and raise error if any are missing
            missing_fields = [field for field in required_fields if field not in result]
            if missing_fields:
                raise ValueError(f"Missing required fields from LLM response: {missing_fields}")
            
            # Validate specific field values (no defaults, just validation)
            valid_priorities = ['P1 - Critical', 'P2 - High', 'P3 - Medium', 'P4 - Low', 'P5 - Very Low']
            if result['priority'] not in valid_priorities:
                raise ValueError(f"Invalid priority '{result['priority']}' for ticket {ticket_id}. Must be one of: {valid_priorities}")
            
            # Validate assigned_team_email
            valid_team_emails = [
                'customerservice@eubank.com',
                'financialcrime@eubank.com', 
                'loansupport@eubank.com',
                'kyc@eubank.com',
                'cardoperations@eubank.com',
                'digitalbanking@eubank.com',
                'internalithelpdesk@eubank.com'
            ]
            if result['assigned_team_email'] not in valid_team_emails:
                raise ValueError(f"Invalid team email '{result['assigned_team_email']}' for ticket {ticket_id}. Must be one of: {valid_team_emails}")
            
            valid_resolution_status = ['open', 'inprogress', 'closed']
            if result['resolution_status'] not in valid_resolution_status:
                raise ValueError(f"Invalid resolution_status '{result['resolution_status']}' for ticket {ticket_id}. Must be one of: {valid_resolution_status}")
            
            # Validate action_pending_from
            if result['action_pending_status'] == 'yes':
                if result['action_pending_from'] not in ['company', 'customer']:
                    raise ValueError(f"Invalid action_pending_from '{result['action_pending_from']}' for ticket {ticket_id}. Must be 'company' or 'customer' when action_pending_status is 'yes'")
            else:
                result['action_pending_from'] = None
            
            # Handle conditional follow_up fields based on existing follow_up_required value
            existing_follow_up_required = ticket_data.get('follow_up_required')
            if existing_follow_up_required == 'no':
                # If follow_up_required is 'no', validate that follow_up fields are null
                if result.get('follow_up_date') is not None:
                    raise ValueError(f"Ticket {ticket_id}: follow_up_required is 'no' but follow_up_date is not null")
                if result.get('follow_up_reason') is not None:
                    raise ValueError(f"Ticket {ticket_id}: follow_up_required is 'no' but follow_up_reason is not null")
            elif existing_follow_up_required == 'yes':
                # If follow_up_required is 'yes', validate that follow_up fields are provided
                if not result.get('follow_up_date'):
                    raise ValueError(f"Ticket {ticket_id}: follow_up_required is 'yes' but follow_up_date is missing or null")
                if not result.get('follow_up_reason'):
                    raise ValueError(f"Ticket {ticket_id}: follow_up_required is 'yes' but follow_up_reason is missing or null")
            
            
            # Validate sentiment structure
            if not isinstance(result.get('sentiment'), dict):
                raise ValueError(f"Invalid sentiment structure for ticket {ticket_id}. Sentiment must be a dictionary with message indices as keys")
            else:
                # Validate individual sentiment values
                for key, value in result['sentiment'].items():
                    try:
                        sentiment_val = float(value)
                        if not (0.0 <= sentiment_val <= 5.0):
                            raise ValueError(f"Invalid sentiment value {value} for message {key} in ticket {ticket_id}. Must be between 0-5")
                        result['sentiment'][key] = int(sentiment_val)
                    except (ValueError, TypeError):
                        raise ValueError(f"Invalid sentiment value '{value}' for message {key} in ticket {ticket_id}. Must be a number between 0-5")
            
            # Validate overall_sentiment range
            try:
                sentiment = float(result['overall_sentiment'])
                if not (0.0 <= sentiment <= 5.0):
                    raise ValueError(f"Invalid overall_sentiment value {sentiment} for ticket {ticket_id}. Must be between 0-5")
                result['overall_sentiment'] = round(sentiment, 1)
            except (ValueError, TypeError):
                raise ValueError(f"Invalid overall_sentiment value '{result['overall_sentiment']}' for ticket {ticket_id}. Must be a number between 0-5")
            
            # Validate title length
            title = result['title'].strip()
            if len(title) < 50:
                raise ValueError(f"Title too short for ticket {ticket_id}: '{title}' (length: {len(title)}). Must be at least 50 characters")
            elif len(title) > 100:
                raise ValueError(f"Title too long for ticket {ticket_id}: '{title}' (length: {len(title)}). Must be at most 100 characters")
            
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

def populate_email_addresses(ticket_record, assigned_team_email):
    """Populate empty email addresses in participants and messages based on assigned team"""
    updates = {}
    
    # Get the customer email from the first participant
    customer_email = None
    customer_name = None
    if ticket_record.get('thread', {}).get('participants'):
        for participant in ticket_record['thread']['participants']:
            if participant.get('type') == 'from' and participant.get('email'):
                customer_email = participant['email']
                customer_name = participant.get('name')
                break
    
    # Update participants - fill empty 'to' email
    if ticket_record.get('thread', {}).get('participants'):
        for i, participant in enumerate(ticket_record['thread']['participants']):
            if participant.get('type') == 'to' and not participant.get('email'):
                updates[f'thread.participants.{i}.email'] = assigned_team_email
                updates[f'thread.participants.{i}.name'] = assigned_team_email.split('@')[0].replace('.', ' ').title()
    
    # Update messages - fill empty email addresses based on message pattern
    if ticket_record.get('messages'):
        for msg_idx, message in enumerate(ticket_record['messages']):
            if message.get('headers'):
                # Handle 'from' field - if empty, it should be the team email (company responses)
                if message['headers'].get('from') and len(message['headers']['from']) > 0:
                    from_participant = message['headers']['from'][0]
                    if not from_participant.get('email'):
                        updates[f'messages.{msg_idx}.headers.from.0.email'] = assigned_team_email
                        updates[f'messages.{msg_idx}.headers.from.0.name'] = assigned_team_email.split('@')[0].replace('.', ' ').title()
                
                # Handle 'to' field - if empty, it should be the customer email
                if message['headers'].get('to') and len(message['headers']['to']) > 0:
                    to_participant = message['headers']['to'][0]
                    if not to_participant.get('email') and customer_email:
                        updates[f'messages.{msg_idx}.headers.to.0.email'] = customer_email
                        updates[f'messages.{msg_idx}.headers.to.0.name'] = customer_name
    
    return updates

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
        
        # Prepare update document with only the fields being generated (excluding stages, follow_up_required, urgency which already exist)
        update_doc = {
            "title": ticket_content['title'],
            "description": ticket_content['description'],
            "priority": ticket_content['priority'],
            "assigned_team_email": ticket_content['assigned_team_email'],
            "ticket_summary": ticket_content['ticket_summary'],
            "action_pending_status": ticket_content['action_pending_status'],
            "action_pending_from": ticket_content['action_pending_from'],
            "resolution_status": ticket_content['resolution_status'],
            "follow_up_date": ticket_content['follow_up_date'],
            "follow_up_reason": ticket_content['follow_up_reason'],
            "next_action_suggestion": ticket_content['next_action_suggestion'],
            "sentiment": ticket_content['sentiment'],
            "overall_sentiment": ticket_content['overall_sentiment'],
            "ticket_raised": ticket_content['ticket_raised']
        }
        
        # Add email address updates
        email_updates = populate_email_addresses(ticket_record, ticket_content['assigned_team_email'])
        update_doc.update(email_updates)
        
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
    logger.info(f"OpenRouter Model: {OPENROUTER_MODEL}")
    
    # Test OpenRouter connection
    if not test_openrouter_connection():
        logger.error("Cannot proceed without OpenRouter connection")
        return
    
    # Get all ticket records that need content generation
    logger.info("Fetching ticket records from database...")
    try:
        # Query for tickets that don't have the remaining fields OR have empty email addresses in participants/messages
        query = {
            "$or": [
                {"title": {"$exists": False}},
                {"description": {"$exists": False}},
                {"priority": {"$exists": False}},
                {"ticket_summary": {"$exists": False}},
                {"action_pending_status": {"$exists": False}},
                {"action_pending_from": {"$exists": False}},
                {"resolution_status": {"$exists": False}},
                {"follow_up_date": {"$exists": False}},
                {"follow_up_reason": {"$exists": False}},
                {"next_action_suggestion": {"$exists": False}},
                {"overall_sentiment": {"$exists": False}},
                {"ticket_raised": {"$exists": False}},
                {"title": {"$in": [None, ""]}},
                {"description": {"$in": [None, ""]}},
                {"priority": {"$in": [None, ""]}},
                {"ticket_summary": {"$in": [None, ""]}},
                # Also include tickets with empty email addresses in participants
                {"thread.participants.1.email": {"$in": [None, ""]}},
                # Include tickets with empty email addresses in messages
                {"messages.headers.from.0.email": {"$in": [None, ""]}},
                {"messages.headers.to.0.email": {"$in": [None, ""]}}
            ]
        }
        
        ticket_records = list(ticket_col.find(query))
        total_tickets = len(ticket_records)
        
        if total_tickets == 0:
            logger.info("All tickets already have the remaining fields (excluding stages, follow_up_required, urgency which already exist)!")
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

def test_openrouter_connection():
    """Test if OpenRouter is accessible and model is available"""
    try:
        logger.info(f"Testing connection to OpenRouter: {OPENROUTER_URL}")
        
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'http://localhost:3000',
            'X-Title': 'EU Banking Ticket Generator'
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
        total_count = ticket_col.count_documents({})
        
        # Count records with complete new fields (excluding stages, follow_up_required, urgency which already exist)
        with_all_new_fields = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "assigned_team_email": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_status": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_from": {"$exists": True},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "follow_up_date": {"$exists": True},
            "follow_up_reason": {"$exists": True},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True},
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
            "assigned_team_email": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_status": {"$exists": True, "$ne": "", "$ne": None},
            "action_pending_from": {"$exists": True},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "follow_up_date": {"$exists": True},
            "follow_up_reason": {"$exists": True},
            "next_action_suggestion": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True},
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
                "openrouter_model": OPENROUTER_MODEL
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
    logger.info("EU Banking Trouble Ticket Content Generator - OpenRouter Version Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {TICKET_COLLECTION}")
    logger.info(f"Model: {OPENROUTER_MODEL}")
    logger.info(f"OpenRouter URL: {OPENROUTER_URL}")
    logger.info(f"Using API key authentication")
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