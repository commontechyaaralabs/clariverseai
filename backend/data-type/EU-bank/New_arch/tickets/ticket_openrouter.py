# EU Banking Trouble Ticket Content Generator - OpenRouter Version
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
from concurrent.futures import ThreadPoolExecutor, as_completed
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

# OpenRouter configuration
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Configuration
BATCH_SIZE = 3
MAX_WORKERS = 2
REQUEST_TIMEOUT = 120
MAX_RETRIES = 5
RETRY_DELAY = 3
BATCH_DELAY = 5.0
API_CALL_DELAY = 1.0

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
                json.dump(self.results, f, indent=2, cls=ObjectIdEncoder)
        except Exception as e:
            logger.error(f"Error saving intermediate results: {e}")
    
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
        ticket_col.create_index("thread.subject_norm")
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

def get_department_mapping():
    """Get the department mapping with emails"""
    return {
        'Customer Service Team': 'customerservice@eubank.com',
        'Financial Crime Investigation Team': 'financialcrime@eubank.com',
        'Loan & Credit Card Support Team': 'loansupport@eubank.com',
        'KYC Team': 'kyc@eubank.com',
        'Card Operations Team': 'cardoperations@eubank.com',
        'Digital Banking Support Team': 'digitalbanking@eubank.com',
        'Internal IT Helpdesk': 'internalithelpdesk@eubank.com'
    }

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

def generate_mock_ticket_response(ticket_data):
    """Generate a mock ticket response when OpenRouter is not available - FAIL instead of using predefined content"""
    raise ValueError("OpenRouter API is not available and mock generation is disabled. All content must be generated by LLM.")


def generate_ticket_conversation(ticket_data):
    """Generate initial customer ticket and company conversation responses"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    ticket_id = str(ticket_data.get('_id', 'unknown'))
    
    try:
        # Extract data from ticket record
        dominant_topic = ticket_data.get('dominant_topic', 'General Banking System Issue')
        subtopics = ticket_data.get('subtopics', 'System malfunction')
        customer_name = ticket_data.get('thread', {}).get('participants', [{}])[0].get('name', 'Customer')
        customer_email = ticket_data.get('thread', {}).get('participants', [{}])[0].get('email', 'customer@email.com')
        
        # Get message count from thread - this determines how many conversation responses to generate
        thread_message_count = ticket_data.get('thread', {}).get('message_count', 3)  # Default to 3 if not specified
        conversation_responses_count = max(1, thread_message_count - 1)  # Subtract 1 for initial message
        
        # Generate realistic banking details
        banking_details = generate_realistic_banking_details()
        
        # Generate ticket raised date
        ticket_raised = generate_random_ticket_raised_date()
        
        # Get department mapping for the prompt
        department_mapping = get_department_mapping()
        
        # Generate dynamic conversation responses template based on message count
        conversation_responses_template = ""
        for i in range(conversation_responses_count):
            response_num = i + 1
            if response_num == 1:
                response_type = "First response from assigned team (acknowledgment/investigation)"
            elif response_num == 2:
                response_type = "Follow-up response (status update/progress)"
            else:
                response_type = f"Response {response_num} (resolution/escalation/closure)"
            
            conversation_responses_template += f"""    {{
      "message": "{response_type} in formal trouble ticket format (MUST BE 150-250 words, NO email greetings/signatures, NO prefixes)",
      "from_type": "customer/team",
      "sentiment": "MUST BE numeric value 0-5 (analyze the emotional tone of this specific message)"
    }}"""
            if i < conversation_responses_count - 1:
                conversation_responses_template += ","
            conversation_responses_template += "\n"
        
        # Enhanced prompt for generating complete trouble ticket conversation
        prompt = """
Generate a realistic EU banking trouble ticket conversation for a trouble ticket dataset. This is NOT an email conversation - it's a formal trouble ticket system with structured messages.

**CRITICAL: ALL DATES MUST BE BETWEEN 2025-01-01 AND 2025-06-30 ONLY!**
**NEVER USE SEPTEMBER, OCTOBER, NOVEMBER, OR DECEMBER 2025!**

Generate a realistic EU banking trouble ticket conversation for a trouble ticket dataset. This is NOT an email conversation - it's a formal trouble ticket system with structured messages.
**TROUBLE TICKET FORMAT REQUIREMENTS - CRITICAL:**
- **NO EMAIL FORMAT**: Do NOT use "Dear", "Sincerely", "Best regards", or email signatures
- **NO PREFIXES**: Do NOT use "Issue Description:", "Problem Report:", "Alert:", or "System Issue:" prefixes
- **DIRECT START**: Start directly with the actual problem description without any introductory text
- **TICKET STYLE**: Write in direct, professional trouble ticket format as if logging into a system
- **RESPONSES**: Use "Response:", "Update:", "Status:", or "Resolution:" format
- **DIRECT LANGUAGE**: Be concise, factual, and solution-focused
- **NO GREETINGS**: Skip "Hello", "Hi", "Dear" - go straight to the issue
- **NO SIGNATURES**: No "Thanks", "Best regards", or contact information at the end
- **WORD COUNT**: Initial message MUST BE 300-400 WORDS, conversation responses MUST BE 150-250 WORDS
- **SENTIMENT**: All sentiment values MUST BE numeric (0-5), not text descriptions
**Context Analysis:**
Topic: {dominant_topic}
Subtopics: {subtopics}
Customer: {customer_name} ({customer_email})
Total Messages Required: {thread_message_count} (1 initial message + {conversation_responses_count} conversation responses)

**TROUBLE TICKET DATASET REQUIREMENTS:**
- This is a formal trouble ticket system, NOT email communication
- Generate realistic banking support scenarios with proper escalation
- Each message should be a formal ticket update or response
- Focus on technical banking issues, customer complaints, and support resolution

**Available System Details:**
Ticket: {ticket_number} | System: {system_name} | Server: {server_name}
Account: {account_number} | Customer ID: {customer_id} | Branch: {branch_code}
Transaction: {transaction_id} | Amount: {currency} {amount}
Error: {error_code} | IP: {ip_address} | Time: {date} {time}

**Database Structure Context:**
The ticket will be stored in MongoDB with this structure:
- provider: "ticket_system"
- thread: Contains thread_id, subject_norm, participants, message_count
- messages: Array of message objects with provider_ids, headers, body
- Each message has headers with date, ticket_title, from, to arrays
- The first message is the initial ticket (customer or internal)
- Subsequent messages are responses between customer and assigned team

**Department Assignment:**
Based on the topic analysis, determine which department should handle this ticket:

Available Departments:
- Customer Service Team (customerservice@eubank.com) - General inquiries, complaints, account access issues, app problems, general banking services
- Financial Crime Investigation Team (financialcrime@eubank.com) - Fraud, suspicious activities, unauthorized transactions, security breaches, money laundering
- Loan & Credit Card Support Team (loansupport@eubank.com) - Loan applications, credit card issues, mortgage problems, debt management, credit-related services
- KYC Team (kyc@eubank.com) - Identity verification, document submission, compliance, regulatory requirements, customer verification
- Card Operations Team (cardoperations@eubank.com) - Card transactions, ATM issues, POS problems, card activation, PIN issues, merchant disputes
- Digital Banking Support Team (digitalbanking@eubank.com) - Online banking, mobile app, digital services, API issues, technical platform problems
- Internal IT Helpdesk (internalithelpdesk@eubank.com) - System outages, server problems, database issues, internal technical problems, infrastructure maintenance

**Department Selection Guidelines:**
- Customer Service: General inquiries, complaints, account access, app issues, general banking questions
- Financial Crime: Fraud, suspicious activities, unauthorized access, security incidents, compliance violations
- Loan & Credit: Loan applications, credit card problems, mortgage issues, debt management, credit services
- KYC: Identity verification, document requirements, compliance, regulatory submissions, customer verification
- Card Operations: Card transactions, ATM problems, POS issues, card activation, PIN problems, merchant issues
- Digital Banking: Online banking problems, mobile app issues, digital platform problems, API issues
- Internal IT: System outages, server problems, database issues, internal technical problems, maintenance

**Ticket Source Detection:**
- Customer issues: Login problems, card issues, account access, app problems, transaction disputes, general inquiries
- Internal issues: System failures, server problems, database issues, compliance alerts, security incidents, infrastructure problems

**INSTRUCTIONS:**
1. **Analyze Topic**: Determine if this is customer-reported or internal company issue
2. **Assign Team**: Select the most appropriate department based on topic analysis
3. **Generate Ticket Title**: Create a professional, descriptive trouble ticket title
4. **First Message**: Generate the initial trouble ticket (customer complaint or internal alert)
5. **Conversation Flow**: Generate exactly {conversation_responses_count} realistic trouble ticket responses between customer and assigned team
6. **Use Realistic Details**: Incorporate provided banking details naturally in trouble ticket format
7. **Professional Language**: All responses should be formal, technical, and solution-oriented for trouble ticket system
8. **Message Count**: CRITICAL - Generate exactly {thread_message_count} total messages (1 initial + {conversation_responses_count} responses)
9. **Trouble Ticket Style**: Each message should be a formal ticket update, not casual conversation

**LOGICAL THINKING REQUIREMENTS:**
10. **Action Pending Logic**: Think about who needs to take action next - company (if waiting for internal resolution) or customer (if waiting for customer response)
11. **Follow-up Logic**: Determine if this issue genuinely needs follow-up based on complexity and resolution status
12. **Follow-up Date Logic**: Only set follow_up_date if follow_up_required is "yes", otherwise set to null
13. **Follow-up Reason Logic**: Only set follow_up_reason if follow_up_required is "yes", otherwise set to null
14. **Resolution Status Logic**: Set appropriate status based on conversation progress and issue complexity
15. **Date Generation Logic**: Generate realistic dates within the conversation timeline - follow_up_date should be after ticket_raised

**CRITICAL DATE RANGE REQUIREMENTS - MUST BE FOLLOWED:**
- **ALL DATES MUST BE BETWEEN 2025-01-01 AND 2025-06-30 ONLY**
- **NEVER USE DATES IN SEPTEMBER, OCTOBER, NOVEMBER, OR DECEMBER 2025**
- **ticket_raised**: Must be between 2025-01-01T00:00:00Z and 2025-06-30T23:59:59Z
- **follow_up_date**: Must be between 2025-01-01T00:00:00Z and 2025-06-30T23:59:59Z (only if follow_up_required is "yes")
- **VALID MONTHS**: January, February, March, April, May, June 2025 ONLY
- **INVALID MONTHS**: July, August, September, October, November, December 2025

**INVALID DATE EXAMPLES (DO NOT USE):**
- **ticket_raised**: "2025-09-12T14:30:00Z" (September 2025 - INVALID)
- **ticket_raised**: "2025-07-15T10:30:00Z" (July 2025 - INVALID)
- **follow_up_date**: "2025-08-20T14:45:00Z" (August 2025 - INVALID)

**LOGICAL EXAMPLES:**
- **Simple Issue (login problem)**: follow_up_required="no", follow_up_date=null, follow_up_reason=null
- **Complex Issue (fraud investigation)**: follow_up_required="yes", follow_up_date="2025-03-15T14:30:00Z", follow_up_reason="Monitor investigation progress"
- **Resolved Issue**: follow_up_required="no", follow_up_date=null, follow_up_reason=null, resolution_status="closed"
- **Waiting for Customer**: action_pending_from="customer", action_pending_status="yes"
- **Internal Investigation**: action_pending_from="company", action_pending_status="yes"

**CONVERSATION FLOW REQUIREMENTS:**
- **Message 1**: Initial ticket (customer complaint or internal alert)
- **Message 2**: First response from assigned team (acknowledgment/investigation)
- **Message 3**: Follow-up response (status update/progress)
- **Message 4+**: Additional responses as needed (resolution/escalation/closure)
- **Alternating Pattern**: Customer and team should alternate responses naturally
- **Progressive Resolution**: Each message should advance the ticket toward resolution
- **Realistic Escalation**: Include appropriate escalation if needed based on issue complexity
- **SENTIMENT TRACKING**: Each response MUST include sentiment analysis (0-5 scale)

**Required Output Format:**
You MUST return a JSON object with these exact fields:

{{
  "ticket_source": "customer/internal",
  "assigned_team": "Selected department name",
  "assigned_team_email": "Corresponding email",
  "ticket_title": "Professional ticket title (50-100 chars)",
  "initial_message": "The initial trouble ticket - customer complaint or internal alert in formal trouble ticket format (MUST BE 300-400 WORDS, NO email greetings/signatures, NO prefixes like 'Issue Description:')",
  "conversation_responses": [
    {conversation_responses_template}
  ],
  "priority": "P1-P5 with description",
  "urgency": true/false,
  "stages": "Current stage from: Receive|Authenticate|Categorize|Resolution|Escalation|Update|Resolved|Close|Report",
  "ticket_summary": "Summary of the entire conversation (100-150 words)",
  "action_pending_status": "yes/no",
  "action_pending_from": "company/customer or null (think logically - who needs to take action?)",
  "resolution_status": "open/inprogress/closed",
  "follow_up_required": "yes/no (think logically - does this issue need follow-up?)",
  "follow_up_date": "ISO timestamp between 2025-01-01T00:00:00Z and 2025-06-30T23:59:59Z or null (only if follow_up_required is yes, otherwise null)",
  "follow_up_reason": "Reason for follow-up or null (only if follow_up_required is yes, otherwise null)",
  "next_action_suggestion": "AI recommendation (30-50 words)",
  "overall_sentiment": 0-5 sentiment scale,
  "ticket_raised": "ISO timestamp between 2025-01-01T00:00:00Z and 2025-06-30T23:59:59Z"
}}

**Priority Guidelines:**
- P1 - Critical: System outage, security breach, compliance failure
- P2 - High: Major functionality impaired, business operations disrupted  
- P3 - Medium: Moderate impact, workaround available
- P4 - Low: Minor impact, minimal business disruption
- P5 - Very Low: Cosmetic issues, enhancement requests

**SENTIMENT ANALYSIS REQUIREMENTS - CRITICAL:**
- **MANDATORY**: Each conversation response MUST include a sentiment score (0-5)
- **MANDATORY**: overall_sentiment MUST be provided as a numeric value (0-5)
- **Individual Message Analysis**: Analyze the emotional tone of EACH message separately
- **Context-Aware**: Consider the conversation flow and escalation level
- **Realistic Progression**: Sentiment may change as the conversation progresses
- **NO STRING VALUES**: Sentiment must be numeric, not text descriptions

**sentiment**: Individual message sentiment analysis using human emotional tone (0-5 scale):
- 0: Happy (pleased, satisfied, positive)
- 1: Calm (baseline for professional communication)
- 2: Bit Irritated (slight annoyance or impatience)
- 3: Moderately Concerned (growing unease or worry)
- 4: Anger (clear frustration or anger)
- 5: Frustrated (extreme frustration, very upset)

**Stages:**
- Receive: Accept and log the request or issue (customer or internal)
- Authenticate: Verify the identity or validity of the requester/source
- Categorize: Classify the request by type, priority, or responsible team
- Resolution: Carry out the actions needed to solve the issue/task
- Escalation: Forward the issue/task to higher authority or specialized team
- Update: Communicate progress or status to the requester (customer/internal)
- Resolved: Mark the issue/task as successfully completed
- Close: Formally finalize and close the case or task after confirmation
- Report: Document and analyze the outcomes for insights and improvement

Generate realistic, professional content that reflects actual EU banking customer service standards. Use the provided banking details naturally within the conversation context.

**CRITICAL:** Return ONLY the JSON object with realistic values. No other text or formatting.
""".format(
            dominant_topic=dominant_topic,
            subtopics=subtopics,
            customer_name=customer_name,
            customer_email=customer_email,
            thread_message_count=thread_message_count,
            conversation_responses_count=conversation_responses_count,
            conversation_responses_template=conversation_responses_template,
            ticket_number=banking_details['ticket_number'],
            system_name=banking_details['system_name'],
            server_name=banking_details['server_name'],
            account_number=banking_details['account_number'],
            customer_id=banking_details['customer_id'],
            branch_code=banking_details['branch_code'],
            transaction_id=banking_details['transaction_id'],
            currency=banking_details['currency'],
            amount=banking_details['amount'],
            error_code=banking_details['error_code'],
            ip_address=banking_details['ip_address'],
            date=banking_details['date'],
            time=banking_details['time'],
            ticket_raised=ticket_raised
        ).strip()

        # Generate content using OpenRouter LLM only - no fallbacks
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
            logger.info("Successfully generated ticket using OpenRouter API")
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
        # Debug logging
        logger.info(f"LLM returned fields for ticket {ticket_id}: {list(result.keys())}")
        
        # Get department mapping for validation
        department_mapping = get_department_mapping()
        
        # Validate required fields exist - fail if missing
        required_fields = [
            'ticket_source', 'assigned_team', 'assigned_team_email', 'ticket_title',
            'initial_message', 'conversation_responses', 'priority', 'urgency',
            'stages', 'ticket_summary', 'action_pending_status', 'action_pending_from',
            'resolution_status', 'follow_up_required', 'follow_up_date', 'follow_up_reason',
            'next_action_suggestion', 'overall_sentiment', 'ticket_raised'
        ]
        
        missing_fields = []
        for field in required_fields:
            if field not in result:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Missing required fields in LLM response: {missing_fields}")
        
        # Validate initial message word count - must be 300-400 words
        initial_message = result.get('initial_message', '')
        word_count = len(initial_message.split())
        if word_count < 300:
            raise ValueError(f"Initial message too short: {word_count} words (minimum 300 required)")
        if word_count > 400:
            raise ValueError(f"Initial message too long: {word_count} words (maximum 400 allowed)")
        
        # Validate that initial message doesn't start with prefixes
        initial_message_lower = initial_message.lower().strip()
        forbidden_prefixes = ['issue description:', 'problem report:', 'alert:', 'system issue:']
        if any(initial_message_lower.startswith(prefix) for prefix in forbidden_prefixes):
            raise ValueError(f"Initial message starts with forbidden prefix. Message starts with: {initial_message[:50]}...")
        
        # Validate logical consistency for conditional fields
        if result.get('follow_up_required') == 'no' and result.get('follow_up_date') is not None:
            result['follow_up_date'] = None
            logger.warning(f"Fixed logical inconsistency: follow_up_required=no but follow_up_date was not null")
        
        if result.get('follow_up_required') == 'no' and result.get('follow_up_reason') is not None:
            result['follow_up_reason'] = None
            logger.warning(f"Fixed logical inconsistency: follow_up_required=no but follow_up_reason was not null")
        
        if result.get('follow_up_required') == 'yes' and result.get('follow_up_date') is None:
            logger.warning(f"Logical inconsistency: follow_up_required=yes but follow_up_date is null")
        
        if result.get('follow_up_required') == 'yes' and result.get('follow_up_reason') is None:
            logger.warning(f"Logical inconsistency: follow_up_required=yes but follow_up_reason is null")
        
        # Validate date ranges - must be between 2025-01-01 and 2025-06-30
        from datetime import datetime
        
        def validate_date_range(date_str, field_name):
            if date_str is None:
                return True
            try:
                # Handle both Z and +00:00 timezone formats
                if date_str.endswith('Z'):
                    date_str = date_str.replace('Z', '+00:00')
                elif '+' not in date_str and '-' not in date_str[-6:]:
                    # If no timezone info, assume UTC
                    date_str = date_str + '+00:00'
                
                date_obj = datetime.fromisoformat(date_str)
                start_date = datetime(2025, 1, 1)
                end_date = datetime(2025, 6, 30, 23, 59, 59)
                
                # Convert to UTC for comparison if timezone-aware
                if date_obj.tzinfo is not None:
                    date_obj = date_obj.replace(tzinfo=None)
                
                if not (start_date <= date_obj <= end_date):
                    raise ValueError(f"{field_name} date {date_str} is outside allowed range (2025-01-01 to 2025-06-30)")
                return True
            except (ValueError, TypeError) as e:
                raise ValueError(f"Invalid {field_name} date format: {date_str} - Error: {e}")
        
        # Validate ticket_raised date
        validate_date_range(result.get('ticket_raised'), 'ticket_raised')
        
        # Validate follow_up_date if present
        validate_date_range(result.get('follow_up_date'), 'follow_up_date')
        
        # Validate field values - fail if invalid
        valid_priorities = ['P1 - Critical', 'P2 - High', 'P3 - Medium', 'P4 - Low', 'P5 - Very Low']
        if not any(priority in result['priority'] for priority in ['P1', 'P2', 'P3', 'P4', 'P5']):
            raise ValueError(f"Invalid priority value: {result['priority']}")
        
        valid_stages = [
            'Receive', 'Authenticate', 'Categorize', 'Resolution', 
            'Escalation', 'Update', 'Resolved', 'Close', 'Report'
        ]
        if result['stages'] not in valid_stages:
            raise ValueError(f"Invalid stages value: {result['stages']}")
        
        valid_resolution_status = ['open', 'inprogress', 'closed']
        if result['resolution_status'] not in valid_resolution_status:
            raise ValueError(f"Invalid resolution_status value: {result['resolution_status']}")
        
        # Validate urgency as boolean
        if not isinstance(result['urgency'], bool):
            raise ValueError(f"Invalid urgency value: {result['urgency']} (must be boolean)")
        
        # Validate assigned team
        if result['assigned_team'] not in department_mapping:
            raise ValueError(f"Invalid assigned team: {result['assigned_team']}")
        
        # Ensure email matches the department
        result['assigned_team_email'] = department_mapping[result['assigned_team']]
        
        # Validate conversation responses structure - fail if invalid
        if not isinstance(result['conversation_responses'], list):
            raise ValueError("conversation_responses must be a list")
        
        # Validate correct number of conversation responses
        if len(result['conversation_responses']) != conversation_responses_count:
            raise ValueError(f"Expected {conversation_responses_count} conversation responses, got {len(result['conversation_responses'])}")
        
        for i, response in enumerate(result['conversation_responses']):
            if not isinstance(response, dict):
                raise ValueError(f"conversation_responses[{i}] must be a dictionary")
            
            # Validate required fields in each response
            if 'message' not in response or not response['message']:
                raise ValueError(f"conversation_responses[{i}] missing 'message' field")
            if 'from_type' not in response or response['from_type'] not in ['customer', 'team']:
                raise ValueError(f"conversation_responses[{i}] missing or invalid 'from_type' field")
            if 'sentiment' not in response:
                raise ValueError(f"conversation_responses[{i}] missing 'sentiment' field")
            
            # Validate conversation response word count - must be 150-250 words
            response_message = response['message']
            response_word_count = len(response_message.split())
            if response_word_count < 150:
                raise ValueError(f"conversation_responses[{i}] too short: {response_word_count} words (minimum 150 required)")
            if response_word_count > 250:
                raise ValueError(f"conversation_responses[{i}] too long: {response_word_count} words (maximum 250 allowed)")
            
            # Validate that response doesn't start with forbidden prefixes
            response_message_lower = response_message.lower().strip()
            if any(response_message_lower.startswith(prefix) for prefix in forbidden_prefixes):
                raise ValueError(f"conversation_responses[{i}] starts with forbidden prefix. Message starts with: {response_message[:50]}...")
            
            # Validate sentiment range
            try:
                # Handle both string and numeric sentiment values
                sentiment_value = response['sentiment']
                if isinstance(sentiment_value, str):
                    # Extract number from string like "0-5 (analyze...)" or just "3"
                    import re
                    numbers = re.findall(r'\d+\.?\d*', sentiment_value)
                    if numbers:
                        sentiment = float(numbers[0])
                    else:
                        raise ValueError(f"Could not extract number from sentiment string: {sentiment_value}")
                else:
                    sentiment = float(sentiment_value)
                
                if not (0.0 <= sentiment <= 5.0):
                    raise ValueError(f"conversation_responses[{i}] sentiment must be between 0.0 and 5.0, got: {sentiment}")
                response['sentiment'] = round(sentiment, 1)
            except (ValueError, TypeError) as e:
                raise ValueError(f"conversation_responses[{i}] invalid sentiment value: {response['sentiment']} - Error: {e}")
        
        # Validate overall_sentiment range - fail if invalid
        try:
            sentiment_value = result['overall_sentiment']
            if isinstance(sentiment_value, str):
                # Extract number from string if needed
                import re
                numbers = re.findall(r'\d+\.?\d*', sentiment_value)
                if numbers:
                    sentiment = float(numbers[0])
                else:
                    raise ValueError(f"Could not extract numeric sentiment from string: {sentiment_value}")
            else:
                sentiment = float(sentiment_value)
            
            if not (0.0 <= sentiment <= 5.0):
                raise ValueError(f"overall_sentiment must be between 0.0 and 5.0, got: {sentiment}")
            result['overall_sentiment'] = round(sentiment, 1)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid overall_sentiment value: {result['overall_sentiment']} - Error: {e}")
        
        generation_time = time.time() - start_time
        
        # Log successful generation
        success_info = {
            'ticket_id': ticket_id,
            'dominant_topic': dominant_topic,
            'ticket_source': result['ticket_source'],
            'assigned_team': result['assigned_team'],
            'ticket_title': result['ticket_title'],
            'priority': result['priority'],
            'urgency': result['urgency'],
            'stages': result['stages'],
            'resolution_status': result['resolution_status'],
            'overall_sentiment': result['overall_sentiment'],
            'generation_time': generation_time,
            'message_count': 1 + len(result['conversation_responses'])
        }
        success_logger.info(json.dumps(success_info, cls=ObjectIdEncoder))
        
        return result
        
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
    """Process a single ticket record to generate conversation and analysis"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate ticket conversation and analysis
        ticket_content = generate_ticket_conversation(ticket_record)
        
        if not ticket_content:
            failure_counter.increment()
            return None
        
        # Prepare messages array - first message is the ticket, then conversation responses
        messages = []
        
        # Get assigned team info
        assigned_team = ticket_content['assigned_team']
        assigned_team_email = ticket_content['assigned_team_email']
        ticket_source = ticket_content['ticket_source']
        
        # Get customer info
        customer_info = ticket_record.get('thread', {}).get('participants', [{}])[0]
        
        # First message - the initial ticket/complaint
        if ticket_source == 'customer':
            # Customer raises ticket to assigned team
            first_message = {
                "provider_ids": {
                    "ticket_system": {
                        "id": f"{ticket_record['_id']}_msg_0",
                        "ticket_id": str(ticket_record['_id'])
                    }
                },
                "headers": {
                    "date": ticket_content['ticket_raised'],
                    "ticket_title": ticket_content['ticket_title'],
                    "from": [customer_info],
                    "to": [{"type": "to", "name": assigned_team, "email": assigned_team_email}]
                },
                "body": {
                    "mime_type": "text/plain",
                    "text": {
                        "plain": ticket_content['initial_message']
                    }
                }
            }
        else:
            # Internal team raises ticket to customer
            first_message = {
                "provider_ids": {
                    "ticket_system": {
                        "id": f"{ticket_record['_id']}_msg_0",
                        "ticket_id": str(ticket_record['_id'])
                    }
                },
                "headers": {
                    "date": ticket_content['ticket_raised'],
                    "ticket_title": ticket_content['ticket_title'],
                    "from": [{"name": assigned_team, "email": assigned_team_email, "type": "from"}],
                    "to": [customer_info]
                },
                "body": {
                    "mime_type": "text/plain",
                    "text": {
                        "plain": ticket_content['initial_message']
                    }
                }
            }
        messages.append(first_message)
        
        # Add conversation responses
        for i, response in enumerate(ticket_content['conversation_responses'], 1):
            response_message = response.get('message', str(response))
            from_type = response.get('from_type', 'team')
            
            # Determine from/to based on from_type
            if from_type == 'customer':
                # Customer responding to team
                message_from = [customer_info]
                message_to = [{"type": "to", "name": assigned_team, "email": assigned_team_email}]
                ticket_title = f"RE: {ticket_content['ticket_title']}"
            else:
                # Team responding to customer
                message_from = [{"name": assigned_team, "email": assigned_team_email, "type": "from"}]
                message_to = [customer_info]
                ticket_title = f"RE: {ticket_content['ticket_title']}"
            
            conversation_message = {
                "provider_ids": {
                    "ticket_system": {
                        "id": f"{ticket_record['_id']}_msg_{i}",
                        "ticket_id": str(ticket_record['_id'])
                    }
                },
                "headers": {
                    "date": ticket_content['ticket_raised'],
                    "ticket_title": ticket_title,
                    "from": message_from,
                    "to": message_to
                },
                "body": {
                    "mime_type": "text/plain",
                    "text": {
                        "plain": response_message
                    }
                }
            }
            messages.append(conversation_message)
        
        # Update thread information
        updated_thread = ticket_record.get('thread', {})
        updated_thread['subject_norm'] = ticket_content['ticket_title']
        updated_thread['message_count'] = len(messages)
        
        # Prepare complete update document
        update_doc = {
            # Update messages array
            "messages": messages,
            
            # Update thread with ticket title
            "thread.subject_norm": ticket_content['ticket_title'],
            "thread.message_count": len(messages),
            
            # Analysis fields
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
            'operation_type': 'conversation_generation',
            'ticket_title': ticket_content['ticket_title'],
            'message_count': len(messages),
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
                    if 'messages' in sample_update['update_doc']:
                        logger.info(f"Sample update - ID: {sample_update['ticket_id']}, Messages: {len(sample_update['update_doc']['messages'])}")
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
        # Query for tickets that don't have generated conversation content
        # Use $nor to find records that don't match the complete criteria
        query = {
            "$nor": [
                {
                    "$and": [
                        {"messages.0.body.text.plain": {"$exists": True, "$ne": "", "$ne": None}},
                        {"thread.subject_norm": {"$exists": True, "$ne": "", "$ne": None}},
                        {"priority": {"$exists": True, "$ne": "", "$ne": None}},
                        {"urgency": {"$exists": True, "$ne": "", "$ne": None}},
                        {"stages": {"$exists": True, "$ne": "", "$ne": None}},
                        {"ticket_summary": {"$exists": True, "$ne": "", "$ne": None}},
                        {"resolution_status": {"$exists": True, "$ne": "", "$ne": None}},
                        {"overall_sentiment": {"$exists": True, "$ne": "", "$ne": None}},
                        {"ticket_raised": {"$exists": True, "$ne": "", "$ne": None}},
                        {"ticket_source": {"$exists": True, "$ne": "", "$ne": None}},
                        {"assigned_team": {"$exists": True, "$ne": "", "$ne": None}}
                    ]
                }
            ]
        }
        
        ticket_records = list(ticket_col.find(query))
        total_tickets = len(ticket_records)
        
        if total_tickets == 0:
            logger.info("All tickets already have complete content and conversations!")
            return
        
        # Convert all ObjectId fields to strings to prevent JSON serialization issues
        for record in ticket_records:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
            
        logger.info(f"Found {total_tickets} tickets needing conversation generation")
        progress_logger.info(f"BATCH_START: total_tickets={total_tickets}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching ticket records: {e}")
        return
    
    # Process in batches
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
                            if completed % 2 == 0:
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
            
            # Progress summary every 3 batches
            if batch_num % 3 == 0 or batch_num == total_batches:
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
                time.sleep(BATCH_DELAY)  # Configurable delay between batches
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
            logger.info(f"Final batch saved: {saved_count} records")
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking trouble ticket conversation generation complete!")
            
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
        
        # Count records with complete conversation data
        with_conversations = ticket_col.count_documents({
            "messages.0.body.text.plain": {"$exists": True, "$ne": "", "$ne": None},
            "thread.subject_norm": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None},
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_summary": {"$exists": True, "$ne": "", "$ne": None},
            "resolution_status": {"$exists": True, "$ne": "", "$ne": None},
            "overall_sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_raised": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_source": {"$exists": True, "$ne": "", "$ne": None},
            "assigned_team": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Count urgent tickets
        urgent_tickets = ticket_col.count_documents({
            "urgency": True
        })
        
        without_complete_data = total_count - with_conversations
        
        # Get stage distribution
        stage_pipeline = [
            {"$match": {"stages": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {"_id": "$stages", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        stage_distribution = list(ticket_col.aggregate(stage_pipeline))
        
        # Get priority distribution
        priority_pipeline = [
            {"$match": {"priority": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        priority_distribution = list(ticket_col.aggregate(priority_pipeline))
        
        # Get message count statistics
        message_pipeline = [
            {"$match": {"messages": {"$exists": True, "$type": "array"}}},
            {"$project": {"message_count": {"$size": "$messages"}}},
            {"$group": {
                "_id": None,
                "avg_messages": {"$avg": "$message_count"},
                "min_messages": {"$min": "$message_count"},
                "max_messages": {"$max": "$message_count"},
                "total_conversations": {"$sum": 1}
            }}
        ]
        message_stats = list(ticket_col.aggregate(message_pipeline))
        
        
        logger.info("Collection Statistics:")
        logger.info(f"Total tickets: {total_count}")
        logger.info(f"With complete conversations: {with_conversations}")
        logger.info(f"Without complete data: {without_complete_data}")
        logger.info(f"Urgent tickets: {urgent_tickets} ({(urgent_tickets/total_count)*100:.1f}% of total)" if total_count > 0 else "Urgent tickets: 0")
        logger.info(f"Completion percentage: {(with_conversations/total_count)*100:.1f}%" if total_count > 0 else "Completion percentage: 0%")
        
        if stage_distribution:
            logger.info("Stage Distribution:")
            for stage in stage_distribution:
                logger.info(f"  {stage['_id']}: {stage['count']} tickets")
        
        if priority_distribution:
            logger.info("Priority Distribution:")
            for priority in priority_distribution:
                logger.info(f"  {priority['_id']}: {priority['count']} tickets")
        
        
        if message_stats and message_stats[0]['total_conversations'] > 0:
            stats = message_stats[0]
            logger.info("Conversation Statistics:")
            logger.info(f"  Average messages per conversation: {stats['avg_messages']:.1f}")
            logger.info(f"  Min messages: {stats['min_messages']}")
            logger.info(f"  Max messages: {stats['max_messages']}")
            logger.info(f"  Total conversations: {stats['total_conversations']}")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_conversations={with_conversations}, urgent={urgent_tickets}")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_tickets(limit=2):
    """Get sample tickets with generated conversations"""
    try:
        samples = list(ticket_col.find({
            "messages.0.body.text.plain": {"$exists": True, "$ne": "", "$ne": None},
            "thread.subject_norm": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "stages": {"$exists": True, "$ne": "", "$ne": None},
            "ticket_source": {"$exists": True, "$ne": "", "$ne": None},
            "assigned_team": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Ticket Conversations:")
        for i, ticket in enumerate(samples, 1):
            logger.info(f"--- Sample Ticket {i} ---")
            logger.info(f"Ticket ID: {ticket.get('_id', 'N/A')}")
            logger.info(f"Title: {ticket.get('thread', {}).get('subject_norm', 'N/A')}")
            logger.info(f"Dominant Topic: {ticket.get('dominant_topic', 'N/A')}")
            logger.info(f"Ticket Source: {ticket.get('ticket_source', 'N/A')}")
            logger.info(f"Assigned Team: {ticket.get('assigned_team', 'N/A')}")
            logger.info(f"Priority: {ticket.get('priority', 'N/A')}")
            logger.info(f"Urgency: {ticket.get('urgency', 'N/A')}")
            logger.info(f"Stage: {ticket.get('stages', 'N/A')}")
            logger.info(f"Resolution Status: {ticket.get('resolution_status', 'N/A')}")
            logger.info(f"Overall Sentiment: {ticket.get('overall_sentiment', 'N/A')}")
            logger.info(f"Message Count: {len(ticket.get('messages', []))}")
            logger.info(f"Ticket Raised: {ticket.get('ticket_raised', 'N/A')}")
            
            # Show first message (ticket content)
            messages = ticket.get('messages', [])
            if messages:
                first_msg = messages[0].get('body', {}).get('text', {}).get('plain', '')
                first_from = messages[0].get('headers', {}).get('from', [{}])[0]
                first_to = messages[0].get('headers', {}).get('to', [{}])[0]
                logger.info(f"Initial Ticket (From: {first_from.get('name', 'N/A')} To: {first_to.get('name', 'N/A')}): {first_msg[:200]}...")
                
                # Show conversation responses
                for j, msg in enumerate(messages[1:], 1):
                    response = msg.get('body', {}).get('text', {}).get('plain', '')
                    response_from = msg.get('headers', {}).get('from', [{}])[0]
                    response_to = msg.get('headers', {}).get('to', [{}])[0]
                    ticket_title = msg.get('headers', {}).get('ticket_title', 'N/A')
                    logger.info(f"Response {j} (From: {response_from.get('name', 'N/A')} To: {response_to.get('name', 'N/A')}) [{ticket_title}]: {response[:150]}...")
            
            logger.info(f"Ticket Summary: {str(ticket.get('ticket_summary', 'N/A'))[:200]}...")
        
    except Exception as e:
        logger.error(f"Error getting sample tickets: {e}")

# Main execution function
def main():
    """Main function to initialize and run the trouble ticket conversation generator"""
    logger.info("EU Banking Trouble Ticket Conversation Generator - OpenRouter Version Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {TICKET_COLLECTION}")
    logger.info(f"Model: {OPENROUTER_MODEL}")
    logger.info(f"OpenRouter API URL: {OPENROUTER_URL}")
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
        
        # Generate conversations and analysis
        logger.info("=" * 80)
        logger.info("GENERATING TICKET CONVERSATIONS AND ANALYSIS")
        logger.info("=" * 80)
        update_tickets_with_content_parallel()
        
        # Show final statistics
        logger.info("=" * 80)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 80)
        get_collection_stats()
        
        # Show sample generated content
        get_sample_generated_tickets()
        
        logger.info("=" * 80)
        logger.info("FINAL SESSION REPORT")
        logger.info("=" * 80)
        success_rate = (success_counter.value / (success_counter.value + failure_counter.value)) * 100 if (success_counter.value + failure_counter.value) > 0 else 0
        logger.info(f"Success Rate: {success_rate:.2f}%")
        logger.info(f"Total Conversations Generated: {success_counter.value}")
        logger.info(f"Total Database Updates: {update_counter.value}")
        
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

# Run the conversation generator
if __name__ == "__main__":
    main()