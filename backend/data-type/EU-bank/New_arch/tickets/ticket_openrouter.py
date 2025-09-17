# EU Banking Trouble Ticket Content Generator - Updated Version with Title Generation
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

# Ollama setup - Updated to use Cloudflare Tunnel endpoint
OLLAMA_BASE_URL = "https://sleeve-applying-sri-tells.trycloudflare.com"
OLLAMA_TOKEN = "d5823ebcd546e7c6b61a0abebe1d8481d6acb2587b88d1cadfbe651fc4f6c6d5"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"
OLLAMA_MODEL = "gemma3:27b"

# Optimized batch processing configuration
BATCH_SIZE = 10  # Process 10 records per batch as requested
CPU_COUNT = multiprocessing.cpu_count()
MAX_WORKERS = min(CPU_COUNT * 2, 16)  # Reduced for stability with batch size of 10
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_DELAY = 1

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

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=180,
    base=RETRY_DELAY,
    on_backoff=lambda details: logger.warning(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_ollama_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call Ollama API with exponential backoff and better error handling"""
    if shutdown_flag.is_set():
        raise KeyboardInterrupt("Shutdown requested")
    
    # Prepare headers for remote endpoint
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {OLLAMA_TOKEN}' if OLLAMA_TOKEN else None
    }
    # Remove None values
    headers = {k: v for k, v in headers.items() if v is not None}
        
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.4,
            "num_predict": 1500,
            "top_k": 30,
            "top_p": 0.9,
            "num_ctx": 6144
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

def generate_title_from_description(description, dominant_topic=None):
    """Generate a concise title from a description (50-100 words)"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    
    try:
        # Enhanced prompt for title generation
        prompt = f"""
Based on the following EU banking trouble ticket description, generate a concise and professional title.

**Description:**
{description}

**Additional Context:**
{f"Dominant Topic: {dominant_topic}" if dominant_topic else ""}

**Title Generation Requirements:**
1. Create a concise title that is 50-100 characters long (NOT words)
2. Must clearly identify the main technical issue
3. Should include the affected system or component when mentioned
4. Use professional IT service management terminology
5. Be specific and actionable
6. Include urgency indicators if critical (e.g., "CRITICAL:", "URGENT:")
7. Follow format: [System/Component] - [Issue Type] - [Brief Description]

**Examples of good titles:**
- "CoreBanking - Transaction Processing Failure - Account 123456789"
- "ATM Network - Hardware Malfunction - ATM-1234 Offline"
- "SWIFT Gateway - Connection Timeout - Payment Processing Delayed"
- "OnlineBanking - Authentication Service Down - Multiple Users Affected"
- "CRITICAL: PaymentHub - Database Connection Lost - All Transactions Halted"

**Output Format:**
Return ONLY a JSON object with this exact field:
{{
  "title": "Generated title here (50-100 characters)"
}}

Generate the title now:
""".strip()

        response = call_ollama_with_backoff(prompt, timeout=60)
        
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
            
            # Validate required field
            if 'title' not in result:
                raise ValueError("Missing required field: title")
            
            title = result['title'].strip()
            
            # Validate title length (50-100 characters)
            if len(title) < 50:
                logger.warning(f"Title too short ({len(title)} chars): {title[:50]}...")
                # Pad with additional context if needed
                if dominant_topic and len(title) + len(dominant_topic) + 3 <= 100:
                    title = f"{title} - {dominant_topic}"
            elif len(title) > 100:
                logger.warning(f"Title too long ({len(title)} chars), truncating: {title[:50]}...")
                title = title[:97] + "..."
            
            result['title'] = title
            
            generation_time = time.time() - start_time
            
            return result
            
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
    except Exception as e:
        generation_time = time.time() - start_time
        logger.error(f"Title generation error: {e}")
        raise

def generate_eu_banking_ticket_content(ticket_data):
    """Generate EU banking trouble ticket content based on dominant topic and subtopics"""
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
        
        # Enhanced prompt for EU banking trouble tickets
        prompt = f"""
Generate a professional EU banking trouble ticket based on the following context:

**Ticket Context:**
- Dominant Topic: {dominant_topic}
- Subtopics: {subtopics}

**Banking System Details to Use (when relevant to the issue):**
- Ticket Number: {banking_details['ticket_number']}
- Incident ID: {banking_details['incident_id']}
- System Name: {banking_details['system_name']}
- Server Name: {banking_details['server_name']}
- Error Code: {banking_details['error_code']}
- IP Address: {banking_details['ip_address']}
- Account Number: {banking_details['account_number']}
- Transaction ID: {banking_details['transaction_id']}
- Amount: {banking_details['currency']} {banking_details['amount']}
- Branch Code: {banking_details['branch_code']}
- Customer ID: {banking_details['customer_id']}
- ATM ID: {banking_details['atm_id']}
- Terminal ID: {banking_details['terminal_id']}
- Date/Time: {banking_details['date']} at {banking_details['time']}
- SWIFT Code: {banking_details['swift_code']}
- IBAN: {banking_details['iban']}

**CRITICAL INSTRUCTIONS FOR TECHNICAL DETAILS:**
- NEVER use placeholders like [Account Number], [System Name], or [Error Code]
- ALWAYS use the specific technical details provided above when relevant to the ticket
- Make the details feel natural and integrated into the technical description
- Use realistic European banking system terminology and formats
- Include specific details that make the ticket authentic and technical

**EU Banking Compliance & Regulatory Context:**
- Reference relevant EU regulations (GDPR, PSD2, CRD IV, MiFID II) where applicable
- Consider EBA (European Banking Authority) guidelines for system availability
- Include ECB (European Central Bank) requirements for payment systems
- Mention SEPA (Single Euro Payments Area) for payment-related issues
- Reference Basel III operational risk requirements where appropriate
- Consider EU Cybersecurity Framework for security incidents

**Priority Guidelines:**
- P1 - Critical: Complete system outage, security breach, regulatory compliance failure, significant financial impact
- P2 - High: Major functionality impaired, multiple users affected, business operations disrupted
- P3 - Medium: Moderate impact, some users affected, workaround available
- P4 - Low: Minor impact, few users affected, minimal business disruption
- P5 - Very Low: Cosmetic issues, enhancement requests, minimal impact

**Urgency Guidelines:**
- Critical: Immediate action required, escalate to senior management
- High: Action required within 4 hours, inform stakeholders
- Medium: Action required within 24 hours, standard process
- Low: Action required within 72 hours, routine handling

**Ticket Generation Instructions:**
1. Generate a descriptive title (50-100 characters) that clearly identifies the technical issue
2. Create a detailed technical description (200-300 words) that:
   - Starts with incident detection details (monitoring alert, user report, system check)
   - Describes the specific technical problem using REAL system details from above
   - Includes affected systems, users, or business processes
   - Mentions specific error messages, codes, or symptoms using PROVIDED details
   - Details business impact and affected services
   - Suggests initial troubleshooting steps or escalation path
   - References relevant EU banking regulations if applicable
   - Uses professional IT service management terminology
   - Includes timestamps, system names, and technical identifiers
3. Assign appropriate priority based on business impact and affected systems
4. Assign appropriate urgency based on time sensitivity and regulatory requirements

**Examples of proper technical detail usage:**
- "System {banking_details['system_name']} on server {banking_details['server_name']} reported error {banking_details['error_code']}"
- "Transaction {banking_details['transaction_id']} for account {banking_details['account_number']} failed processing"
- "ATM {banking_details['atm_id']} at branch {banking_details['branch_code']} is experiencing hardware malfunction"
- "Payment processing for IBAN {banking_details['iban']} encountered SWIFT gateway timeout"

**Output Format:**
Return ONLY a JSON object with these exact fields:
{{
  "title": "Concise professional title (50-100 characters)",
  "description": "Detailed technical description of the trouble ticket with REAL system details, not placeholders. Include incident detection, technical symptoms, business impact, and next steps.",
  "priority": "P1 - Critical" | "P2 - High" | "P3 - Medium" | "P4 - Low" | "P5 - Very Low",
  "urgency": "Critical" | "High" | "Medium" | "Low"
}}

Generate the trouble ticket content now with REALISTIC technical details integrated naturally into the description.
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
            
            # Validate required fields
            required_fields = ['title', 'description', 'priority', 'urgency']
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            # Validate title length
            title = result['title'].strip()
            if len(title) < 50:
                if dominant_topic and len(title) + len(dominant_topic) + 3 <= 100:
                    result['title'] = f"{title} - {dominant_topic}"
            elif len(title) > 100:
                result['title'] = title[:97] + "..."
            
            # Validate priority format
            valid_priorities = ['P1 - Critical', 'P2 - High', 'P3 - Medium', 'P4 - Low', 'P5 - Very Low']
            if result['priority'] not in valid_priorities:
                logger.warning(f"Invalid priority '{result['priority']}' for ticket {ticket_id}, defaulting to P3 - Medium")
                result['priority'] = 'P3 - Medium'
            
            # Validate urgency format
            valid_urgencies = ['Critical', 'High', 'Medium', 'Low']
            if result['urgency'] not in valid_urgencies:
                logger.warning(f"Invalid urgency '{result['urgency']}' for ticket {ticket_id}, defaulting to Medium")
                result['urgency'] = 'Medium'
            
            # Validate that no placeholders exist in the content
            content_to_check = result['description'] + result['title']
            placeholder_indicators = ['[', ']', 'placeholder', 'PLACEHOLDER', 'example', 'EXAMPLE']
            
            for indicator in placeholder_indicators:
                if indicator in content_to_check:
                    logger.warning(f"Potential placeholder detected in content for {ticket_id}")
                    break
            
            generation_time = time.time() - start_time
            
            # Log successful generation
            success_info = {
                'ticket_id': ticket_id,
                'dominant_topic': dominant_topic,
                'title': result['title'],
                'priority': result['priority'],
                'urgency': result['urgency'],
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

def process_single_title_generation(ticket_record):
    """Process a single ticket record to generate title from existing description"""
    if shutdown_flag.is_set():
        return None
        
    try:
        description = ticket_record.get('description', '')
        dominant_topic = ticket_record.get('dominant_topic', '')
        
        if not description or len(description.strip()) < 50:
            logger.warning(f"Skipping title generation for {ticket_record.get('_id')} - insufficient description")
            return None
        
        # Generate title based on existing description
        title_content = generate_title_from_description(description, dominant_topic)
        
        if not title_content:
            failure_counter.increment()
            return None
        
        # Prepare update document - only title
        update_doc = {
            "title": title_content['title']
        }
        
        title_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'ticket_id': str(ticket_record['_id']),
            'update_doc': update_doc,
            'operation_type': 'title_generation',
            'original_data': {
                'dominant_topic': ticket_record.get('dominant_topic'),
                'description_preview': description[:100] + '...' if len(description) > 100 else description
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'ticket_id': str(ticket_record['_id']),
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Title generation error for {ticket_record.get('_id', 'unknown')}: {str(e)[:100]}")
        failure_counter.increment()
        return None

def process_single_ticket_update(ticket_record):
    """Process a single ticket record to generate description, priority, and urgency"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate ticket content based on existing data
        ticket_content = generate_eu_banking_ticket_content(ticket_record)
        
        if not ticket_content:
            failure_counter.increment()
            return None
        
        # Prepare update document - title, description, priority, urgency
        update_doc = {
            "title": ticket_content['title'],
            "description": ticket_content['description'],
            "priority": ticket_content['priority'],
            "urgency": ticket_content['urgency']
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

def generate_titles_for_existing_descriptions():
    """Generate titles for tickets that have descriptions but no titles"""
    
    logger.info("Starting Title Generation for Existing Descriptions...")
    
    # Get all ticket records that have descriptions but no titles
    logger.info("Fetching ticket records with descriptions but no titles...")
    try:
        # Query for tickets that have description but don't have title
        query = {
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "$or": [
                {"title": {"$exists": False}},
                {"title": {"$in": [None, ""]}}
            ]
        }
        
        ticket_records = list(ticket_col.find(query))
        total_tickets = len(ticket_records)
        
        if total_tickets == 0:
            logger.info("All tickets with descriptions already have titles!")
            return
        
        # Convert all ObjectId fields to strings to prevent JSON serialization issues
        for record in ticket_records:
            if '_id' in record and isinstance(record['_id'], ObjectId):
                record['_id'] = str(record['_id'])
        
        logger.info(f"Found {total_tickets} tickets needing title generation")
        progress_logger.info(f"TITLE_GENERATION_START: total_tickets={total_tickets}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching ticket records for title generation: {e}")
        return
    
    # Process in batches
    total_batches = (total_tickets + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []
    
    logger.info(f"Processing title generation in {total_batches} batches of {BATCH_SIZE} tickets each")
    
    try:
        for batch_num in range(1, total_batches + 1):
            if shutdown_flag.is_set():
                logger.info(f"Shutdown requested. Stopping title generation at batch {batch_num-1}/{total_batches}")
                break
                
            batch_start = (batch_num - 1) * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_tickets)
            batch_records = ticket_records[batch_start:batch_end]
            
            logger.info(f"Processing title generation batch {batch_num}/{total_batches} ({len(batch_records)} tickets)...")
            progress_logger.info(f"TITLE_BATCH_START: batch={batch_num}/{total_batches}, records={len(batch_records)}")
            
            # Process batch with parallelization
            successful_updates = []
            batch_start_time = time.time()
            
            # Use ThreadPoolExecutor for I/O bound operations (API calls)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks for this batch
                futures = {
                    executor.submit(process_single_title_generation, record): record 
                    for record in batch_records
                }
                
                # Collect results with progress tracking
                completed = 0
                try:
                    for future in as_completed(futures, timeout=REQUEST_TIMEOUT * 2):
                        if shutdown_flag.is_set():
                            logger.warning("Cancelling remaining title generation tasks...")
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
                                logger.info(f"Title generation batch progress: {progress:.1f}% ({completed}/{len(batch_records)})")
                                
                        except Exception as e:
                            logger.error(f"Error processing title generation future result: {e}")
                            completed += 1
                            
                except Exception as e:
                    logger.error(f"Error collecting title generation batch results: {e}")
            
            batch_end_time = time.time()
            batch_duration = batch_end_time - batch_start_time
            
            # Add successful updates to accumulator
            batch_updates.extend(successful_updates)
            
            logger.info(f"Title generation batch {batch_num} complete: {len(successful_updates)}/{len(batch_records)} successful")
            logger.info(f"Batch duration: {batch_duration:.2f}s")
            progress_logger.info(f"TITLE_BATCH_COMPLETE: batch={batch_num}, successful={len(successful_updates)}, duration={batch_duration:.2f}s")
            
            # Save to database every batch
            if len(batch_updates) >= BATCH_SIZE and not shutdown_flag.is_set():
                saved_count = save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear the accumulator
                
                logger.info(f"Title generation database update complete: {saved_count} records saved")
            
            # Progress summary every 5 batches
            if batch_num % 5 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_tickets) * 100
                logger.info(f"Title Generation Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Titles Generated: {title_counter.value} | Failures: {failure_counter.value} | DB Updates: {total_updated}")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"TITLE_PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, titles={title_counter.value}, failures={failure_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches
            if not shutdown_flag.is_set() and batch_num < total_batches:
                time.sleep(0.5)
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
            logger.info(f"Final title generation batch saved: {saved_count} records")
        
        logger.info("Title generation for existing descriptions complete!")
        logger.info(f"Total titles generated and saved: {total_updated}")
        
    except KeyboardInterrupt:
        logger.info("Title generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error in title generation: {e}")
        shutdown_flag.set()

def update_tickets_with_content_parallel():
    """Update existing tickets with generated description, priority, and urgency using optimized batch processing"""
    
    logger.info("Starting EU Banking Trouble Ticket Content Generation...")
    logger.info(f"System Info: {CPU_COUNT} CPU cores detected")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Request timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"Max retries per request: {MAX_RETRIES}")
    
    # Test Ollama connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        logger.error("Please check the Ollama endpoint configuration and network connectivity")
        return
    
    # Get all ticket records that need content generation
    logger.info("Fetching ticket records from database...")
    try:
        # Query for tickets that don't have description, priority, and urgency fields
        query = {
            "$or": [
                {"description": {"$exists": False}},
                {"priority": {"$exists": False}},
                {"urgency": {"$exists": False}},
                {"description": {"$in": [None, ""]}},
                {"priority": {"$in": [None, ""]}},
                {"urgency": {"$in": [None, ""]}}
            ]
        }
        
        ticket_records = list(ticket_col.find(query))
        total_tickets = len(ticket_records)
        
        if total_tickets == 0:
            logger.info("All tickets already have description, priority, and urgency!")
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
            
            # Save to database every 10 records (one batch)
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
                time.sleep(0.5)
        
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

def test_network_connectivity():
    """Test basic network connectivity to Ollama endpoint"""
    try:
        logger.info("Testing basic network connectivity to Cloudflare Tunnel endpoint...")
        
        # For Cloudflare Tunnel, we test HTTP connectivity instead of TCP
        import requests
        response = requests.get(OLLAMA_BASE_URL, timeout=10)
        
        if response.status_code in [200, 404, 405]:  # Any response means connectivity works
            logger.info("✅ Network connectivity test passed - Cloudflare Tunnel is reachable")
            return True
        else:
            logger.error(f"❌ Network connectivity test failed - unexpected status code: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Network connectivity test error: {e}")
        return False

def test_ollama_connection():
    """Test if Ollama is running and model is available"""
    try:
        logger.info(f"Testing connection to Ollama: {OLLAMA_BASE_URL}")
        
        # First, test basic connectivity with a simple hello message
        logger.info("Testing basic connectivity with hello message...")
        
        # Simple hello test with minimal payload
        hello_payload = {
            "model": OLLAMA_MODEL,
            "prompt": "Say 'hello' in one word",
            "stream": False,
            "options": {"num_predict": 5}
        }
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {OLLAMA_TOKEN}' if OLLAMA_TOKEN else None
        }
        # Remove None values
        headers = {k: v for k, v in headers.items() if v is not None}
        
        # Use shorter timeout for quick test
        test_response = requests.post(
            OLLAMA_URL, 
            json=hello_payload,
            headers=headers,
            timeout=15  # Reduced timeout for quick test
        )
        
        logger.info(f"Hello test status: {test_response.status_code}")
        
        if not test_response.text.strip():
            logger.error("Empty response from hello endpoint")
            return False
        
        test_response.raise_for_status()
        
        try:
            result = test_response.json()
            if "response" in result:
                logger.info("Ollama hello test successful")
                logger.info(f"Hello response: {result['response'][:50]}...")
                
                # Now test with the original generation test
                logger.info("Testing full generation capability...")
                
                test_payload = {
                    "model": OLLAMA_MODEL,
                    "prompt": "Generate a JSON object with 'test': 'success'",
                    "stream": False,
                    "options": {"num_predict": 20}
                }
                
                test_response = requests.post(
                    OLLAMA_URL, 
                    json=test_payload,
                    headers=headers,
                    timeout=30
                )
                
                logger.info(f"Generation test status: {test_response.status_code}")
                
                if not test_response.text.strip():
                    logger.error("Empty response from generation endpoint")
                    return False
                
                test_response.raise_for_status()
                
                result = test_response.json()
                if "response" in result:
                    logger.info("Ollama full generation test successful")
                    logger.info(f"Test response: {result['response'][:100]}...")
                    return True
                else:
                    logger.error(f"No 'response' field in test. Fields: {list(result.keys())}")
                    return False
                    
            else:
                logger.error(f"No 'response' field in hello test. Fields: {list(result.keys())}")
                return False
                
        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {e}")
            return False
            
    except requests.exceptions.Timeout:
        logger.error("Connection test timed out - Ollama endpoint may be unreachable")
        return False
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - Ollama endpoint is not reachable")
        return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Connection test failed: {e}")
        return False

def get_collection_stats():
    """Get collection statistics"""
    try:
        total_count = ticket_col.count_documents({})
        
        # Count records with titles
        with_titles = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Count records with complete content
        with_content = ticket_col.count_documents({
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Count records with all fields
        with_all_fields = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_titles = total_count - with_titles
        without_content = total_count - with_content
        without_all = total_count - with_all_fields
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(ticket_col.aggregate(pipeline))
        
        # Get priority distribution if available
        priority_pipeline = [
            {"$match": {"priority": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {"_id": "$priority", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        priority_distribution = list(ticket_col.aggregate(priority_pipeline))
        
        # Get urgency distribution if available
        urgency_pipeline = [
            {"$match": {"urgency": {"$exists": True, "$ne": "", "$ne": None}}},
            {"$group": {"_id": "$urgency", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]
        
        urgency_distribution = list(ticket_col.aggregate(urgency_pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"Total tickets: {total_count}")
        logger.info(f"With titles: {with_titles}")
        logger.info(f"With content (description, priority & urgency): {with_content}")
        logger.info(f"With all fields (title, description, priority & urgency): {with_all_fields}")
        logger.info(f"Without titles: {without_titles}")
        logger.info(f"Without content: {without_content}")
        logger.info(f"Without all fields: {without_all}")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"{i}. {topic['_id']}: {topic['count']} tickets")
        
        if priority_distribution:
            logger.info("Priority Distribution:")
            for priority in priority_distribution:
                logger.info(f"  {priority['_id']}: {priority['count']} tickets")
        
        if urgency_distribution:
            logger.info("Urgency Distribution:")
            for urgency in urgency_distribution:
                logger.info(f"  {urgency['_id']}: {urgency['count']} tickets")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_titles={with_titles}, with_content={with_content}, with_all_fields={with_all_fields}")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_tickets(limit=3):
    """Get sample tickets with generated content"""
    try:
        samples = list(ticket_col.find({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Ticket Content:")
        for i, ticket in enumerate(samples, 1):
            logger.info(f"--- Sample Ticket {i} ---")
            logger.info(f"Ticket ID: {ticket.get('ticket_id', 'N/A')}")
            logger.info(f"Title: {ticket.get('title', 'N/A')}")
            logger.info(f"Dominant Topic: {ticket.get('dominant_topic', 'N/A')}")
            logger.info(f"Subtopics: {str(ticket.get('subtopics', 'N/A'))[:100]}...")
            logger.info(f"Priority: {ticket.get('priority', 'N/A')}")
            logger.info(f"Urgency: {ticket.get('urgency', 'N/A')}")
            logger.info(f"Description Preview: {str(ticket.get('description', 'N/A'))[:200]}...")
            
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
        with_titles = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None}
        })
        with_content = ticket_col.count_documents({
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None}
        })
        with_all_fields = ticket_col.count_documents({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        # Generate report
        status_report = {
            "timestamp": datetime.now().isoformat(),
            "session_stats": {
                "successful_generations": success_counter.value,
                "failed_generations": failure_counter.value,
                "titles_generated": title_counter.value,
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
                "tickets_with_titles": with_titles,
                "tickets_with_content": with_content,
                "tickets_with_all_fields": with_all_fields,
                "tickets_without_titles": total_count - with_titles,
                "tickets_without_content": total_count - with_content,
                "tickets_without_all": total_count - with_all_fields,
                "title_completion_percentage": (with_titles / total_count) * 100 if total_count > 0 else 0,
                "content_completion_percentage": (with_content / total_count) * 100 if total_count > 0 else 0,
                "full_completion_percentage": (with_all_fields / total_count) * 100 if total_count > 0 else 0
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

def validate_generated_content():
    """Validate recently generated content quality"""
    try:
        # Get recent generated tickets
        recent_tickets = list(ticket_col.find({
            "title": {"$exists": True, "$ne": "", "$ne": None},
            "description": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "urgency": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(20))
        
        validation_results = {
            "total_validated": len(recent_tickets),
            "validation_issues": [],
            "quality_metrics": {
                "avg_title_length": 0,
                "avg_description_length": 0,
                "critical_tickets_count": 0,
                "high_urgency_tickets_count": 0,
                "tickets_with_placeholders": 0,
                "priority_distribution": {},
                "urgency_distribution": {}
            }
        }
        
        title_lengths = []
        description_lengths = []
        critical_count = 0
        high_urgency_count = 0
        placeholder_count = 0
        priority_counts = {}
        urgency_counts = {}
        
        valid_priorities = ['P1 - Critical', 'P2 - High', 'P3 - Medium', 'P4 - Low', 'P5 - Very Low']
        valid_urgencies = ['Critical', 'High', 'Medium', 'Low']
        
        for ticket in recent_tickets:
            title = ticket.get('title', '')
            description = ticket.get('description', '')
            priority = ticket.get('priority', '')
            urgency = ticket.get('urgency', '')
            
            # Validate title
            if len(title) < 50 or len(title) > 100:
                validation_results["validation_issues"].append({
                    "ticket_id": ticket.get('ticket_id'),
                    "issue": f"Title length {len(title)} outside optimal range (50-100)"
                })
            
            # Validate description
            if len(description) < 200 or len(description) > 800:
                validation_results["validation_issues"].append({
                    "ticket_id": ticket.get('ticket_id'),
                    "issue": f"Description length {len(description)} outside optimal range (200-800)"
                })
            
            # Validate priority
            if priority not in valid_priorities:
                validation_results["validation_issues"].append({
                    "ticket_id": ticket.get('ticket_id'),
                    "issue": f"Invalid priority: {priority}"
                })
            
            # Validate urgency
            if urgency not in valid_urgencies:
                validation_results["validation_issues"].append({
                    "ticket_id": ticket.get('ticket_id'),
                    "issue": f"Invalid urgency: {urgency}"
                })
            
            # Check for technical details presence
            required_elements = ['system', 'error', 'incident', 'impact']
            missing_elements = [elem for elem in required_elements if elem.lower() not in description.lower()]
            if missing_elements:
                validation_results["validation_issues"].append({
                    "ticket_id": ticket.get('ticket_id'),
                    "issue": f"Missing technical elements: {', '.join(missing_elements)}"
                })
            
            # Check for placeholders
            content_to_check = title + " " + description
            if '[' in content_to_check or 'placeholder' in content_to_check.lower():
                validation_results["validation_issues"].append({
                    "ticket_id": ticket.get('ticket_id'),
                    "issue": "Contains placeholder text"
                })
                placeholder_count += 1
            
            title_lengths.append(len(title))
            description_lengths.append(len(description))
            
            # Count priorities and urgencies
            priority_counts[priority] = priority_counts.get(priority, 0) + 1
            urgency_counts[urgency] = urgency_counts.get(urgency, 0) + 1
            
            if priority == 'P1 - Critical':
                critical_count += 1
            if urgency == 'Critical' or urgency == 'High':
                high_urgency_count += 1
        
        # Calculate metrics
        if title_lengths:
            validation_results["quality_metrics"]["avg_title_length"] = sum(title_lengths) / len(title_lengths)
        if description_lengths:
            validation_results["quality_metrics"]["avg_description_length"] = sum(description_lengths) / len(description_lengths)
        validation_results["quality_metrics"]["critical_tickets_count"] = critical_count
        validation_results["quality_metrics"]["high_urgency_tickets_count"] = high_urgency_count
        validation_results["quality_metrics"]["tickets_with_placeholders"] = placeholder_count
        validation_results["quality_metrics"]["priority_distribution"] = priority_counts
        validation_results["quality_metrics"]["urgency_distribution"] = urgency_counts
        
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
    """Main function to initialize and run the trouble ticket content generator"""
    logger.info("🎫 EU Banking Trouble Ticket Content Generator Starting...")
    logger.info(f"💾 Database: {DB_NAME}")
    logger.info(f"📂 Collection: {TICKET_COLLECTION}")
    logger.info(f"🤖 Model: {OLLAMA_MODEL}")
    logger.info(f"🔗 Ollama URL: {OLLAMA_BASE_URL}")
    logger.info(f"🔑 Using token authentication")
    logger.info(f"⚡ Max Workers: {MAX_WORKERS}")
    logger.info(f"📦 Batch Size: {BATCH_SIZE}")
    logger.info(f"📝 Log Directory: {LOG_DIR}")
    
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
        
        # STEP 1: Generate titles for tickets that have descriptions but no titles
        logger.info("=" * 80)
        logger.info("STEP 1: GENERATING TITLES FOR EXISTING DESCRIPTIONS")
        logger.info("=" * 80)
        generate_titles_for_existing_descriptions()
        
        # Show stats after title generation
        logger.info("Stats after title generation:")
        get_collection_stats()
        
        # STEP 2: Generate full content (title, description, priority, urgency) for remaining tickets
        logger.info("=" * 80)
        logger.info("STEP 2: GENERATING FULL CONTENT FOR REMAINING TICKETS")
        logger.info("=" * 80)
        update_tickets_with_content_parallel()
        
        # Show final statistics
        logger.info("=" * 80)
        logger.info("FINAL STATISTICS")
        logger.info("=" * 80)
        get_collection_stats()
        
        # Show sample generated content
        get_sample_generated_tickets()
        
        # Validate content quality
        validation_results = validate_generated_content()
        if validation_results and validation_results["validation_issues"]:
            logger.warning(f"Found {len(validation_results['validation_issues'])} content validation issues")
        
        # Generate final status report
        status_report = generate_status_report()
        if status_report:
            logger.info("=" * 80)
            logger.info("FINAL SESSION REPORT")
            logger.info("=" * 80)
            logger.info(f"Success Rate: {status_report['session_stats']['success_rate']:.2f}%")
            logger.info(f"Titles Generated: {status_report['session_stats']['titles_generated']}")
            logger.info(f"Full Content Generated: {status_report['session_stats']['successful_generations']}")
            logger.info(f"Total Database Updates: {status_report['session_stats']['database_updates']}")
            logger.info(f"Title Completion: {status_report['database_stats']['title_completion_percentage']:.2f}%")
            logger.info(f"Content Completion: {status_report['database_stats']['content_completion_percentage']:.2f}%")
            logger.info(f"Full Completion: {status_report['database_stats']['full_completion_percentage']:.2f}%")
        
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
        logger.info(f"📋 Main Log: {MAIN_LOG_FILE}")
        logger.info(f"✅ Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"❌ Failure Log: {FAILURE_LOG_FILE}")
        logger.info(f"📊 Progress Log: {PROGRESS_LOG_FILE}")
        logger.info(f"💾 Intermediate Results: {INTERMEDIATE_RESULTS_FILE}")

# Run the content generator
if __name__ == "__main__":
    main()
