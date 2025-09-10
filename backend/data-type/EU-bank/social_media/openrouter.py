# EU Bank Trustpilot Review Generator - Ollama Version with Platform Field Usage
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
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue
import atexit
import psutil
from pathlib import Path
from pymongo import UpdateOne
import random
import hashlib


# Load environment variables
load_dotenv()

# Fix console encoding for Windows
import io
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8')

# Custom stream handler that handles encoding issues
class SafeStreamHandler(logging.StreamHandler):
    def __init__(self, stream=None):
        super().__init__(stream)
    
    def emit(self, record):
        try:
            super().emit(record)
        except UnicodeEncodeError:
            # Fallback: replace problematic characters
            record.msg = str(record.msg).encode('ascii', 'replace').decode('ascii')
            super().emit(record)

# MongoDB setup - Updated collection name
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
SOCIAL_MEDIA_COLLECTION = "socialmedia"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"eu_bank_trustpilot_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_trustpilot_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_trustpilot_generations_{timestamp}.log"

# Configure main logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(MAIN_LOG_FILE, encoding='utf-8'),
        SafeStreamHandler(sys.stdout)
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

logger = logging.getLogger(__name__)

# Global variables for graceful shutdown
shutdown_flag = threading.Event()
client = None
db = None
social_media_col = None

# Ollama setup - using same configuration as Reddit generator
OLLAMA_BASE_URL = "https://teen-everybody-dpi-tyler.trycloudflare.com"
OLLAMA_TOKEN = "b3545313daf559ad21f54cef56ea339ae5093e4d9d5b1a0896d31e859e8ffd1a"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_TAGS_URL = f"{OLLAMA_BASE_URL}/api/tags"
OLLAMA_MODEL = "gemma3:27b"

# Optimized batch processing configuration
BATCH_SIZE = 2  # Very conservative for consistency
CPU_COUNT = multiprocessing.cpu_count()
MAX_WORKERS = min(CPU_COUNT, 4)  # Very conservative
REQUEST_TIMEOUT = 180
MAX_RETRIES = 3
RETRY_DELAY = 2

fake = Faker()

# Thread-safe counters
class LoggingCounter:
    def __init__(self, name):
        self._value = 0
        self._lock = threading.Lock()
        self._name = name
    
    def increment(self):
        with self._lock:
            self._value += 1
            return self._value
    
    @property
    def value(self):
        with self._lock:
            return self._value

success_counter = LoggingCounter("SUCCESS_COUNT")
failure_counter = LoggingCounter("FAILURE_COUNT")
update_counter = LoggingCounter("UPDATE_COUNT")

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
    global client, db, social_media_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        db = client[DB_NAME]
        social_media_col = db[SOCIAL_MEDIA_COLLECTION]
        
        # Create indexes for better performance
        social_media_col.create_index("channel")
        social_media_col.create_index("dominant_topic")
        social_media_col.create_index("rating")
        social_media_col.create_index("platform")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def generate_random_date():
    """Generate a random date between 30 days ago and today"""
    start_date = datetime.now() - timedelta(days=30)
    end_date = datetime.now()
    
    time_between = end_date - start_date
    days_between = time_between.days
    random_days = random.randrange(days_between)
    
    random_date = start_date + timedelta(days=random_days)
    return random_date.strftime("%d-%m-%Y")

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=300,  # Increased max time
    base=RETRY_DELAY,
    on_backoff=lambda details: logger.warning(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_ollama_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call Ollama API with exponential backoff and better error handling"""
    if shutdown_flag.is_set():
        raise KeyboardInterrupt("Shutdown requested")
    
    # Add delay for rate limiting
    time.sleep(1)
    
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
            "temperature": 0.7,
            "num_predict": 1500,
            "top_k": 40,
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
        logger.warning(f"Request timed out after {timeout} seconds. This might be due to model processing time.")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - check Ollama endpoint")
        raise
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            logger.warning(f"Rate limit exceeded (429). Waiting before retry...")
            time.sleep(10)  # Wait 10 seconds for rate limit
        elif e.response.status_code == 408:
            logger.warning(f"Request timeout (408). This might be due to model processing time.")
            time.sleep(5)  # Wait 5 seconds before retry
        logger.error(f"HTTP error: {e.response.status_code} - {e.response.text[:200]}")
        raise
    except Exception as e:
        logger.error(f"Ollama API error: {e}")
        raise

def generate_trustpilot_review_content(social_media_data):
    """Generate Trustpilot review content based on dominant topic and subtopics for EU Bank"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    record_id = social_media_data.get('_id', 'unknown')
    
    try:
        # Extract data from social media record
        dominant_topic = social_media_data.get('dominant_topic', 'Banking Experience')
        subtopics = social_media_data.get('subtopics', 'General banking services')
        username = social_media_data.get('username', 'Anonymous User')
        channel = social_media_data.get('channel', 'Trustpilot')
        
        # Handle subtopics whether it's a list or string
        if isinstance(subtopics, str):
            subtopics_list = [subtopic.strip() for subtopic in subtopics.split(',') if subtopic.strip()]
        elif isinstance(subtopics, list):
            subtopics_list = subtopics
        else:
            subtopics_list = ['General banking services']
        
        # Prepare subtopics as formatted string
        subtopics_str = ', '.join(subtopics_list)
        
        logger.info(f"Record {record_id}: Generating Trustpilot review content")
        
        # Generate unique seeds for each call
        topic_hash = abs(hash(dominant_topic + subtopics_str)) % 1000
        length_hash = abs(hash(str(len(dominant_topic + subtopics_str)))) % 1000
        random_seed = abs(hash(f"{dominant_topic}{subtopics_str}{topic_hash}")) % 1000

        trustpilot_prompt = f"""
You are a European bank customer writing a review for your bank on Trustpilot. Analyze the DOMINANT TOPIC and SUBTOPICS to understand the exact banking experience, then generate an authentic Trustpilot review.

**BANKING EXPERIENCE TO ANALYZE:**
Dominant Topic: {dominant_topic}
Subtopics: {subtopics_str}
Channel: {channel}

**ANALYSIS INSTRUCTIONS:**
1. **Determine Experience Type from Dominant Topic:**
   - Words like "Excellent", "Outstanding", "Great", "Fast", "Smooth", "Easy", "Perfect", "Amazing" → POSITIVE experience (4-5 stars)
   - Words like "Poor", "Slow", "Issues", "Problems", "Terrible", "Awful", "Bad", "Disappointing" → NEGATIVE experience (1-2 stars)
   - Words like "Average", "Decent", "Okay", "Mixed", "Improvements Needed" → NEUTRAL experience (3 stars)

2. **Understand Specific Context from Subtopics:**
   - Identify the exact banking service area (customer service, online banking, loans, cards, etc.)
   - Determine service quality vs. technical vs. staff interaction issues
   - Understand scope and impact of the banking experience

**FORCED VARIATION PARAMETERS:**
- Customer Type: {['tech_savvy_professional', 'elderly_customer', 'busy_parent', 'small_business_owner', 'student', 'frequent_traveler', 'security_conscious', 'first_time_customer'][topic_hash % 8]}
- Review Style: {['detailed_experience', 'brief_emotional', 'comparison_based', 'service_focused', 'problem_solver', 'frustrated_urgent', 'appreciative_loyal', 'constructive_feedback'][length_hash % 8]}
- Usage Context: {['daily_banking', 'business_banking', 'mortgage_experience', 'customer_service', 'first_experience', 'long_term_customer', 'specific_product', 'branch_visit'][random_seed % 8]}

**REVIEW GENERATION RULES:**

**For POSITIVE Experiences (4-5 stars):**
- Highlight specific banking services that exceeded expectations
- Mention helpful staff members or efficient processes
- Reference reliability, security, and customer support quality
- Express satisfaction with banking relationship
- Compare favorably to other banks (if relevant)
- Mention specific situations where the bank helped

**For NEGATIVE Experiences (1-2 stars):**
- Detail specific problems and frustrations with banking services
- Explain impact on financial needs or business operations
- Mention poor customer service experiences or system failures
- Express disappointment and demand for improvements
- Compare unfavorably to competitor banks (if relevant)
- Request immediate resolution or consideration of switching banks

**For NEUTRAL Experiences (3 stars):**
- Balanced view of banking service pros and cons
- Acknowledge both satisfactory and problematic aspects
- Suggest specific improvements for better service
- Moderate tone without extreme emotions
- Constructive feedback for bank management

**DIVERSITY REQUIREMENTS:**
- NEVER repeat time references: vary between "last month", "recently", "for years", "this week", "since opening account", etc.
- Rotate service mentions: "customer service", "online banking", "branch staff", "mobile app", "loan process", etc.
- Avoid repetitive phrases: "great service", "terrible experience", "needs improvement"
- Use different problem/praise descriptions for similar experiences
- Vary emotional intensity based on customer type and rating

**CUSTOMER-APPROPRIATE LANGUAGE:**

**Tech Savvy Professional:** Banking terminology, digital features, efficiency metrics, security protocols
**Elderly Customer:** Simple language, traditional banking focus, branch service emphasis, personal relationship importance
**Busy Parent:** Time efficiency, convenience, family banking needs, accessible service
**Small Business Owner:** Business banking focus, commercial services, relationship management, operational reliability
**Student:** Budget-friendly services, simple processes, digital features, cost considerations
**Frequent Traveler:** International banking, card usage abroad, travel services, accessibility
**Security Conscious:** Security measures, fraud protection, privacy policies, trust factors
**First Time Customer:** Onboarding experience, learning process, initial impressions, guidance quality

**RATING DISTRIBUTION LOGIC:**
- POSITIVE topics: 70% chance 5-star, 30% chance 4-star
- NEGATIVE topics: 60% chance 1-star, 40% chance 2-star  
- NEUTRAL topics: 100% chance 3-star

**TRUSTPILOT REVIEW REQUIREMENTS:**
- Always include a compelling Title that summarizes the experience
- Write genuine, human-like review text that feels authentic
- Include specific details about the banking experience
- Use natural language with appropriate emotional tone
- Reference specific timeframes and situations
- Generate random "Date of experience" within last 30 days

**REVIEW LENGTH GUIDELINES:**
- 1-2 star reviews: Longer, more detailed complaints (150-400 words)
- 3-star reviews: Moderate length, balanced feedback (100-250 words)
- 4-5 star reviews: Vary between brief praise (75-150 words) and detailed appreciation (150-300 words)

**OUTPUT FORMAT:**
```json
{{
  "rating": {1 if any(word in dominant_topic.lower() for word in ["terrible", "awful", "horrible", "worst"]) else 2 if any(word in dominant_topic.lower() for word in ["poor", "bad", "disappointing", "issues", "problems"]) else 4 if any(word in dominant_topic.lower() for word in ["good", "great", "nice", "helpful"]) else 5 if any(word in dominant_topic.lower() for word in ["excellent", "outstanding", "perfect", "amazing", "fantastic"]) else 3},
  "priority": "{"P1 - High" if any(word in dominant_topic.lower() for word in ["critical", "urgent", "terrible", "security", "fraud"]) else "P2 - Medium" if any(word in dominant_topic.lower() for word in ["slow", "issues", "problems", "disappointing"]) else "P3 - Low"}",
  "useful_count": {random_seed % 25},
  "sentiment": "{"Positive" if any(word in dominant_topic.lower() for word in ["excellent", "great", "outstanding", "perfect", "amazing", "fantastic", "good", "nice"]) else "Negative" if any(word in dominant_topic.lower() for word in ["poor", "bad", "terrible", "awful", "disappointing", "issues", "problems", "worst", "horrible"]) else "Neutral"}",
  "Title": "Generate compelling Trustpilot review title that captures the main banking experience (4-10 words)",
  "text": "Generate authentic Trustpilot review that clearly reflects the dominant topic banking experience using assigned customer type and style - appropriate length for rating with specific banking details",
  "urgency": {str(any(word in dominant_topic.lower() for word in ["critical", "urgent", "terrible", "fraud", "security", "emergency"])).lower()},
  "Date of experience": "Generate random date in DD-MM-YYYY format within last 30 days"
}}
```

**CRITICAL SUCCESS CRITERIA:**
1. Review content must obviously relate to the dominant topic banking experience
2. Reader should understand the specific banking service experience from the review
3. Sentiment and rating must match the dominant topic (positive/negative/neutral)
4. Subtopics must be referenced or implied in the review content
5. Customer type voice must be authentic and consistent throughout
6. Review must be completely unique in structure and phrasing
7. Banking context must be realistic and specific to European banking services
8. Review length must be appropriate for the rating given
9. Title must be compelling and accurately reflect the experience
10. Date of experience must be a realistic recent date
11. Include specific banking service details (branches, staff, processes, etc.)
12. Use natural, conversational language that feels genuine

**BANKING SERVICES TO REFERENCE (when relevant):**
- Online/Mobile Banking
- Customer Service (phone, email, chat)
- Branch Services
- ATM Network
- Card Services (debit/credit)
- Loans (personal, mortgage, business)
- Investment Services
- Business Banking
- International Banking
- Security Features
- Account Management

**GENERATION INSTRUCTION:**
Read the dominant topic "{dominant_topic}" and subtopics "{subtopics_str}". Understand what specific banking experience this represents. Generate an authentic Trustpilot review from the assigned customer type that clearly communicates this specific banking experience with appropriate rating, compelling title, and realistic details.

Topic Analysis: {dominant_topic} - {subtopics_str}
Channel Context: {channel}
Bank Type: European Bank
Review Platform: Trustpilot
""".strip()

        response = call_ollama_with_backoff(trustpilot_prompt)
        
        # Add delay between API calls to prevent rate limiting
        time.sleep(1)
        
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
            
            generation_time = time.time() - start_time
            
            # Validate Trustpilot specific fields
            if 'rating' not in result:
                result['rating'] = 3
            if 'urgency' not in result:
                result['urgency'] = False
            if 'useful_count' not in result:
                result['useful_count'] = random.randint(0, 25)
            if 'priority' not in result:
                result['priority'] = 'P2 - Medium'
            if 'sentiment' not in result:
                result['sentiment'] = 'Neutral'
            if 'Title' not in result or not result['Title']:
                result['Title'] = 'Banking experience review'
            if 'Date of experience' not in result:
                result['Date of experience'] = generate_random_date()
            
            # Ensure rating is within valid range
            result['rating'] = max(1, min(5, result['rating']))
            
            # Ensure useful_count is within valid range
            result['useful_count'] = max(0, min(100, result['useful_count']))
            
            # Ensure urgency is boolean
            if isinstance(result['urgency'], str):
                result['urgency'] = result['urgency'].lower() == 'true'
            
            # Log successful generation
            success_info = {
                'record_id': str(record_id),
                'username': username,
                'channel': channel,
                'dominant_topic': dominant_topic,
                'subtopics': subtopics_str,
                'rating': result.get('rating', 3),
                'priority': result.get('priority', 'P2 - Medium'),
                'sentiment': result.get('sentiment', 'Neutral'),
                'urgency': result.get('urgency', False),
                'useful_count': result.get('useful_count', 0),
                'title': result.get('Title', ''),
                'date_of_experience': result.get('Date of experience', ''),
                'generation_time': generation_time,
                'text_length': len(result.get('text', ''))
            }
            success_logger.info(json.dumps(success_info))
            
            return result
            
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'record_id': str(record_id),
            'dominant_topic': social_media_data.get('dominant_topic', 'Unknown'),
            'channel': social_media_data.get('channel', 'Unknown'),
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_social_media_record(social_media_record):
    """Process a single social media record to generate Trustpilot review content"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate Trustpilot review content based on existing data
        review_content = generate_trustpilot_review_content(social_media_record)
        
        if not review_content:
            failure_counter.increment()
            return None
        
        # Prepare update document with all LLM-generated Trustpilot fields
        update_doc = {
            "rating": review_content.get('rating', 3),
            "priority": review_content.get('priority', 'P2 - Medium'),
            "useful_count": review_content.get('useful_count', random.randint(0, 25)),
            "sentiment": review_content.get('sentiment', 'Neutral'),
            "Title": review_content.get('Title', 'Banking experience review'),
            "text": review_content.get('text', ''),
            "urgency": review_content.get('urgency', False),
            "Date of experience": review_content.get('Date of experience', generate_random_date()),
            "domain": "banking",
            "content_generated_at": datetime.now().isoformat()
        }
        
        success_counter.increment()
        
        return {
            'record_id': social_media_record['_id'],
            'update_doc': update_doc
        }
        
    except Exception as e:
        logger.error(f"Task processing error for {social_media_record.get('_id', 'unknown')}: {str(e)}")
        failure_counter.increment()
        return None

def save_batch_to_database(batch_updates):
    """Save a batch of updates to the database"""
    if not batch_updates or shutdown_flag.is_set():
        return 0
    
    try:
        logger.info(f"Saving batch of {len(batch_updates)} updates to database...")
        
        bulk_operations = []
        for update_data in batch_updates:
            operation = UpdateOne(
                filter={"_id": update_data['record_id']},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
        
        if bulk_operations:
            try:
                result = social_media_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                logger.info(f"Successfully saved {updated_count} records to database")
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                return 0
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def get_collection_stats():
    """Get collection statistics for social media records with Trustpilot channel"""
    try:
        # Count all social media records
        total_count = social_media_col.count_documents({})
        
        # Count Trustpilot records specifically
        trustpilot_query = {"channel": "Trustpilot"}
        trustpilot_count = social_media_col.count_documents(trustpilot_query)
        
        # Count by different channel variations
        trustpilot_variations = social_media_col.count_documents({
            "channel": {"$regex": "trustpilot", "$options": "i"}
        })
        
        # Count Trustpilot records with and without generated content
        with_content = social_media_col.count_documents({
            "channel": "Trustpilot",
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "rating": {"$exists": True, "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "Title": {"$exists": True, "$ne": "", "$ne": None},
            "useful_count": {"$exists": True},
            "Date of experience": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_content = trustpilot_count - with_content
        
        logger.info("EU Bank Social Media Collection Statistics:")
        logger.info(f"Total social media records: {total_count}")
        logger.info(f"Trustpilot records (exact match): {trustpilot_count}")
        logger.info(f"Trustpilot records (all variations): {trustpilot_variations}")
        logger.info(f"With generated content: {with_content}")
        logger.info(f"Without generated content: {without_content}")
        
        # Show sample channel values
        sample_channels = social_media_col.distinct("channel")
        logger.info(f"Available channels: {sample_channels}")
        
        # Show sample record structure for Trustpilot
        sample = social_media_col.find_one({"channel": "Trustpilot"})
        if sample:
            logger.info("Sample Trustpilot record structure:")
            for key, value in sample.items():
                if key == '_id':
                    logger.info(f"  {key}: {value}")
                elif isinstance(value, str) and len(value) > 50:
                    logger.info(f"  {key}: {value[:50]}...")
                else:
                    logger.info(f"  {key}: {value}")
        else:
            logger.info("No Trustpilot records found in collection")
        
    except Exception as e:
        logger.error(f"Error getting social media collection stats: {e}")

def test_ollama_connection():
    """Test if remote Ollama is running and model is available"""
    try:
        logger.info(f"Testing connection to remote Ollama: {OLLAMA_BASE_URL}")
        logger.info(f"Using model: {OLLAMA_MODEL}")
        
        # Prepare headers for remote endpoint
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {OLLAMA_TOKEN}' if OLLAMA_TOKEN else None
        }
        headers = {k: v for k, v in headers.items() if v is not None}
        
        # Test basic connection with simple generation
        test_prompt = "Generate a JSON object with 'test': 'success' and nothing else."
        
        test_payload = {
            "model": OLLAMA_MODEL,
            "prompt": test_prompt,
            "stream": False,
            "options": {"num_predict": 50, "temperature": 0.1}
        }
        
        response = requests.post(
            OLLAMA_URL,
            json=test_payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            if "response" in result:
                logger.info("Ollama connection test successful")
                return True
            else:
                logger.error("Invalid response structure from Ollama")
                return False
        else:
            logger.error(f"Ollama test failed with status: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"Ollama connection test failed: {e}")
        return False

def update_social_media_trustpilot_reviews():
    """Update social media records with Trustpilot channel with generated review content"""
    
    logger.info("Starting EU Bank Social Media Trustpilot Review Content Generation...")
    logger.info(f"Target Channel: Trustpilot")
    logger.info(f"Collection: {SOCIAL_MEDIA_COLLECTION}")
    logger.info(f"Bank: EU Bank (European)")
    logger.info(f"Review Platform: Trustpilot")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    
    # Test Ollama connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        return
    
    # Get all social media records with Trustpilot channel that need content generation
    logger.info("Fetching Trustpilot social media records from database...")
    try:
        # Search for Trustpilot records without generated content
        query = {
            "channel": "Trustpilot",
            "$or": [
                {"text": {"$exists": False}},
                {"rating": {"$exists": False}},
                {"priority": {"$exists": False}},
                {"sentiment": {"$exists": False}},
                {"useful_count": {"$exists": False}},
                {"Title": {"$exists": False}},
                {"Date of experience": {"$exists": False}},
                {"text": {"$in": [None, ""]}},
                {"rating": {"$in": [None]}},
                {"priority": {"$in": [None, ""]}},
                {"sentiment": {"$in": [None, ""]}},
                {"useful_count": {"$in": [None]}},
                {"Title": {"$in": [None, ""]}},
                {"Date of experience": {"$in": [None, ""]}}
            ]
        }
        
        social_media_records = list(social_media_col.find(query))
        total_records = len(social_media_records)
        
        if total_records == 0:
            logger.info("No Trustpilot social media records found needing content generation!")
            logger.info("Checking what Trustpilot records exist...")
            
            # Show what Trustpilot records exist
            all_trustpilot_records = list(social_media_col.find({"channel": "Trustpilot"}))
            if all_trustpilot_records:
                logger.info(f"Found {len(all_trustpilot_records)} Trustpilot records with existing content")
                sample = all_trustpilot_records[0]
                logger.info("Sample existing Trustpilot record:")
                for key, value in sample.items():
                    logger.info(f"  {key}: {value}")
            else:
                logger.info("No Trustpilot records found at all!")
                
                # Check for similar channel names
                all_channels = social_media_col.distinct("channel")
                logger.info(f"Available channels in collection: {all_channels}")
                
                # Check for trustpilot related channels with case insensitive search
                trustpilot_related = social_media_col.count_documents({
                    "channel": {"$regex": "trustpilot", "$options": "i"}
                })
                logger.info(f"Records with trustpilot related channels: {trustpilot_related}")
            return
            
        logger.info(f"Found {total_records} Trustpilot social media records needing content generation")
        
    except Exception as e:
        logger.error(f"Error fetching Trustpilot social media records: {e}")
        return
    
    # Process records in batches
    total_updated = 0
    batch_updates = []
    
    try:
        for i in range(0, total_records, BATCH_SIZE):
            if shutdown_flag.is_set():
                logger.info("Shutdown requested. Stopping processing...")
                break
                
            batch_records = social_media_records[i:i + BATCH_SIZE]
            batch_num = (i // BATCH_SIZE) + 1
            total_batches = (total_records + BATCH_SIZE - 1) // BATCH_SIZE
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_records)} records)...")
            
            # Process batch with reduced parallelization
            successful_updates = []
            
            # Use ThreadPoolExecutor with limited workers
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_single_social_media_record, record): record 
                    for record in batch_records
                }
                
                for future in as_completed(futures):
                        if shutdown_flag.is_set():
                            break
                            
                        try:
                            result = future.result(timeout=60)
                            if result:
                                successful_updates.append(result)
                        except Exception as e:
                            logger.error(f"Error processing future result: {e}")
            
            # Add successful updates to accumulator
            batch_updates.extend(successful_updates)
            
            logger.info(f"Batch {batch_num} complete: {len(successful_updates)}/{len(batch_records)} successful")
            
            # Save to database every batch
            if batch_updates:
                saved_count = save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []
                logger.info(f"Database updated: {saved_count} records saved")
            
            # Progress summary
            logger.info(f"Progress: {min(i + BATCH_SIZE, total_records)}/{total_records} records processed")
            logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value}")
        
        logger.info("EU Bank Social Media Trustpilot review generation complete!")
        logger.info(f"Total records updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def main():
    """Main function to initialize and run the EU Bank Social Media Trustpilot review generator"""
    logger.info("EU Bank Social Media Trustpilot Review Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {SOCIAL_MEDIA_COLLECTION}")
    logger.info(f"Target Channel: Trustpilot")
    logger.info(f"Bank: EU Bank (European Banking)")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_BASE_URL}")
    logger.info(f"Review Platform: Trustpilot")
    
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
        
        # Run the social media Trustpilot review generation
        update_social_media_trustpilot_reviews()
        
        # Show final statistics
        get_collection_stats()
        
    except KeyboardInterrupt:
        logger.info("EU Bank Social Media Trustpilot review generation interrupted!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        cleanup_resources()
        
        logger.info("Session complete. Check log files:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")

# Run the EU Bank Social Media Trustpilot review generator
if __name__ == "__main__":
    main()