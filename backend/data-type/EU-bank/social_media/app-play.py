# EU Bank Social Media App Store Review Generator - Ollama Version with Platform Field Usage
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
MAIN_LOG_FILE = LOG_DIR / f"eu_bank_socialmedia_appstore_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_socialmedia_appstore_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_socialmedia_appstore_generations_{timestamp}.log"

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

def get_platform_from_record(social_media_data):
    """Get platform directly from the database record with validation"""
    platform = social_media_data.get('platform', '').strip()
    channel = social_media_data.get('channel', '').strip()
    record_id = social_media_data.get('_id', 'unknown')
    
    logger.info(f"Record {record_id}: Platform field contains '{platform}', Channel: '{channel}'")
    
    # Validate platform field
    if platform in ['Google Play Store', 'App Store']:
        logger.info(f"Record {record_id}: Using platform '{platform}' from database field")
        return platform
    elif platform.lower() in ['google play store', 'app store']:
        # Handle case variations
        normalized_platform = 'Google Play Store' if 'google' in platform.lower() else 'App Store'
        logger.info(f"Record {record_id}: Normalized platform from '{platform}' to '{normalized_platform}'")
        return normalized_platform
    else:
        # Default fallback when platform field is empty or invalid
        logger.warning(f"Record {record_id}: Invalid or missing platform field '{platform}', defaulting to 'Google Play Store'")
        return 'Google Play Store'

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

def generate_app_store_review_content(social_media_data):
    """Generate App Store review content based on dominant topic and subtopics for EU Bank"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    record_id = social_media_data.get('_id', 'unknown')
    
    try:
        # Extract data from social media record
        dominant_topic = social_media_data.get('dominant_topic', 'App Performance')
        subtopics = social_media_data.get('subtopics', 'General functionality')
        username = social_media_data.get('username', 'Anonymous User')
        channel = social_media_data.get('channel', 'App Store/Google Play')
        
        # Get platform directly from database record
        target_platform = get_platform_from_record(social_media_data)
        
        # Handle subtopics whether it's a list or string
        if isinstance(subtopics, str):
            subtopics_list = [subtopic.strip() for subtopic in subtopics.split(',') if subtopic.strip()]
        elif isinstance(subtopics, list):
            subtopics_list = subtopics
        else:
            subtopics_list = ['General functionality']
        
        # Prepare subtopics as formatted string
        subtopics_str = ', '.join(subtopics_list)
        
        logger.info(f"Record {record_id}: Generating content for platform '{target_platform}' from database field")
        
        # Generate unique seeds for each call
        topic_hash = abs(hash(dominant_topic + subtopics_str)) % 1000
        length_hash = abs(hash(str(len(dominant_topic + subtopics_str)))) % 1000
        random_seed = abs(hash(f"{dominant_topic}{subtopics_str}{topic_hash}")) % 1000

        app_store_prompt = f"""
You are a European bank customer writing a review for your bank's mobile banking app on {target_platform}. Analyze the DOMINANT TOPIC and SUBTOPICS to understand the exact app experience, then generate an authentic app store review.

**APP EXPERIENCE TO ANALYZE:**
Dominant Topic: {dominant_topic}
Subtopics: {subtopics_str}
Platform: {target_platform}
Channel: {channel}

**ANALYSIS INSTRUCTIONS:**
1. **Determine Experience Type from Dominant Topic:**
   - Words like "Excellent", "Outstanding", "Great", "Fast", "Smooth", "Easy", "Perfect" → POSITIVE experience (4-5 stars)
   - Words like "Poor", "Slow", "Crashes", "Bugs", "Problems", "Issues", "Terrible", "Awful" → NEGATIVE experience (1-2 stars)
   - Words like "Average", "Decent", "Okay", "Improvements Needed", "Mixed" → NEUTRAL experience (3 stars)

2. **Understand Specific Context from Subtopics:**
   - Identify the exact app feature area (login, transfers, notifications, UI/UX, etc.)
   - Determine technical vs. usability vs. security issues
   - Understand scope and impact of the experience

**FORCED VARIATION PARAMETERS:**
- Customer Type: {['tech_savvy_professional', 'elderly_user', 'busy_parent', 'small_business_owner', 'student', 'frequent_traveler', 'security_conscious', 'first_time_user'][topic_hash % 8]}
- Review Style: {['detailed_technical', 'brief_emotional', 'comparison_based', 'feature_focused', 'problem_solver', 'frustrated_urgent', 'appreciative_loyal', 'constructive_feedback'][length_hash % 8]}
- Usage Context: {['daily_banking', 'business_transactions', 'travel_usage', 'emergency_access', 'first_experience', 'long_term_user', 'feature_testing', 'security_focused'][random_seed % 8]}

**REVIEW GENERATION RULES:**

**For POSITIVE Experiences (4-5 stars):**
- Highlight specific features that work well
- Mention convenience and time-saving benefits
- Reference reliable performance and security
- Express satisfaction with user experience
- Compare favorably to other banking apps (if relevant)
- Mention specific use cases where app excels

**For NEGATIVE Experiences (1-2 stars):**
- Detail specific problems and frustrations
- Explain impact on daily banking activities
- Mention failed transactions or security concerns
- Express disappointment and urgency for fixes
- Compare unfavorably to competitors (if relevant)
- Demand immediate improvements

**For NEUTRAL Experiences (3 stars):**
- Balanced view of pros and cons
- Acknowledge both working and problematic features
- Suggest specific improvements
- Moderate tone without extreme emotions
- Constructive feedback for developers

**DIVERSITY REQUIREMENTS:**
- NEVER repeat time references: vary between "last week", "this morning", "for months", "recently", "since the update", etc.
- Rotate app versions: "latest update", "version 3.2", "after the iOS update", "new interface", etc.
- Avoid repetitive phrases: "works great", "major issues", "needs improvement"
- Use different problem descriptions for same issues
- Vary emotional intensity based on customer type and rating

**CUSTOMER-APPROPRIATE LANGUAGE:**

**Tech Savvy Professional:** Technical terminology, API mentions, security protocols, efficiency metrics
**Elderly User:** Simple language, basic feature focus, comparison to branch banking, reliability emphasis
**Busy Parent:** Time efficiency, quick access, family account management, convenience features
**Small Business Owner:** Business transaction focus, bulk operations, reporting features, reliability for operations
**Student:** Budget tracking, simple interface, mobile-first features, cost-saving benefits
**Frequent Traveler:** International usage, offline features, currency conversion, travel notifications
**Security Conscious:** Security features, encryption, fraud protection, privacy controls
**First Time User:** Learning curve, onboarding experience, help features, initial impressions

**RATING DISTRIBUTION LOGIC:**
- POSITIVE topics: 70% chance 5-star, 30% chance 4-star
- NEGATIVE topics: 60% chance 1-star, 40% chance 2-star  
- NEUTRAL topics: 100% chance 3-star

**PLATFORM-SPECIFIC ELEMENTS:**
**Google Play Store:**
- Mention Android-specific features (widgets, notifications, etc.)
- Reference Play Store update process
- Compare to other Android banking apps
- Do NOT generate a review "title" for Google Play Store

**App Store (iOS):**
- Mention iOS-specific features (Face ID, Touch ID, Siri integration)
- Reference App Store update process
- Compare to other iOS banking apps
- Generate review "Title" for App Store (iOS requires titles)

**REVIEW LENGTH GUIDELINES:**
- 1-2 star reviews: Longer, more detailed complaints (150-400 words)
- 3-star reviews: Moderate length, balanced feedback (100-250 words)
- 4-5 star reviews: Vary between brief praise (50-150 words) and detailed appreciation (150-300 words)

**OUTPUT FORMAT FOR GOOGLE PLAY STORE:**
```json
{{
  "rating": {1 if any(word in dominant_topic.lower() for word in ["terrible", "awful", "crashes", "unusable"]) else 2 if any(word in dominant_topic.lower() for word in ["poor", "slow", "problems", "issues", "bugs"]) else 4 if any(word in dominant_topic.lower() for word in ["good", "fast", "smooth", "easy"]) else 5 if any(word in dominant_topic.lower() for word in ["excellent", "outstanding", "perfect", "amazing"]) else 3},
  "priority": "{"P1 - Critical" if any(word in dominant_topic.lower() for word in ["crashes", "security", "login", "critical"]) else "P2 - Medium" if any(word in dominant_topic.lower() for word in ["slow", "bugs", "issues"]) else "P3 - Low"}",
  "review_helpful": {random_seed % 15},
  "sentiment": "{"Positive" if any(word in dominant_topic.lower() for word in ["excellent", "great", "outstanding", "perfect", "smooth", "fast", "easy"]) else "Negative" if any(word in dominant_topic.lower() for word in ["poor", "slow", "crashes", "bugs", "problems", "issues", "terrible"]) else "Neutral"}",
  "text": "Generate authentic app store review that clearly reflects the dominant topic experience using assigned customer type and style - appropriate length for rating",
  "urgency": {str(any(word in dominant_topic.lower() for word in ["critical", "urgent", "crashes", "security", "login"])).lower()},
  "platform": "Google Play Store"
}}
```

**OUTPUT FORMAT FOR APP STORE (iOS):**
```json
{{
  "rating": {1 if any(word in dominant_topic.lower() for word in ["terrible", "awful", "crashes", "unusable"]) else 2 if any(word in dominant_topic.lower() for word in ["poor", "slow", "problems", "issues", "bugs"]) else 4 if any(word in dominant_topic.lower() for word in ["good", "fast", "smooth", "easy"]) else 5 if any(word in dominant_topic.lower() for word in ["excellent", "outstanding", "perfect", "amazing"]) else 3},
  "priority": "{"P1 - Critical" if any(word in dominant_topic.lower() for word in ["crashes", "security", "login", "critical"]) else "P2 - Medium" if any(word in dominant_topic.lower() for word in ["slow", "bugs", "issues"]) else "P3 - Low"}",
  "review_helpful": {random_seed % 15},
  "sentiment": "{"Positive" if any(word in dominant_topic.lower() for word in ["excellent", "great", "outstanding", "perfect", "smooth", "fast", "easy"]) else "Negative" if any(word in dominant_topic.lower() for word in ["poor", "slow", "crashes", "bugs", "problems", "issues", "terrible"]) else "Neutral"}",
  "Title": "Generate concise review title that summarizes the main experience (3-8 words)",
  "text": "Generate authentic app store review that clearly reflects the dominant topic experience using assigned customer type and style - appropriate length for rating",
  "urgency": {str(any(word in dominant_topic.lower() for word in ["critical", "urgent", "crashes", "security", "login"])).lower()},
  "platform": "App Store"
}}
```

**CRITICAL SUCCESS CRITERIA:**
1. Review content must obviously relate to the dominant topic
2. Reader should understand the specific app experience from the review
3. Sentiment and rating must match the dominant topic (positive/negative/neutral)
4. Subtopics must be referenced or implied in the review content
5. Customer type voice must be authentic and consistent
6. Review must be completely unique in structure and phrasing
7. Banking app context must be realistic and specific to European banking
8. Review length must be appropriate for the rating given
9. Platform-specific elements should be naturally integrated
10. For App Store (iOS), include a Title field
11. For Google Play Store, do NOT include a Title field

**GENERATION INSTRUCTION:**
Read the dominant topic "{dominant_topic}" and subtopics "{subtopics_str}". Understand what specific app experience this represents. Generate an authentic {target_platform} review from the assigned customer type that clearly communicates this specific banking app experience with appropriate rating and detail level.

Platform being generated for: {target_platform}
Expected output format: {"With Title field" if target_platform == "App Store" else "Without Title field"}

Topic Analysis: {dominant_topic} - {subtopics_str}
Platform Context: {target_platform}
Channel Context: {channel}
""".strip()

        response = call_ollama_with_backoff(app_store_prompt)
        
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
            
            # Validate app store specific fields
            if 'rating' not in result:
                result['rating'] = 3
            if 'urgency' not in result:
                result['urgency'] = False
            if 'review_helpful' not in result:
                result['review_helpful'] = random.randint(0, 15)
            if 'platform' not in result:
                result['platform'] = target_platform
            if 'priority' not in result:
                result['priority'] = 'P2 - Medium'
            if 'sentiment' not in result:
                result['sentiment'] = 'Neutral'
            
            # Ensure rating is within valid range
            result['rating'] = max(1, min(5, result['rating']))
            
            # Ensure platform-specific fields are correct
            result['platform'] = target_platform
            
            # For App Store, ensure Title exists, for Google Play Store, ensure it doesn't
            if target_platform == "App Store":
                if 'Title' not in result or not result['Title']:
                    # Generate a simple title based on rating
                    if result['rating'] >= 4:
                        result['Title'] = "Great banking app experience"
                    elif result['rating'] <= 2:
                        result['Title'] = "App needs improvements"
                    else:
                        result['Title'] = "Mixed banking app experience"
            else:  # Google Play Store
                if 'Title' in result:
                    del result['Title']  # Remove Title field for Google Play Store
            
            # Log successful generation
            success_info = {
                'record_id': str(record_id),
                'username': username,
                'channel': channel,
                'platform': target_platform,
                'dominant_topic': dominant_topic,
                'subtopics': subtopics_str,
                'rating': result.get('rating', 3),
                'priority': result.get('priority', 'P2 - Medium'),
                'sentiment': result.get('sentiment', 'Neutral'),
                'urgency': result.get('urgency', False),
                'review_helpful': result.get('review_helpful', 0),
                'has_title': 'Title' in result,
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
            'platform': social_media_data.get('platform', 'Unknown'),
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_social_media_record(social_media_record):
    """Process a single social media record to generate app store review content"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate app store review content based on existing data
        review_content = generate_app_store_review_content(social_media_record)
        
        if not review_content:
            failure_counter.increment()
            return None
        
        # Prepare update document with all LLM-generated app store fields
        update_doc = {
            "rating": review_content.get('rating', 3),
            "priority": review_content.get('priority', 'P2 - Medium'),
            "review_helpful": review_content.get('review_helpful', random.randint(0, 15)),
            "sentiment": review_content.get('sentiment', 'Neutral'),
            "text": review_content.get('text', ''),
            "urgency": review_content.get('urgency', False),
            "platform": review_content.get('platform', 'Google Play Store'),
            "domain": "banking",
            "content_generated_at": datetime.now().isoformat()
        }
        
        # Add Title field only for App Store (iOS)
        if 'Title' in review_content:
            update_doc['Title'] = review_content['Title']
        
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
    """Get collection statistics for social media records with App Store/Google Play channel"""
    try:
        # Count all social media records
        total_count = social_media_col.count_documents({})
        
        # Count App Store/Google Play records specifically
        app_store_query = {"channel": "App Store/Google Play"}
        app_store_count = social_media_col.count_documents(app_store_query)
        
        # Count by different channel variations
        app_store_variations = social_media_col.count_documents({
            "channel": {"$regex": "app.*store|google.*play", "$options": "i"}
        })
        
        # Count App Store/Google Play records with and without generated content
        with_content = social_media_col.count_documents({
            "channel": "App Store/Google Play",
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "rating": {"$exists": True, "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None},
            "platform": {"$exists": True, "$ne": "", "$ne": None},
            "review_helpful": {"$exists": True}
        })
        
        without_content = app_store_count - with_content
        
        # Count by platform field values
        app_store_ios = social_media_col.count_documents({
            "channel": "App Store/Google Play",
            "platform": "App Store"
        })
        
        google_play = social_media_col.count_documents({
            "channel": "App Store/Google Play", 
            "platform": "Google Play Store"
        })
        
        # Count other platform field values
        other_platforms = social_media_col.count_documents({
            "channel": "App Store/Google Play",
            "platform": {"$nin": ["App Store", "Google Play Store", None, ""]}
        })
        
        logger.info("EU Bank Social Media Collection Statistics:")
        logger.info(f"Total social media records: {total_count}")
        logger.info(f"App Store/Google Play records (exact match): {app_store_count}")
        logger.info(f"App Store/Google Play records (all variations): {app_store_variations}")
        logger.info(f"With generated content: {with_content}")
        logger.info(f"Without generated content: {without_content}")
        logger.info(f"Platform field = 'App Store': {app_store_ios}")
        logger.info(f"Platform field = 'Google Play Store': {google_play}")
        logger.info(f"Platform field = Other values: {other_platforms}")
        
        # Show sample channel values
        sample_channels = social_media_col.distinct("channel")
        logger.info(f"Available channels: {sample_channels}")
        
        # Show sample platform values for App Store/Google Play channel
        platform_values = social_media_col.distinct("platform", {"channel": "App Store/Google Play"})
        logger.info(f"Platform values in App Store/Google Play records: {platform_values}")
        
        # Show sample record structure for App Store/Google Play
        sample = social_media_col.find_one({"channel": "App Store/Google Play"})
        if sample:
            logger.info("Sample App Store/Google Play record structure:")
            for key, value in sample.items():
                if key == '_id':
                    logger.info(f"  {key}: {value}")
                elif isinstance(value, str) and len(value) > 50:
                    logger.info(f"  {key}: {value[:50]}...")
                else:
                    logger.info(f"  {key}: {value}")
        else:
            logger.info("No App Store/Google Play records found in collection")
        
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

def test_platform_field_usage():
    """Test the platform field usage functionality"""
    logger.info("Testing platform field usage functionality...")
    
    # Test cases for platform field usage
    test_cases = [
        {
            'channel': 'App Store/Google Play',
            'platform': 'Google Play Store',
            'username': 'testuser123',
            'expected': 'Google Play Store',
            'description': 'Valid Google Play Store platform field'
        },
        {
            'channel': 'App Store/Google Play',
            'platform': 'App Store',
            'username': 'iosuser',
            'expected': 'App Store',
            'description': 'Valid App Store platform field'
        },
        {
            'channel': 'App Store/Google Play',
            'platform': 'google play store',
            'username': 'androiduser',
            'expected': 'Google Play Store',
            'description': 'Lowercase Google Play Store normalization'
        },
        {
            'channel': 'App Store/Google Play',
            'platform': 'app store',
            'username': 'appleuser',
            'expected': 'App Store',
            'description': 'Lowercase App Store normalization'
        },
        {
            'channel': 'App Store/Google Play',
            'platform': '',
            'username': 'emptyuser',
            'expected': 'Google Play Store',
            'description': 'Empty platform field fallback'
        },
        {
            'channel': 'App Store/Google Play',
            'platform': 'Invalid Platform',
            'username': 'invaliduser',
            'expected': 'Google Play Store',
            'description': 'Invalid platform field fallback'
        }
    ]
    
    for i, test_case in enumerate(test_cases, 1):
        logger.info(f"Test case {i}: {test_case['description']}")
        
        # Create mock social media data
        test_data = {
            '_id': f'test_{i}',
            'channel': test_case['channel'],
            'platform': test_case['platform'],
            'username': test_case['username'],
            'dominant_topic': 'App Performance',
            'subtopics': 'General functionality'
        }
        
        try:
            determined_platform = get_platform_from_record(test_data)
            
            if determined_platform == test_case['expected']:
                logger.info(f"✅ Test {i} PASSED: Correctly determined '{determined_platform}'")
            else:
                logger.warning(f"❌ Test {i} FAILED: Expected '{test_case['expected']}', got '{determined_platform}'")
                
        except Exception as e:
            logger.error(f"❌ Test {i} ERROR: {e}")
    
    logger.info("Platform field usage test complete.")

def update_social_media_app_store_reviews():
    """Update social media records with App Store/Google Play channel with generated review content"""
    
    logger.info("Starting EU Bank Social Media App Store Review Content Generation...")
    logger.info(f"Target Channel: App Store/Google Play")
    logger.info(f"Collection: {SOCIAL_MEDIA_COLLECTION}")
    logger.info(f"Bank: EU Bank (European)")
    logger.info(f"Platform Source: Database platform field")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    
    # Test Ollama connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        return
    
    # Test platform field usage
    test_platform_field_usage()
    
    # Get all social media records with App Store/Google Play channel that need content generation
    logger.info("Fetching App Store/Google Play social media records from database...")
    try:
        # Search for App Store/Google Play records without generated content
        query = {
            "channel": "App Store/Google Play",
            "$or": [
                {"text": {"$exists": False}},
                {"rating": {"$exists": False}},
                {"priority": {"$exists": False}},
                {"sentiment": {"$exists": False}},
                {"review_helpful": {"$exists": False}},
                {"text": {"$in": [None, ""]}},
                {"rating": {"$in": [None]}},
                {"priority": {"$in": [None, ""]}},
                {"sentiment": {"$in": [None, ""]}},
                {"review_helpful": {"$in": [None]}}
            ]
        }
        
        social_media_records = list(social_media_col.find(query))
        total_records = len(social_media_records)
        
        if total_records == 0:
            logger.info("No App Store/Google Play social media records found needing content generation!")
            logger.info("Checking what App Store/Google Play records exist...")
            
            # Show what App Store/Google Play records exist
            all_app_store_records = list(social_media_col.find({"channel": "App Store/Google Play"}))
            if all_app_store_records:
                logger.info(f"Found {len(all_app_store_records)} App Store/Google Play records with existing content")
                sample = all_app_store_records[0]
                logger.info("Sample existing App Store/Google Play record:")
                for key, value in sample.items():
                    logger.info(f"  {key}: {value}")
            else:
                logger.info("No App Store/Google Play records found at all!")
                
                # Check for similar channel names
                all_channels = social_media_col.distinct("channel")
                logger.info(f"Available channels in collection: {all_channels}")
                
                # Check for app store related channels with case insensitive search
                app_store_related = social_media_col.count_documents({
                    "channel": {"$regex": "app.*store|google.*play", "$options": "i"}
                })
                logger.info(f"Records with app store related channels: {app_store_related}")
            return
            
        logger.info(f"Found {total_records} App Store/Google Play social media records needing content generation")
        
        # Show platform field distribution
        platform_distribution = {}
        for record in social_media_records:
            platform = record.get('platform', 'Unknown')
            platform_distribution[platform] = platform_distribution.get(platform, 0) + 1
        
        logger.info("Platform field distribution in records to process:")
        for platform, count in platform_distribution.items():
            logger.info(f"  {platform}: {count} records")
        
    except Exception as e:
        logger.error(f"Error fetching App Store/Google Play social media records: {e}")
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
            
            # Show platform distribution for this batch
            batch_platforms = {}
            for record in batch_records:
                platform = get_platform_from_record(record)
                batch_platforms[platform] = batch_platforms.get(platform, 0) + 1
            logger.info(f"Batch {batch_num} platform distribution: {batch_platforms}")
            
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
        
        logger.info("EU Bank Social Media App Store review generation complete!")
        logger.info(f"Total records updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def main():
    """Main function to initialize and run the EU Bank Social Media App Store review generator"""
    logger.info("EU Bank Social Media App Store Review Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {SOCIAL_MEDIA_COLLECTION}")
    logger.info(f"Target Channel: App Store/Google Play")
    logger.info(f"Bank: EU Bank (European Banking)")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_BASE_URL}")
    logger.info(f"Platform Source: Database platform field")
    
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
        
        # Run the social media app store review generation
        update_social_media_app_store_reviews()
        
        # Show final statistics
        get_collection_stats()
        
    except KeyboardInterrupt:
        logger.info("EU Bank Social Media App Store review generation interrupted!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        cleanup_resources()
        
        logger.info("Session complete. Check log files:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")

# Run the EU Bank Social Media App Store review generator
if __name__ == "__main__":
    main()