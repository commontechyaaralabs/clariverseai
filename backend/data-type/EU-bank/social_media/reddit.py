# Reddit Social Media Content Generator - Fixed Version
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

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
SOCIAL_MEDIA_COLLECTION = "socialmedia"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"reddit_generator_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_reddit_generations_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_reddit_generations_{timestamp}.log"

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

# OpenRouter setup
OPENROUTER_API_KEY = "sk-xxxx"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"

# Optimized batch processing configuration
BATCH_SIZE = 2  # Very conservative for free tier
CPU_COUNT = multiprocessing.cpu_count()
MAX_WORKERS = min(CPU_COUNT, 4)  # Very conservative for free tier
REQUEST_TIMEOUT = 180
MAX_RETRIES = 3
RETRY_DELAY = 2  # Slightly longer delay

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
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=300,  # Increased max time
    base=RETRY_DELAY,
    on_backoff=lambda details: logger.warning(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_openrouter_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call OpenRouter API with exponential backoff and better error handling"""
    if shutdown_flag.is_set():
        raise KeyboardInterrupt("Shutdown requested")
    
    # Add delay for free tier rate limiting
    time.sleep(2)
    
    headers = {
        'Authorization': f'Bearer {OPENROUTER_API_KEY}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'https://github.com/your-repo',
        'X-Title': 'Reddit Content Generator'
    }
        
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that generates authentic Reddit social media content for banking discussions. Always respond with valid JSON format as requested."
            },
            {
                "role": "user", 
                "content": prompt
            }
        ],
        "temperature": 0.7,
        "max_tokens": 1200,
        "top_p": 0.9
    }
    
    try:
        response = requests.post(
            OPENROUTER_BASE_URL, 
            json=payload, 
            headers=headers,
            timeout=timeout
        )
        
        if not response.text.strip():
            raise ValueError("Empty response from OpenRouter API")
        
        response.raise_for_status()
        
        try:
            result = response.json()
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error. Response text: {response.text[:200]}...")
            raise
        
        if "choices" not in result or len(result["choices"]) == 0:
            logger.error(f"No 'choices' field or empty choices. Available fields: {list(result.keys())}")
            raise KeyError("No valid choices in OpenRouter response")
        
        content = result["choices"][0]["message"]["content"]
        return content
        
    except requests.exceptions.Timeout:
        logger.warning(f"Request timed out after {timeout} seconds. This might be due to model processing time.")
        raise
    except requests.exceptions.ConnectionError:
        logger.error("Connection error - check OpenRouter endpoint")
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
        logger.error(f"OpenRouter API error: {e}")
        raise

def generate_reddit_post_content(social_media_data):
    """Generate Reddit post content based on dominant topic and subtopics"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    record_id = social_media_data.get('_id', 'unknown')
    
    try:
        # Extract data from social media record
        dominant_topic = social_media_data.get('dominant_topic', 'General Banking')
        subtopics = social_media_data.get('subtopics', 'General operations')
        username = social_media_data.get('username', 'Anonymous User')
        
        # Handle subtopics whether it's a list or string
        if isinstance(subtopics, str):
            subtopics_list = [subtopic.strip() for subtopic in subtopics.split(',') if subtopic.strip()]
        elif isinstance(subtopics, list):
            subtopics_list = subtopics
        else:
            subtopics_list = ['General operations']
        
        # Prepare subtopics as formatted string
        subtopics_str = ', '.join(subtopics_list)
        
        # Enhanced Reddit prompt focused on your data structure

        reddit_prompt = f"""
Write a genuine Reddit post as a banking customer sharing their experience. Think like a real person telling their story naturally.

**YOUR BANKING EXPERIENCE:**
You've dealt with {dominant_topic} involving {subtopics_str}. Share this through your personal story, not as topic categories.


**WRITE NATURALLY:**
Start your post however feels most natural for YOUR specific situation. Real people don't follow templates - they just start talking about what happened to them. Some people jump right into the problem, others give background first, some ask questions, others make statements. Write however YOU would naturally begin telling this story.

**BE AUTHENTIC:**
- Use your natural speaking voice
- Include personal details that matter to your story
- Express genuine emotions about what happened
- Write like you're explaining the situation to a friend
- Use everyday language, not formal complaint language

**AVOID REPETITIVE PATTERNS:**
Never start posts the same way. Each person tells their story differently based on their personality, situation, and what matters most to them. Let your opening flow naturally from your specific circumstances.

**STORY VARIETY:**
Your experience could be:
- A recent incident you're dealing with
- An ongoing problem you're frustrated about
- Something you discovered that surprised you
- A situation that's affecting your life
- A pattern you've noticed over time
- A resolution you want to share

**NATURAL LANGUAGE:**
- Use contractions and casual speech
- Include realistic hesitations or uncertainties
- Mix sentence lengths naturally
- Add personal context that makes it real
- Express emotions the way you actually would

**OUTPUT JSON:**
{{
  "priority": "P1 - Critical|P2 - Medium|P3 - Low",
  "like_count": <integer 5-50>,
  "share_count": <integer 0-15>,
  "comment_count": <integer 1-30>,
  "sentiment": "Positive|Negative|Neutral", 
  "text": "<authentic Reddit post written in your natural voice about your banking experience>",
  "urgency": true|false
}}

Write as yourself sharing a real experience. Let your personality and situation naturally determine how you start and tell your story.

Banking issues: {dominant_topic} - {subtopics_str}
Username: {username}
""".strip()

        response = call_openrouter_with_backoff(reddit_prompt)
        
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
            
            # Log successful generation
            success_info = {
                'record_id': str(record_id),
                'username': username,
                'dominant_topic': dominant_topic,
                'subtopics': subtopics_str,
                'priority': result.get('priority', 'P2 - Medium'),
                'sentiment': result.get('sentiment', 'Neutral'),
                'generation_time': generation_time,
                'urgency': result.get('urgency', False)
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
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_reddit_record(social_media_record):
    """Process a single social media record to generate Reddit content"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate Reddit content based on existing data
        reddit_content = generate_reddit_post_content(social_media_record)
        
        if not reddit_content:
            failure_counter.increment()
            return None
        
        # Prepare update document with all LLM-generated fields
        update_doc = {
            "priority": reddit_content.get('priority', 'P2 - Medium'),
            "like_count": reddit_content.get('like_count', random.randint(5, 50)),
            "share_count": reddit_content.get('share_count', random.randint(0, 10)),
            "comment_count": reddit_content.get('comment_count', random.randint(1, 25)),
            "sentiment": reddit_content.get('sentiment', 'Neutral'),
            "text": reddit_content.get('text', ''),
            "urgency": reddit_content.get('urgency', False),
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
    """Get collection statistics for Reddit records"""
    try:
        # Case-insensitive search for Reddit records
        total_count = social_media_col.count_documents({
            "channel": {"$regex": "^reddit$", "$options": "i"}
        })
        
        # Count Reddit records with and without generated content
        with_content = social_media_col.count_documents({
            "channel": {"$regex": "^reddit$", "$options": "i"},
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "priority": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_content = total_count - with_content
        
        logger.info("Reddit Collection Statistics:")
        logger.info(f"Total Reddit records: {total_count}")
        logger.info(f"With generated content: {with_content}")
        logger.info(f"Without generated content: {without_content}")
        
        # Show sample record structure
        sample = social_media_col.find_one({"channel": {"$regex": "^reddit$", "$options": "i"}})
        if sample:
            logger.info("Sample record structure:")
            for key, value in sample.items():
                if key == '_id':
                    logger.info(f"  {key}: {value}")
                elif isinstance(value, str) and len(value) > 50:
                    logger.info(f"  {key}: {value[:50]}...")
                else:
                    logger.info(f"  {key}: {value}")
        
    except Exception as e:
        logger.error(f"Error getting Reddit collection stats: {e}")

def test_openrouter_connection():
    """Test if OpenRouter is available and model is accessible"""
    try:
        logger.info(f"Testing connection to OpenRouter...")
        logger.info(f"Using model: {OPENROUTER_MODEL}")
        
        test_prompt = "Generate a JSON object with 'test': 'success' and nothing else."
        
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://github.com/your-repo',
            'X-Title': 'Reddit Content Generator Test'
        }
        
        payload = {
            "model": OPENROUTER_MODEL,
            "messages": [
                {"role": "system", "content": "You are a helpful assistant that responds with valid JSON."},
                {"role": "user", "content": test_prompt}
            ],
            "temperature": 0.1,
            "max_tokens": 50
        }
        
        response = requests.post(
            OPENROUTER_BASE_URL,
            json=payload,
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            if "choices" in result and len(result["choices"]) > 0:
                logger.info("OpenRouter connection test successful")
                return True
            else:
                logger.error("Invalid response structure from OpenRouter")
                return False
        else:
            logger.error(f"OpenRouter test failed with status: {response.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"OpenRouter connection test failed: {e}")
        return False

def update_reddit_posts_with_content():
    """Update Reddit social media records with generated content"""
    
    logger.info("Starting Reddit Social Media Content Generation...")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    
    # Test OpenRouter connection
    if not test_openrouter_connection():
        logger.error("Cannot proceed without OpenRouter connection")
        return
    
    # Get all Reddit social media records that need content generation
    logger.info("Fetching Reddit social media records from database...")
    try:
        # Case-insensitive search for Reddit records without generated content
        query = {
            "channel": {"$regex": "^reddit$", "$options": "i"},
            "$or": [
                {"text": {"$exists": False}},
                {"priority": {"$exists": False}},
                {"sentiment": {"$exists": False}},
                {"text": {"$in": [None, ""]}},
                {"priority": {"$in": [None, ""]}},
                {"sentiment": {"$in": [None, ""]}}
            ]
        }
        
        social_media_records = list(social_media_col.find(query))
        total_records = len(social_media_records)
        
        if total_records == 0:
            logger.info("No Reddit records found needing content generation!")
            logger.info("Checking what Reddit records exist...")
            
            # Show what Reddit records exist
            all_reddit = list(social_media_col.find({"channel": {"$regex": "^reddit$", "$options": "i"}}))
            if all_reddit:
                logger.info(f"Found {len(all_reddit)} Reddit records with existing content")
                sample = all_reddit[0]
                logger.info("Sample existing Reddit record:")
                for key, value in sample.items():
                    logger.info(f"  {key}: {value}")
            else:
                logger.info("No Reddit records found at all!")
                # Show available channels
                channels = social_media_col.distinct("channel")
                logger.info(f"Available channels: {channels}")
            return
            
        logger.info(f"Found {total_records} Reddit records needing content generation")
        
    except Exception as e:
        logger.error(f"Error fetching social media records: {e}")
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
            
            # Process batch with reduced parallelization for free tier
            successful_updates = []
            
            # Use ThreadPoolExecutor with limited workers
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(process_single_reddit_record, record): record 
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
        
        logger.info("Reddit content generation complete!")
        logger.info(f"Total records updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def main():
    """Main function to initialize and run the Reddit content generator"""
    logger.info("Reddit Social Media Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {SOCIAL_MEDIA_COLLECTION}")
    logger.info(f"Target Channel: Reddit (case-insensitive)")
    logger.info(f"Model: {OPENROUTER_MODEL}")
    
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
        
        # Run the Reddit content generation
        update_reddit_posts_with_content()
        
        # Show final statistics
        get_collection_stats()
        
    except KeyboardInterrupt:
        logger.info("Reddit content generation interrupted!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        cleanup_resources()
        
        logger.info("Session complete. Check log files:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")

# Run the Reddit content generator
if __name__ == "__main__":
    main()