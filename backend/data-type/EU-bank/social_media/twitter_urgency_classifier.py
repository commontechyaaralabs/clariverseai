# Twitter Urgency Classifier - OpenRouter Integration
import os
import time
import json
import requests
import signal
import sys
import logging
from datetime import datetime
from pymongo import MongoClient
from dotenv import load_dotenv
import backoff
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
from pathlib import Path
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"twitter_urgency_classifier_{timestamp}.log"
SUCCESS_LOG_FILE = LOG_DIR / f"successful_urgency_classifications_{timestamp}.log"
FAILURE_LOG_FILE = LOG_DIR / f"failed_urgency_classifications_{timestamp}.log"

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

# Global variables
client = None
db = None
social_media_col = None

# OpenRouter setup
OPENROUTER_API_KEY = "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"
OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1/chat/completions"
OPENROUTER_MODEL = "google/gemma-3-27b-it:free"

# Configuration
REQUEST_TIMEOUT = 180
MAX_RETRIES = 3
RETRY_DELAY = 2

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
        social_media_col.create_index("urgency")
        logger.info("Database connection established and indexes created")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

@backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, json.JSONDecodeError, KeyError, ValueError),
    max_tries=MAX_RETRIES,
    max_time=300,
    base=RETRY_DELAY,
    on_backoff=lambda details: logger.warning(f"Retry {details['tries']}/{MAX_RETRIES} after {details['wait']:.1f}s")
)
def call_openrouter_with_backoff(prompt, timeout=REQUEST_TIMEOUT):
    """Call OpenRouter API with exponential backoff and better error handling"""
    
    # Add delay for free tier rate limiting
    time.sleep(2)
    
    headers = {
        'Authorization': f'Bearer {OPENROUTER_API_KEY}',
        'Content-Type': 'application/json',
        'HTTP-Referer': 'https://github.com/your-repo',
        'X-Title': 'Twitter Urgency Classifier'
    }
        
    payload = {
        "model": OPENROUTER_MODEL,
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant that analyzes banking-related social media content to determine urgency. Always respond with valid JSON format as requested."
            },
            {
                "role": "user", 
                "content": prompt
            }
        ],
        "temperature": 0.3,
        "max_tokens": 100,
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

def classify_twitter_urgency(twitter_record):
    """Classify urgency for a Twitter record based on its text content"""
    
    start_time = time.time()
    record_id = twitter_record.get('_id', 'unknown')
    text_content = twitter_record.get('text', '')
    
    if not text_content:
        logger.warning(f"Record {record_id} has no text content")
        return False
    
    try:
        # Create urgency classification prompt
        urgency_prompt = f"""
Analyze this banking Twitter post and determine if it's urgent (requires immediate attention) or not urgent.

**URGENT indicators:**
- Financial emergencies (fraud, unauthorized transactions, account lockouts)
- Critical system failures affecting customer access
- Security breaches or suspicious activities
- Time-sensitive financial issues (payment deadlines, overdrafts)
- Customer service escalations with immediate impact
- System outages preventing essential banking operations

**NOT URGENT indicators:**
- General complaints or feedback
- Feature requests or suggestions
- Routine inquiries
- General dissatisfaction without immediate impact
- Questions about services or policies
- Non-critical technical issues

**Twitter Post Text:**
"{text_content}"

**Output JSON format:**
{{
  "urgency": true/false
}}

Respond with ONLY the JSON object, no additional text.
""".strip()

        response = call_openrouter_with_backoff(urgency_prompt)
        
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
            
            # Validate the response structure
            if 'urgency' not in result:
                raise ValueError("Missing 'urgency' field in LLM response")
            
            urgency_value = result.get('urgency', False)
            
            # Ensure urgency is boolean
            if isinstance(urgency_value, str):
                urgency_value = urgency_value.lower() in ['true', '1', 'yes', 'urgent']
            elif not isinstance(urgency_value, bool):
                urgency_value = bool(urgency_value)
            
            classification_time = time.time() - start_time
            
            # Log successful classification
            success_info = {
                'record_id': str(record_id),
                'text_preview': text_content[:100] + "..." if len(text_content) > 100 else text_content,
                'urgency': urgency_value,
                'classification_time': classification_time
            }
            success_logger.info(json.dumps(success_info))
            
            return urgency_value
            
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
    except Exception as e:
        classification_time = time.time() - start_time
        error_info = {
            'record_id': str(record_id),
            'text_preview': text_content[:100] + "..." if len(text_content) > 100 else text_content,
            'error': str(e),
            'classification_time': classification_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def get_twitter_collection_stats():
    """Get collection statistics for Twitter records"""
    try:
        # Case-insensitive search for Twitter records
        total_count = social_media_col.count_documents({
            "channel": {"$regex": "^twitter$", "$options": "i"}
        })
        
        # Count Twitter records with and without urgency classification
        with_urgency = social_media_col.count_documents({
            "channel": {"$regex": "^twitter$", "$options": "i"},
            "urgency": {"$exists": True, "$ne": None}
        })
        
        without_urgency = total_count - with_urgency
        
        # Count urgent vs non-urgent
        urgent_count = social_media_col.count_documents({
            "channel": {"$regex": "^twitter$", "$options": "i"},
            "urgency": True
        })
        
        non_urgent_count = social_media_col.count_documents({
            "channel": {"$regex": "^twitter$", "$options": "i"},
            "urgency": False
        })
        
        logger.info("Twitter Collection Statistics:")
        logger.info(f"Total Twitter records: {total_count}")
        logger.info(f"With urgency classification: {with_urgency}")
        logger.info(f"Without urgency classification: {without_urgency}")
        logger.info(f"Urgent records: {urgent_count}")
        logger.info(f"Non-urgent records: {non_urgent_count}")
        
        # Show sample record structure
        sample = social_media_col.find_one({"channel": {"$regex": "^twitter$", "$options": "i"}})
        if sample:
            logger.info("Sample record structure:")
            for key, value in sample.items():
                if key == '_id':
                    logger.info(f"  {key}: {value}")
                elif key == 'text' and isinstance(value, str) and len(value) > 50:
                    logger.info(f"  {key}: {value[:50]}...")
                else:
                    logger.info(f"  {key}: {value}")
        
    except Exception as e:
        logger.error(f"Error getting Twitter collection stats: {e}")

def test_openrouter_connection():
    """Test if OpenRouter is available and model is accessible"""
    try:
        logger.info(f"Testing connection to OpenRouter...")
        logger.info(f"Using model: {OPENROUTER_MODEL}")
        
        test_prompt = "Generate a JSON object with 'urgency': true and nothing else."
        
        headers = {
            'Authorization': f'Bearer {OPENROUTER_API_KEY}',
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://github.com/your-repo',
            'X-Title': 'Twitter Urgency Classifier Test'
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

def classify_twitter_urgency_batch():
    """Classify urgency for all Twitter records that don't have urgency classification"""
    
    logger.info("Starting Twitter Urgency Classification...")
    
    # Test OpenRouter connection
    if not test_openrouter_connection():
        logger.error("Cannot proceed without OpenRouter connection")
        return
    
    # Get all Twitter records that need urgency classification
    logger.info("Fetching Twitter records from database...")
    try:
        # Case-insensitive search for Twitter records without urgency classification
        query = {
            "channel": {"$regex": "^twitter$", "$options": "i"},
            "$or": [
                {"urgency": {"$exists": False}},
                {"urgency": {"$in": [None, ""]}}
            ]
        }
        
        twitter_records = list(social_media_col.find(query))
        total_records = len(twitter_records)
        
        if total_records == 0:
            logger.info("No Twitter records found needing urgency classification!")
            logger.info("Checking what Twitter records exist...")
            
            # Show what Twitter records exist
            all_twitter = list(social_media_col.find({"channel": {"$regex": "^twitter$", "$options": "i"}}))
            if all_twitter:
                logger.info(f"Found {len(all_twitter)} Twitter records with existing urgency classification")
                sample = all_twitter[0]
                logger.info("Sample existing Twitter record:")
                for key, value in sample.items():
                    logger.info(f"  {key}: {value}")
            else:
                logger.info("No Twitter records found at all!")
                # Show available channels
                channels = social_media_col.distinct("channel")
                logger.info(f"Available channels: {channels}")
            return
            
        logger.info(f"Found {total_records} Twitter records needing urgency classification")
        
    except Exception as e:
        logger.error(f"Error fetching Twitter records: {e}")
        return
    
    # Process records one by one to avoid rate limiting
    total_updated = 0
    success_count = 0
    failure_count = 0
    
    try:
        for i, record in enumerate(twitter_records, 1):
            record_id = record.get('_id', 'unknown')
            text_content = record.get('text', '')
            
            logger.info(f"Processing record {i}/{total_records} (ID: {record_id})")
            
            if not text_content:
                logger.warning(f"Record {record_id} has no text content, skipping...")
                continue
            
            try:
                # Classify urgency
                urgency_value = classify_twitter_urgency(record)
                
                # Update the record with urgency classification
                update_result = social_media_col.update_one(
                    {"_id": record['_id']},
                    {"$set": {"urgency": urgency_value}}
                )
                
                if update_result.modified_count > 0:
                    total_updated += 1
                    success_count += 1
                    logger.info(f"✓ Record {record_id} classified as {'URGENT' if urgency_value else 'NOT URGENT'}")
                else:
                    logger.warning(f"Failed to update record {record_id}")
                    failure_count += 1
                
            except Exception as e:
                logger.error(f"Error processing record {record_id}: {str(e)}")
                failure_count += 1
            
            # Progress update every 10 records
            if i % 10 == 0:
                logger.info(f"Progress: {i}/{total_records} records processed")
                logger.info(f"Success: {success_count} | Failures: {failure_count}")
        
        logger.info("Twitter urgency classification complete!")
        logger.info(f"Total records updated: {total_updated}")
        logger.info(f"Successful classifications: {success_count}")
        logger.info(f"Failed classifications: {failure_count}")
        
    except KeyboardInterrupt:
        logger.info("Classification interrupted by user!")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

def main():
    """Main function to initialize and run the Twitter urgency classifier"""
    logger.info("Twitter Urgency Classifier Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {SOCIAL_MEDIA_COLLECTION}")
    logger.info(f"Target Channel: Twitter (case-insensitive)")
    logger.info(f"Model: {OPENROUTER_MODEL}")
    
    # Initialize database
    if not init_database():
        logger.error("Cannot proceed without database connection")
        return
    
    try:
        # Show current collection stats
        get_twitter_collection_stats()
        
        # Run the urgency classification
        classify_twitter_urgency_batch()
        
        # Show final statistics
        get_twitter_collection_stats()
        
    except KeyboardInterrupt:
        logger.info("Twitter urgency classification interrupted!")
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        cleanup_resources()
        
        logger.info("Session complete. Check log files:")
        logger.info(f"Main Log: {MAIN_LOG_FILE}")
        logger.info(f"Success Log: {SUCCESS_LOG_FILE}")
        logger.info(f"Failure Log: {FAILURE_LOG_FILE}")

# Run the Twitter urgency classifier
if __name__ == "__main__":
    main()
