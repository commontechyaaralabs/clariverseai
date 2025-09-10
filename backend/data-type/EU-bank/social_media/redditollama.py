# Reddit Content Generator - Ollama Version
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

# Ollama setup - using same configuration as email generator
OLLAMA_BASE_URL = "https://hey-assurance-bikini-philip.trycloudflare.com"
OLLAMA_TOKEN = "11c3922465e2e6060ea01f1aac894cfdff8c7870d9e46ab4061d69c71973e439"
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
            "temperature": 0.7,
            "num_predict": 1200,
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
        
        # Enhanced Reddit prompt

        # Generate unique seeds for each call
        topic_hash = abs(hash(dominant_topic + subtopics_str + username)) % 1000
        length_hash = abs(hash(str(len(dominant_topic + subtopics_str)))) % 1000
        user_hash = abs(hash(username)) % 1000

        reddit_prompt = f"""
You are a European bank customer sharing your experience on Reddit. Analyze the DOMINANT TOPIC and SUBTOPICS to understand the exact banking experience, then generate an authentic Reddit post.

**BANKING EXPERIENCE TO ANALYZE:**
Dominant Topic: {dominant_topic}
Subtopics: {subtopics_str}

**ANALYSIS INSTRUCTIONS:**
1. **Determine Experience Type from Dominant Topic:**
   - Words like "Outstanding", "Excellent", "Successful", "Smooth", "Quick", "Resolved" → POSITIVE experience
   - Words like "Delayed", "Failed", "Problems", "Issues", "Error", "Blocked", "Declined" → NEGATIVE experience  
   - Words like "Update", "Process", "Request", "Inquiry", "Information" → NEUTRAL experience

2. **Understand Specific Context from Subtopics:**
   - Identify the exact banking service area (payments, cards, loans, etc.)
   - Determine technical vs. customer service vs. process issues
   - Understand scope and impact of the experience

**FORCED VARIATION PARAMETERS:**
- Customer Type: {['tech_entrepreneur', 'retired_professional', 'university_student', 'freelance_creative', 'working_parent', 'expat_consultant', 'small_shop_owner', 'recent_graduate'][topic_hash % 8]}
- Writing Style: {['detailed_storyteller', 'straight_to_point', 'cautiously_asking', 'venting_frustrated', 'professionally_analytical', 'casually_conversational', 'genuinely_grateful', 'seeking_validation'][length_hash % 8]}
- Context: {['just_happened_today', 'ongoing_for_weeks', 'recurring_monthly_issue', 'first_time_banking', 'comparing_to_previous_bank', 'business_impact_serious', 'personal_life_affected', 'following_up_previous_post'][user_hash % 8]}

**REDDIT POST GENERATION RULES:**

**For POSITIVE Experiences:**
- Express genuine appreciation for good service
- Mention specific benefits or quick resolution
- Reference helpful staff or smooth processes
- Use appreciative tone matching the customer type
- Include relevant positive outcome

**For NEGATIVE Experiences:**
- Focus on the specific problem mentioned in dominant topic
- Connect subtopics to real-world consequences
- Express frustration appropriate to customer type
- Demand resolution or express disappointment
- Reference impact on daily life/business

**For NEUTRAL Experiences:**
- Ask questions about services or processes
- Share factual updates about banking procedures
- Request information or clarification
- Use informative, straightforward tone

**DIVERSITY REQUIREMENTS:**
- NEVER repeat time references: vary between "this morning", "all week", "since Monday", "20 minutes", "3 days", etc.
- Avoid repetitive phrases: "sort this out", "seriously?", "real-time processing"
- Use different problem descriptions for same issues
- Vary emotional intensity based on customer type and context
- NEVER mention specific bank names - use generic terms like "my bank", "the bank", "they", "them"

**CUSTOMER-APPROPRIATE LANGUAGE:**

**Tech Entrepreneur:** Technical terms, efficiency focus, business impact language
**Retired Professional:** Formal tone, references to long-term relationship, traditional expectations  
**University Student:** Casual language, budget concerns, social context
**Freelance Creative:** Creative metaphors, flexible lifestyle references, irregular income context
**Working Parent:** Time constraints, family impact, practical concerns
**Expat Consultant:** International context, professional needs, location references
**Small Shop Owner:** Business operations, cash flow, customer service impact
**Recent Graduate:** Learning process, first adult banking, contemporary language

**EMOJI USAGE GUIDELINES:**
- Use emojis SPARINGLY and only when they add genuine value to the post
- Appropriate for: casual users, students, creative types, when expressing strong emotions
- Avoid for: formal complaints, professional contexts, technical discussions
- Maximum 1-2 emojis per post, placed naturally within the text
- NEVER overuse emojis or use them in every sentence

**REDDIT POST CHARACTERISTICS:**

**LENGTH VARIETY:**
- Short posts (50-150 words): Quick vent, simple question, brief update
- Medium posts (150-400 words): Detailed story with context and consequences  
- Long posts (400-500 words): Complex situations, multiple issues, seeking comprehensive advice

**NATURAL REDDIT OPENING PATTERNS:**
Rotate these based on your persona and situation:

**Pattern A - Direct Problem Statement:**
"So my bank just [specific issue]. Anyone else dealing with this?"

**Pattern B - Background + Current Issue:**
"I've been with them for [time] and never had issues until [recent problem]..."

**Pattern C - Question-Based Opening:**
"Is it normal for [bank service] to [problem]? Because [your specific situation]..."

**Pattern D - Timeline Story:**
"This started [time period] when [initial incident]. Now [current situation]..."

**Pattern E - Comparative Context:**
"Switched from [other bank] to my current bank because [reason], but now [current issue]..."

**Pattern F - Seeking Validation:**
"Am I overreacting or is this bank behavior actually [your judgment]?"

**Pattern G - Warning/PSA Style:**
"Heads up about [banking issue] - [your experience] might affect others too"

**Pattern H - Success/Resolution Story:**
"Update: They actually [positive outcome]. Here's what happened..."

**PERSONA-SPECIFIC LANGUAGE:**

**Tech Entrepreneur:** Technical terms, efficiency focus, business impact language
*"Their mobile API keeps timing out during transfers. As a dev, this is embarrassing level coding..."*

**Retired Professional:** Formal tone, references to long-term relationship, traditional expectations
*"I've banked with them since 1985, back when you knew your teller's name. This new online system is..."*

**University Student:** Casual language, budget concerns, social context
*"Bruh, they charged me €35 for overdraft on a €3 coffee. I'm literally eating ramen for a week now..."*

**Freelance Creative:** Creative metaphors, flexible lifestyle references, irregular income context
*"My client payment vanished into the digital void. As someone whose income is already unpredictable..."*

**Working Parent:** Time constraints, family impact, practical concerns
*"Between soccer practice and grocery runs, I don't have time for their system to be down. The kids need..."*

**Expat Consultant:** International context, professional needs, location references
*"Working across EU borders, reliable banking isn't optional. When their SEPA transfers fail during client payments..."*

**Small Shop Owner:** Business operations, cash flow, customer service impact
*"When your payment processor fails during morning rush, every missed transaction hurts. Small businesses can't afford..."*

**Recent Graduate:** Learning process, first adult banking, contemporary language
*"Just got my first real job and thought adult banking would be straightforward. Apparently not when..."*

**REDDIT-SPECIFIC ELEMENTS:**

**Community Engagement:**
- Ask for others' experiences: "Anyone else had this issue?"
- Seek advice: "What would you do in my situation?"
- Request recommendations: "Should I switch banks?"
- Share warnings: "Avoid this if you [specific use case]"
- Provide updates: "UPDATE: They finally [resolution]"

**Reddit Language Patterns:**
- Use "Edit:" for corrections/updates
- Include "TL;DR:" for long posts
- Reference other Redditors: "Like u/someone said..."
- Use Reddit slang naturally: "This", "NTA", "Red flag"
- Acknowledge Reddit community: "You guys were right about..."

**Emotional Authenticity:**
- **Frustration:** "I'm so done with this", "absolutely ridiculous", "losing my mind"
- **Confusion:** "Am I missing something?", "Maybe I'm wrong but...", "Help me understand"
- **Gratitude:** "Honestly impressed", "they really came through", "restored my faith"
- **Anxiety:** "Getting worried about", "not sure what to do", "this is stressing me out"

**TOPIC-SPECIFIC CONTENT GENERATION:**

**For POSITIVE Experiences:**
- Share specific helpful actions bank took
- Contrast with previous bad experiences
- Recommend to other Redditors
- Provide detailed positive outcomes
- Thank specific staff or departments

**For NEGATIVE Experiences:**  
- Detail the problem and its impact
- Express genuine frustration/concern
- Seek advice from community
- Warn others about similar issues
- Request suggestions for alternatives

**For NEUTRAL/QUESTION Posts:**
- Ask for information about banking processes
- Seek recommendations for services
- Compare options between banks
- Request clarification on policies
- Share factual updates about changes

**ENGAGEMENT METRICS LOGIC:**
- **High engagement:** Relatable problems, controversial bank policies, success stories, detailed helpful advice
- **Medium engagement:** Common issues, specific questions, personal experiences
- **Low engagement:** Niche problems, simple questions, routine updates

**OUTPUT FORMAT:**
```json
{{
  "priority": "{"P1 - Critical" if any(word in dominant_topic for word in ["Failed", "Error", "Blocked", "Problem"]) else "P2 - Medium" if any(word in dominant_topic for word in ["Delayed", "Issues", "Slow"]) else "P3 - Low"}",
  "like_count": {5 + (topic_hash % 45)},
  "share_count": {0 + (length_hash % 15)},
  "comment_count": {1 + (user_hash % 29)},
  "sentiment": "{"Positive" if any(word in dominant_topic for word in ["Outstanding", "Excellent", "Great", "Quick", "Resolved"]) else "Negative" if any(word in dominant_topic for word in ["Failed", "Delayed", "Problem", "Error", "Issues", "Blocked"]) else "Neutral"}",
  "text": "Generate Reddit post that clearly reflects the dominant topic experience using assigned customer type and style - natural Reddit post length",
  "urgency": {str(any(word in dominant_topic for word in ["Failed", "Blocked", "Error", "Critical"])).lower()}
}}
```

**CRITICAL SUCCESS CRITERIA:**
1. Reddit post content must obviously relate to the dominant topic
2. Reader should understand the banking experience from the post
3. Sentiment must match the dominant topic (positive/negative/neutral)
4. Subtopics must be referenced or implied in the post content
5. Customer type voice must be authentic and consistent
6. Post must be completely unique in structure and phrasing
7. Banking context must be realistic and specific to EU banking
8. NEVER mention specific bank names - use generic references like "my bank", "they", "them"
9. Use emojis sparingly and only when they add genuine value - avoid overuse

**GENERATION INSTRUCTION:**
Read the dominant topic "{dominant_topic}" and subtopics "{subtopics_str}". Understand what banking experience this represents. Generate an authentic Reddit post from the assigned customer type that clearly communicates this specific experience without mentioning any specific bank name.

Topic Analysis: {dominant_topic} - {subtopics_str}
""".strip()

        response = call_ollama_with_backoff(reddit_prompt)
        
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

def update_reddit_posts_with_content():
    """Update Reddit social media records with generated content"""
    
    logger.info("Starting Reddit Social Media Content Generation...")
    logger.info(f"Target Channel: Reddit")
    logger.info(f"Bank: European Banking")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    
    # Test Ollama connection
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
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
            
            # Process batch with reduced parallelization
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
    logger.info(f"Bank: European Banking")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_BASE_URL}")
    
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