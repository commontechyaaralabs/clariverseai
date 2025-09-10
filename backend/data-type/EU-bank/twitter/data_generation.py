# EU Banking Twitter Content Generator - Ollama Version
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
TWITTER_COLLECTION = "twitter"

# Ollama setup - Updated to use Cloudflare Tunnel endpoint
OLLAMA_BASE_URL = "https://metallic-heel-about-prizes.trycloudflare.com/"
OLLAMA_TOKEN = "eac6f9ff2fd50c497cc54348ccf3961bb4022eed77c001012b2aeba0dfc7d76e"
OLLAMA_URL = f"{OLLAMA_BASE_URL}/api/generate"
OLLAMA_MODEL = "gemma3:27b"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log files
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
MAIN_LOG_FILE = LOG_DIR / f"twitter_generator_{timestamp}.log"
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

# Add Windows-specific console handler configuration
if sys.platform == "win32":
    import codecs
    # Configure stdout to use UTF-8 encoding
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8')
    # Also try to set console code page to UTF-8
    try:
        import subprocess
        subprocess.run(['chcp', '65001'], shell=True, capture_output=True)
    except:
        pass

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
twitter_col = None

# Optimized batch processing configuration
BATCH_SIZE = 5  # Reduced from 10 to respect rate limits
CPU_COUNT = multiprocessing.cpu_count()
MAX_WORKERS = min(CPU_COUNT, 8)  # Reduced workers to avoid overwhelming API
REQUEST_TIMEOUT = 120
MAX_RETRIES = 3
RETRY_DELAY = 1

# Rate limiting configuration for Ollama
RATE_LIMIT_PER_MINUTE = 30  # Ollama can handle more requests
RATE_LIMIT_DELAY = 60 / RATE_LIMIT_PER_MINUTE  # ~2 seconds between requests
RATE_LIMIT_WINDOW = 60  # 1 minute window

# Configuration options
ENABLE_RATE_LIMITING = True  # Set to False to disable rate limiting
RETRY_RATE_LIMITED = True    # Set to False to skip retrying rate-limited requests
MAX_RETRY_ATTEMPTS = 3       # Maximum retry attempts for rate-limited requests

class RateLimiter:
    """Rate limiter for Ollama API calls"""
    
    def __init__(self, max_requests_per_minute, enabled=True):
        self.max_requests = max_requests_per_minute
        self.enabled = enabled
        self.requests = []
        self.lock = threading.Lock()
        
        if self.enabled:
            logger.info(f"Rate limiter enabled: {self.max_requests} requests per minute")
            logger.info(f"Recommended delay between requests: {self.get_delay():.2f} seconds")
        else:
            logger.warning("Rate limiter disabled - this may cause API errors!")
    
    def wait_if_needed(self):
        """Wait if we need to respect rate limits"""
        if not self.enabled:
            return
            
        with self.lock:
            now = time.time()
            # Remove requests older than 1 minute
            self.requests = [req_time for req_time in self.requests if now - req_time < 60]
            
            if len(self.requests) >= self.max_requests:
                # Wait until we can make another request
                oldest_request = min(self.requests)
                wait_time = 60 - (now - oldest_request) + 1
                if wait_time > 0:
                    logger.info(f"Rate limit reached. Waiting {wait_time:.1f} seconds...")
                    time.sleep(wait_time)
                    now = time.time()
            
            # Record this request
            self.requests.append(now)
    
    def get_delay(self):
        """Get recommended delay between requests"""
        return 60 / self.max_requests if self.enabled else 0
    
    def get_current_usage(self):
        """Get current rate limit usage"""
        with self.lock:
            now = time.time()
            recent_requests = [req_time for req_time in self.requests if now - req_time < 60]
            return len(recent_requests), self.max_requests

# Initialize rate limiter
rate_limiter = RateLimiter(RATE_LIMIT_PER_MINUTE, ENABLE_RATE_LIMITING)

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
    
    def mark_as_saved(self, tweet_ids):
        """Mark results as saved to database"""
        with self._lock:
            for result in self.results:
                if result.get('tweet_id') in tweet_ids:
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
    global client, db, twitter_col
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        db = client[DB_NAME]
        twitter_col = db[TWITTER_COLLECTION]
        
        # Create indexes for better performance (only if they don't exist)
        try:
            # Check existing indexes
            existing_indexes = list(twitter_col.list_indexes())
            index_names = [idx['name'] for idx in existing_indexes]
            
            # Create indexes only if they don't exist
            if "tweet_id_1" not in index_names:
                twitter_col.create_index("tweet_id", unique=True)
                logger.info("Created unique index on tweet_id")
            else:
                logger.info("Unique index on tweet_id already exists")
                
            if "user_id_1" not in index_names:
                twitter_col.create_index("user_id")
                logger.info("Created index on user_id")
            else:
                logger.info("Index on user_id already exists")
                
            if "username_1" not in index_names:
                twitter_col.create_index("username")
                logger.info("Created index on username")
            else:
                logger.info("Index on username already exists")
                
            if "dominant_topic_1" not in index_names:
                twitter_col.create_index("dominant_topic")
                logger.info("Created index on dominant_topic")
            else:
                logger.info("Index on dominant_topic already exists")
                
        except Exception as index_error:
            logger.warning(f"Index creation warning (continuing): {index_error}")
        
        logger.info("Database connection established and indexes verified")
        return True
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return False

def generate_realistic_tweet_metrics():
    """Generate realistic Twitter engagement metrics"""
    # Base engagement ranges based on typical EU banking social media performance
    engagement_scenarios = [
        # High engagement (viral/controversial content)
        {"retweet_range": (50, 500), "like_range": (200, 2000), "reply_range": (10, 100), "quote_range": (5, 50), "weight": 0.1},
        # Medium-high engagement (popular content)
        {"retweet_range": (20, 80), "like_range": (100, 400), "reply_range": (5, 30), "quote_range": (2, 15), "weight": 0.2},
        # Medium engagement (standard content)
        {"retweet_range": (5, 25), "like_range": (30, 120), "reply_range": (2, 15), "quote_range": (1, 8), "weight": 0.4},
        # Low-medium engagement
        {"retweet_range": (1, 10), "like_range": (10, 50), "reply_range": (0, 8), "quote_range": (0, 4), "weight": 0.2},
        # Low engagement (niche content)
        {"retweet_range": (0, 5), "like_range": (2, 20), "reply_range": (0, 3), "quote_range": (0, 2), "weight": 0.1}
    ]
    
    # Select engagement scenario based on weight
    scenario = random.choices(engagement_scenarios, weights=[s["weight"] for s in engagement_scenarios])[0]
    
    metrics = {
        'retweet_count': random.randint(scenario["retweet_range"][0], scenario["retweet_range"][1]),
        'like_count': random.randint(scenario["like_range"][0], scenario["like_range"][1]),
        'reply_count': random.randint(scenario["reply_range"][0], scenario["reply_range"][1]),
        'quote_count': random.randint(scenario["quote_range"][0], scenario["quote_range"][1])
    }
    
    return metrics

def generate_relevant_hashtags(dominant_topic, subtopics):
    """Generate relevant hashtags based on topic and subtopics"""
    
    # Banking topic hashtag mapping
    topic_hashtags = {
        "Risk Management": ["#RiskManagement", "#BankingRisk", "#FinancialRisk", "#RiskAssessment", "#EURisk"],
        "Compliance": ["#Compliance", "#BankingCompliance", "#FinancialCompliance", "#EUCompliance", "#RegTech"],
        "Internal Audit": ["#InternalAudit", "#BankingAudit", "#FinancialAudit", "#AuditCompliance", "#EUAudit"],
        "Credit Risk": ["#CreditRisk", "#LendingRisk", "#BankingCredit", "#FinancialCredit", "#EUCredit"],
        "Operational Risk": ["#OperationalRisk", "#BankingOps", "#FinancialOps", "#OpRisk", "#EUBanking"],
        "Market Risk": ["#MarketRisk", "#TradingRisk", "#FinancialMarkets", "#EUMarkets", "#BankingMarkets"],
        "Cybersecurity": ["#Cybersecurity", "#BankingSecurity", "#FinTechSecurity", "#EUCyber", "#DigitalBanking"],
        "Digital Transformation": ["#DigitalBanking", "#FinTech", "#BankingInnovation", "#EUDigital", "#TechTransformation"],
        "Customer Service": ["#CustomerService", "#BankingService", "#FinancialService", "#CustomerExperience", "#EUCustomers"],
        "Regulatory Reporting": ["#RegReporting", "#BankingReporting", "#FinancialReporting", "#EURegulation", "#ComplianceReporting"]
    }
    
    # General banking hashtags
    general_banking = ["#Banking", "#Finance", "#EUBanking", "#FinancialServices", "#EuropeBank", "#BankingNews", "#FinanceNews"]
    
    # EU specific hashtags
    eu_specific = ["#EuropeanBanking", "#EUFinance", "#EBA", "#ECB", "#EURegulation", "#SingleMarket", "#BankingUnion"]
    
    # Get topic-specific hashtags
    topic_specific = topic_hashtags.get(dominant_topic, [])
    
    # Create hashtag pool
    hashtag_pool = topic_specific + general_banking + eu_specific
    
    # Remove duplicates while preserving order
    seen = set()
    unique_hashtags = []
    for tag in hashtag_pool:
        if tag not in seen:
            seen.add(tag)
            unique_hashtags.append(tag)
    
    # Select 2-4 hashtags randomly
    num_hashtags = random.randint(2, min(4, len(unique_hashtags)))
    selected_hashtags = random.sample(unique_hashtags, num_hashtags)
    
    # Always include at least one EU banking hashtag if not present
    eu_banking_tags = ["#EUBanking", "#Banking", "#EuropeanBanking"]
    has_banking_tag = any(tag in selected_hashtags for tag in eu_banking_tags)
    if not has_banking_tag and len(selected_hashtags) < 4:
        selected_hashtags.append(random.choice(eu_banking_tags))
    
    return selected_hashtags

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
    
    try:
        # Apply rate limiting before making the request
        rate_limiter.wait_if_needed()
        
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
                "temperature": 0.8,
                "num_predict": 1200,
                "top_k": 40,
                "top_p": 0.9,
                "num_ctx": 4096
            }
        }
        
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

def generate_eu_banking_tweet_content(tweet_data):
    """Generate EU banking tweet content based on dominant topic and subtopics"""
    if shutdown_flag.is_set():
        return None
    
    start_time = time.time()
    tweet_id = tweet_data.get('tweet_id', 'unknown')
    
    try:
        # Extract data from tweet record
        username = tweet_data.get('username', 'Unknown User')
        dominant_topic = tweet_data.get('dominant_topic', 'General Banking')
        subtopics = tweet_data.get('subtopics', 'General operations')
        
        # Enhanced prompt for realistic banking Twitter content
        prompt = f"""
Generate a realistic banking-related tweet based on the following context:

*Tweet Context:*
- Username: {username}
- Dominant Topic: {dominant_topic}
- Subtopics: {subtopics}

*Tweet Generation Requirements:*
1. Create tweet text (180-280 characters) that:
   - Relates directly to the dominant topic: {dominant_topic}
   - Incorporates aspects of the subtopics: {subtopics}
   - Uses professional but engaging tone appropriate for banking sector
   - Can be either POSITIVE or NEGATIVE sentiment (mix both types)
   - May reference current banking trends, regulations, or challenges
   - Should feel authentic and realistic for a banking professional or customer

*Content Variety Guidelines:*
- POSITIVE tweets can discuss: innovations, achievements, customer satisfaction, regulatory improvements, market growth, digital transformation success, fintech partnerships, new services
- NEGATIVE tweets can discuss: concerns about regulations, service issues, compliance burdens, economic uncertainties, customer complaints, system outages, security breaches
- Use a mix of perspectives: banking professionals, customers, analysts, regulators, fintech companies, financial advisors

*Hashtag Requirements:*
- Generate 3-5 relevant hashtags based on the topic and banking context
- Focus on banking industry tags like: #Banking, #Finance, #FinTech, #DigitalBanking, #FinancialServices, #CreditRisk, #Compliance, #BankingNews, #FinanceNews
- Topic-specific tags based on dominant_topic: 
  - Risk Management: #RiskManagement, #BankingRisk, #FinancialRisk
  - Compliance: #Compliance, #RegTech, #BankingCompliance
  - Digital Transformation: #FinTech, #DigitalBanking, #BankingInnovation
  - Customer Service: #CustomerService, #BankingService, #CustomerExperience
  - Cybersecurity: #Cybersecurity, #BankingSecurity, #FinTechSecurity
- Avoid overusing location-based hashtags - keep it general banking focused

*Banking Context Examples:*
- Regulatory compliance, digital transformation, customer experience improvements
- Interest rate changes, lending practices, mobile banking features
- Cybersecurity measures, fraud prevention, data protection
- Sustainable finance, ESG requirements, green banking initiatives
- Open banking, payment innovations, cryptocurrency discussions
- Economic trends impact, inflation concerns, market volatility
- Branch closures, digital-first approaches, customer support

*Engagement Metrics Guidelines:*
Generate realistic engagement numbers based on content type:
- POSITIVE/Promotional content: Higher likes (50-300), moderate retweets (10-50)
- NEGATIVE/Complaint content: Higher replies (15-80), moderate likes (20-150)
- NEWS/Updates: Balanced engagement across all metrics
- TECHNICAL/Regulatory: Lower overall engagement but higher quote tweets (5-25)

*Classification Guidelines:*
- Sentiment: "Positive" (supportive, promotional, success stories), "Negative" (complaints, concerns, criticism), "Neutral" (factual updates, news)
- Urgency: true (urgent alerts, breaking news, system issues, security breaches, critical announcements), false (general updates, non-time-sensitive content, regular news, promotional content)
- Priority: "P3 - Low" (general social content), "P2 - Medium" (important announcements), "P1 - Critical" (urgent alerts, major incidents)

*Output Format:*
Return ONLY a JSON object with these exact fields (ALL must be generated by you):
{{
  "text": "Tweet content here with hashtags integrated naturally",
  "hashtags": ["hashtag1", "hashtag2", "hashtag3"],
  "retweet_count": [generate realistic number based on content type],
  "like_count": [generate realistic number based on content type],
  "reply_count": [generate realistic number based on content type],
  "quote_count": [generate realistic number based on content type],
  "sentiment": "Positive/Negative/Neutral",
  "urgency": true/false, 
  "priority": "P3 - Low/P2 - Medium/P1 - Critical"
}}

*Critical Instructions:*
- ALL fields including hashtags must be generated by you - no placeholders
- Generate 3-5 relevant hashtags focused on banking topics, not geographical regions
- Tweet text must be realistic and authentic banking industry content
- Hashtags should be naturally integrated or at the end
- Engagement metrics should be realistic based on tweet content and sentiment
- Sentiment must accurately match the tone of the tweet content
- Make it sound like real banking sector social media content from professionals, customers, or industry observers
- Focus on banking topics, services, challenges, and innovations rather than regional aspects
- Ensure variety in sentiment (mix positive and negative tweets)
- Generate appropriate engagement numbers based on content quality and type

Generate the complete tweet content now with ALL required fields:
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
            required_fields = ['text', 'hashtags', 'sentiment', 'urgency', 'priority']
            for field in required_fields:
                if field not in result:
                    raise ValueError(f"Missing required field: {field}")
            
            # Validate and fix data types
            if not isinstance(result['hashtags'], list):
                result['hashtags'] = ["#Banking", "#Finance", "#FinancialServices"]  # fallback hashtags
            
            # Ensure 3-5 hashtags - focus on banking, not geography
            banking_hashtags = ["#Banking", "#Finance", "#FinancialServices", "#FinTech", "#BankingNews", 
                              "#DigitalBanking", "#FinanceNews", "#BankingInnovation", "#CustomerService", 
                              "#RiskManagement", "#Compliance", "#Cybersecurity"]
            
            if len(result['hashtags']) < 3:
                # Add relevant banking hashtags to reach minimum
                for tag in banking_hashtags:
                    if tag not in result['hashtags'] and len(result['hashtags']) < 5:
                        result['hashtags'].append(tag)
            elif len(result['hashtags']) > 5:
                # Limit to 5 hashtags
                result['hashtags'] = result['hashtags'][:5]
            
            # Ensure metrics are integers and realistic
            result['retweet_count'] = max(0, int(result.get('retweet_count', 0)))
            result['like_count'] = max(0, int(result.get('like_count', 0)))
            result['reply_count'] = max(0, int(result.get('reply_count', 0)))
            result['quote_count'] = max(0, int(result.get('quote_count', 0)))
            
            # Validate realistic engagement ranges (sanity check)
            if result['like_count'] > 10000:
                result['like_count'] = random.randint(100, 1000)
            if result['retweet_count'] > 2000:
                result['retweet_count'] = random.randint(10, 200)
            if result['reply_count'] > 500:
                result['reply_count'] = random.randint(5, 100)
            if result['quote_count'] > 200:
                result['quote_count'] = random.randint(1, 50)
            
            # Validate tweet length
            if len(result['text']) > 280:
                result['text'] = result['text'][:277] + "..."
            
            # Validate sentiment values
            valid_sentiments = ['Positive', 'Negative', 'Neutral']
            if result['sentiment'] not in valid_sentiments:
                result['sentiment'] = 'Neutral'
            
            # Validate urgency values
            valid_urgency = [True, False]
            if result['urgency'] not in valid_urgency:
                result['urgency'] = False
            
            # Validate priority values
            valid_priority = ['P3 - Low', 'P2 - Medium', 'P1 - Critical']
            if result['priority'] not in valid_priority:
                result['priority'] = 'P3 - Low'
            
            generation_time = time.time() - start_time
            
            # Log successful generation
            success_info = {
                'tweet_id': tweet_id,
                'username': username,
                'dominant_topic': dominant_topic,
                'text_preview': result['text'][:50] + '...',
                'sentiment': result['sentiment'],
                'generation_time': generation_time
            }
            success_logger.info(json.dumps(success_info))
            
            return result
            
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON parsing failed: {e}")
        
    except Exception as e:
        generation_time = time.time() - start_time
        error_info = {
            'tweet_id': tweet_id,
            'username': tweet_data.get('username', 'Unknown'),
            'dominant_topic': tweet_data.get('dominant_topic', 'Unknown'),
            'error': str(e),
            'generation_time': generation_time
        }
        failure_logger.error(json.dumps(error_info))
        raise

def process_single_tweet_update(tweet_record):
    """Process a single tweet record to generate content"""
    if shutdown_flag.is_set():
        return None
        
    try:
        # Generate tweet content based on existing data
        tweet_content = generate_eu_banking_tweet_content(tweet_record)
        
        if not tweet_content:
            failure_counter.increment()
            return None
        
        # Prepare update document with all new fields
        update_doc = {
            "text": tweet_content['text'],
            "hashtags": tweet_content['hashtags'],
            "retweet_count": tweet_content['retweet_count'],
            "like_count": tweet_content['like_count'],
            "reply_count": tweet_content['reply_count'],
            "quote_count": tweet_content['quote_count'],
            "sentiment": tweet_content['sentiment'],
            "urgency": tweet_content['urgency'],
            "priority": tweet_content['priority']
        }
        
        success_counter.increment()
        
        # Create intermediate result
        intermediate_result = {
            'tweet_id': tweet_record['tweet_id'],
            'update_doc': update_doc,
            'original_data': {
                'username': tweet_record.get('username'),
                'dominant_topic': tweet_record.get('dominant_topic'),
                'subtopics': tweet_record.get('subtopics', '')[:100] + '...' if len(str(tweet_record.get('subtopics', ''))) > 100 else tweet_record.get('subtopics', '')
            }
        }
        
        # Add to intermediate results
        results_manager.add_result(intermediate_result)
        
        return {
            'tweet_id': tweet_record['tweet_id'],
            'update_doc': update_doc
        }
        
    except Exception as e:
        error_msg = str(e)
        if "rate limit" in error_msg.lower() or "429" in error_msg:
            logger.warning(f"Rate limit error for {tweet_record.get('tweet_id', 'unknown')}: {error_msg[:100]}")
            # Don't increment failure counter for rate limits - these will be retried
        else:
            logger.error(f"Task processing error for {tweet_record.get('tweet_id', 'unknown')}: {error_msg[:100]}")
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
        tweet_ids = []
        
        for update_data in batch_updates:
            # Create UpdateOne operation properly
            operation = UpdateOne(
                filter={"tweet_id": update_data['tweet_id']},
                update={"$set": update_data['update_doc']}
            )
            bulk_operations.append(operation)
            tweet_ids.append(update_data['tweet_id'])
        
        # Execute bulk write with proper error handling
        if bulk_operations:
            try:
                result = twitter_col.bulk_write(bulk_operations, ordered=False)
                updated_count = result.modified_count
                
                # Mark intermediate results as saved
                results_manager.mark_as_saved(tweet_ids)
                
                # Update counter
                update_counter._value += updated_count
                
                logger.info(f"Successfully saved {updated_count} records to database")
                progress_logger.info(f"DATABASE_SAVE: {updated_count} records saved, total_updates: {update_counter.value}")
                
                # Log some details of what was saved
                if updated_count > 0:
                    sample_update = batch_updates[0]
                    logger.info(f"Sample update - ID: {sample_update['tweet_id']}, Text: {sample_update['update_doc']['text'][:50]}...")
                
                return updated_count
                
            except Exception as db_error:
                logger.error(f"Bulk write operation failed: {db_error}")
                
                # Try individual updates as fallback
                logger.info("Attempting individual updates as fallback...")
                individual_success = 0
                
                for update_data in batch_updates:
                    try:
                        result = twitter_col.update_one(
                            {"tweet_id": update_data['tweet_id']},
                            {"$set": update_data['update_doc']}
                        )
                        if result.modified_count > 0:
                            individual_success += 1
                    except Exception as individual_error:
                        logger.error(f"Individual update failed for {update_data['tweet_id']}: {individual_error}")
                
                if individual_success > 0:
                    results_manager.mark_as_saved([up['tweet_id'] for up in batch_updates[:individual_success]])
                    update_counter._value += individual_success
                    logger.info(f"Fallback: {individual_success} records saved individually")
                
                return individual_success
        
        return 0
        
    except Exception as e:
        logger.error(f"Database save error: {e}")
        return 0

def retry_rate_limited_requests(failed_records, max_retries=3):
    """Retry requests that failed due to rate limits"""
    if not failed_records:
        return []
    
    logger.info(f"Retrying {len(failed_records)} rate-limited requests...")
    retry_success = []
    
    for attempt in range(max_retries):
        if not failed_records:
            break
            
        logger.info(f"Retry attempt {attempt + 1}/{max_retries} for {len(failed_records)} records")
        
        # Wait for rate limit window to reset
        if attempt > 0:
            wait_time = 60 + (attempt * 30)  # Progressive backoff
            logger.info(f"Waiting {wait_time} seconds before retry...")
            time.sleep(wait_time)
        
        # Process failed records with proper rate limiting
        current_batch = failed_records[:]
        failed_records = []
        
        for record in current_batch:
            try:
                # Apply rate limiting
                rate_limiter.wait_if_needed()
                
                result = process_single_tweet_update(record)
                if result:
                    retry_success.append(result)
                else:
                    failed_records.append(record)
                    
            except Exception as e:
                if "rate limit" in str(e).lower() or "429" in str(e):
                    failed_records.append(record)
                else:
                    logger.error(f"Unexpected error retrying {record.get('tweet_id', 'unknown')}: {e}")
        
        if failed_records:
            logger.info(f"Retry attempt {attempt + 1}: {len(retry_success)} succeeded, {len(failed_records)} still failed")
        else:
            logger.info(f"All rate-limited requests succeeded on attempt {attempt + 1}")
            break
    
    if failed_records:
        logger.warning(f"Failed to retry {len(failed_records)} requests after {max_retries} attempts")
    
    return retry_success

def show_rate_limit_status():
    """Show current rate limit status"""
    if ENABLE_RATE_LIMITING:
        current_usage, max_usage = rate_limiter.get_current_usage()
        remaining = max_usage - current_usage
        logger.info(f"Rate Limit Status: {current_usage}/{max_usage} requests used this minute ({remaining} remaining)")
        return current_usage, max_usage
    else:
        logger.info("Rate limiting disabled")
        return 0, 0

def update_tweets_with_content_parallel():
    """Update existing tweets with generated content using optimized batch processing"""
    
    logger.info("Starting EU Banking Twitter Content Generation...")
    logger.info(f"System Info: {CPU_COUNT} CPU cores detected")
    logger.info(f"Batch size: {BATCH_SIZE}")
    logger.info(f"Max workers: {MAX_WORKERS}")
    logger.info(f"Request timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"Max retries per request: {MAX_RETRIES}")
    
    # Ollama connection already tested in main function
    
    # Get all tweet records that need content generation
    logger.info("Fetching tweet records from database...")
    try:
        # Query for tweets that don't have the new generated fields
        query = {
            "$or": [
                {"text": {"$exists": False}},
                {"hashtags": {"$exists": False}},
                {"sentiment": {"$exists": False}},
                {"text": {"$in": [None, ""]}},
                {"hashtags": {"$in": [None, []]}},
                {"sentiment": {"$in": [None, ""]}}
            ]
        }
        
        tweet_records = list(twitter_col.find(query))
        total_tweets = len(tweet_records)
        
        if total_tweets == 0:
            logger.info("All tweets already have generated content!")
            return
            
        logger.info(f"Found {total_tweets} tweets needing content generation")
        progress_logger.info(f"BATCH_START: total_tweets={total_tweets}, batch_size={BATCH_SIZE}")
        
    except Exception as e:
        logger.error(f"Error fetching tweet records: {e}")
        return
    
    # Process in batches of BATCH_SIZE (5)
    total_batches = (total_tweets + BATCH_SIZE - 1) // BATCH_SIZE
    total_updated = 0
    batch_updates = []  # Accumulate updates for batch saving
    rate_limited_failures = []  # Track rate-limited failures for retry
    
    logger.info(f"Processing in {total_batches} batches of {BATCH_SIZE} tweets each")
    logger.info(f"Rate limit: {RATE_LIMIT_PER_MINUTE} requests per minute")
    logger.info(f"Recommended delay between requests: {rate_limiter.get_delay():.2f} seconds")
    
    try:
        for batch_num in range(1, total_batches + 1):
            if shutdown_flag.is_set():
                logger.info(f"Shutdown requested. Stopping at batch {batch_num-1}/{total_batches}")
                break
                
            batch_start = (batch_num - 1) * BATCH_SIZE
            batch_end = min(batch_start + BATCH_SIZE, total_tweets)
            batch_records = tweet_records[batch_start:batch_end]
            
            logger.info(f"Processing batch {batch_num}/{total_batches} ({len(batch_records)} tweets)...")
            progress_logger.info(f"BATCH_START: batch={batch_num}/{total_batches}, records={len(batch_records)}")
            
            # Process batch with parallelization
            successful_updates = []
            batch_start_time = time.time()
            
            # Use ThreadPoolExecutor for I/O bound operations (API calls)
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Submit all tasks for this batch
                futures = {
                    executor.submit(process_single_tweet_update, record): record 
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
                            else:
                                # Check if it was a rate limit failure
                                original_record = futures[future]
                                rate_limited_failures.append(original_record)
                            
                            # Progress indicator
                            if completed % 2 == 0:  # More frequent updates for smaller batches
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
            
            # Save to database every 5 records (one batch)
            if len(batch_updates) >= BATCH_SIZE and not shutdown_flag.is_set():
                saved_count = save_batch_to_database(batch_updates)
                total_updated += saved_count
                batch_updates = []  # Clear the accumulator
                
                logger.info(f"Database update complete: {saved_count} records saved")
            
            # Progress summary every 5 batches
            if batch_num % 5 == 0 or batch_num == total_batches:
                overall_progress = ((batch_num * BATCH_SIZE) / total_tweets) * 100
                logger.info(f"Overall Progress: {overall_progress:.1f}% | Batches: {batch_num}/{total_batches}")
                logger.info(f"Success: {success_counter.value} | Failures: {failure_counter.value} | DB Updates: {total_updated}")
                logger.info(f"Rate-limited failures: {len(rate_limited_failures)}")
                
                # Show rate limit status
                show_rate_limit_status()
                
                # Calculate estimated completion time based on rate limits
                remaining_batches = total_batches - batch_num
                if remaining_batches > 0:
                    # Each batch takes approximately BATCH_SIZE * rate_limit_delay seconds
                    estimated_seconds = remaining_batches * BATCH_SIZE * rate_limiter.get_delay()
                    estimated_minutes = estimated_seconds / 60
                    logger.info(f"Estimated time remaining: {estimated_minutes:.1f} minutes")
                
                # System resource info
                cpu_percent = psutil.cpu_percent()
                memory_percent = psutil.virtual_memory().percent
                logger.info(f"System: CPU {cpu_percent:.1f}% | Memory {memory_percent:.1f}%")
                progress_logger.info(f"PROGRESS_SUMMARY: batch={batch_num}/{total_batches}, success={success_counter.value}, failures={failure_counter.value}, db_updates={total_updated}")
            
            # Brief pause between batches to respect rate limits
            if not shutdown_flag.is_set() and batch_num < total_batches:
                time.sleep(rate_limiter.get_delay())
        
        # Save any remaining updates
        if batch_updates and not shutdown_flag.is_set():
            saved_count = save_batch_to_database(batch_updates)
            total_updated += saved_count
            logger.info(f"Final batch saved: {saved_count} records")
        
        # Retry rate-limited failures
        if rate_limited_failures and not shutdown_flag.is_set():
            logger.info(f"Attempting to retry {len(rate_limited_failures)} rate-limited requests...")
            retry_success = retry_rate_limited_requests(rate_limited_failures)
            if retry_success:
                # Save retried successful updates
                saved_count = save_batch_to_database(retry_success)
                total_updated += saved_count
                logger.info(f"Retry successful: {saved_count} additional records saved")
        
        if shutdown_flag.is_set():
            logger.info("Content generation interrupted gracefully!")
        else:
            logger.info("EU Banking Twitter content generation complete!")
            
        logger.info(f"Total tweets updated: {total_updated}")
        logger.info(f"Successful generations: {success_counter.value}")
        logger.info(f"Failed generations: {failure_counter.value}")
        logger.info(f"Rate-limited failures: {len(rate_limited_failures)}")
        logger.info(f"Data updated in MongoDB: {DB_NAME}.{TWITTER_COLLECTION}")
        
        # Final progress log
        progress_logger.info(f"FINAL_SUMMARY: total_updated={total_updated}, success={success_counter.value}, failures={failure_counter.value}")
        
    except KeyboardInterrupt:
        logger.info("Generation interrupted by user!")
        shutdown_flag.set()
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        shutdown_flag.set()

def test_ollama_connection():
    """Test if Ollama is accessible and model is available"""
    try:
        logger.info(f"Testing connection to Ollama: {OLLAMA_BASE_URL}")
        logger.info(f"Testing model: {OLLAMA_MODEL}")
        
        # Test basic connection with simple generation
        logger.info("Testing simple generation...")
        
        # Simple test prompt
        test_prompt = 'Generate a JSON object: {"test": "success"}'
        
        response = call_ollama_with_backoff(test_prompt, timeout=30)
        
        if response and "success" in response.lower():
            logger.info("Ollama connection test successful")
            logger.info(f"Test response: {response[:100]}...")
            return True
        else:
            logger.error("Ollama connection test failed - unexpected response")
            return False
            
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        return False

def get_collection_stats():
    """Get collection statistics"""
    try:
        total_count = twitter_col.count_documents({})
        
        # Count records with and without generated content
        with_content = twitter_col.count_documents({
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "hashtags": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        })
        
        without_content = total_count - with_content
        
        # Get sample dominant topics
        pipeline = [
            {"$group": {"_id": "$dominant_topic", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}},
            {"$limit": 10}
        ]
        
        top_topics = list(twitter_col.aggregate(pipeline))
        
        logger.info("Collection Statistics:")
        logger.info(f"Total tweets: {total_count}")
        logger.info(f"With generated content: {with_content}")
        logger.info(f"Without generated content: {without_content}")
        
        logger.info("Top Dominant Topics:")
        for i, topic in enumerate(top_topics, 1):
            logger.info(f"{i}. {topic['_id']}: {topic['count']} tweets")
            
        progress_logger.info(f"COLLECTION_STATS: total={total_count}, with_content={with_content}, without_content={without_content}")
            
    except Exception as e:
        logger.error(f"Error getting collection stats: {e}")

def get_sample_generated_tweets(limit=3):
    """Get sample tweets with generated content"""
    try:
        samples = list(twitter_col.find({
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "hashtags": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(limit))
        
        logger.info("Sample Generated Tweet Content:")
        for i, tweet in enumerate(samples, 1):
            logger.info(f"--- Sample Tweet {i} ---")
            logger.info(f"Tweet ID: {tweet.get('tweet_id', 'N/A')}")
            logger.info(f"Username: {tweet.get('username', 'N/A')}")
            logger.info(f"Dominant Topic: {tweet.get('dominant_topic', 'N/A')}")
            logger.info(f"Subtopics: {str(tweet.get('subtopics', 'N/A'))[:100]}...")
            logger.info(f"Text: {tweet.get('text', 'N/A')}")
            logger.info(f"Hashtags: {tweet.get('hashtags', [])}")
            logger.info(f"Sentiment: {tweet.get('sentiment', 'N/A')}")
            logger.info(f"Engagement: {tweet.get('like_count', 0)} likes, {tweet.get('retweet_count', 0)} retweets")
            logger.info(f"Priority: {tweet.get('priority', 'N/A')}")
            
    except Exception as e:
        logger.error(f"Error getting sample tweets: {e}")

def generate_status_report():
    """Generate comprehensive status report"""
    try:
        report_file = LOG_DIR / f"status_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        # Get intermediate results stats
        pending_results = results_manager.get_pending_updates()
        total_intermediate = len(results_manager.results)
        
        # Get database stats
        total_count = twitter_col.count_documents({})
        with_content = twitter_col.count_documents({
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "hashtags": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
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
                "total_tweets": total_count,
                "tweets_with_content": with_content,
                "tweets_without_content": total_count - with_content,
                "completion_percentage": (with_content / total_count) * 100 if total_count > 0 else 0
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
            if 'tweet_id' in result and 'update_doc' in result:
                batch_updates.append({
                    'tweet_id': result['tweet_id'],
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
        # Get recent generated tweets
        recent_tweets = list(twitter_col.find({
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "hashtags": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(20))
        
        validation_results = {
            "total_validated": len(recent_tweets),
            "validation_issues": [],
            "quality_metrics": {
                "avg_text_length": 0,
                "avg_hashtag_count": 0,
                "sentiment_distribution": {"Positive": 0, "Negative": 0, "Neutral": 0},
                "avg_engagement": {"likes": 0, "retweets": 0, "replies": 0, "quotes": 0}
            }
        }
        
        text_lengths = []
        hashtag_counts = []
        sentiment_counts = {"Positive": 0, "Negative": 0, "Neutral": 0}
        total_likes = 0
        total_retweets = 0
        total_replies = 0
        total_quotes = 0
        
        for tweet in recent_tweets:
            text = tweet.get('text', '')
            hashtags = tweet.get('hashtags', [])
            sentiment = tweet.get('sentiment', 'Neutral')
            
            # Validate tweet length
            if len(text) > 280:
                validation_results["validation_issues"].append({
                    "tweet_id": tweet.get('tweet_id'),
                    "issue": f"Tweet length {len(text)} exceeds 280 characters"
                })
            
            # Validate hashtags
            if not hashtags or len(hashtags) < 3:
                validation_results["validation_issues"].append({
                    "tweet_id": tweet.get('tweet_id'),
                    "issue": f"Insufficient hashtags: {len(hashtags)} (minimum 3 required)"
                })
            elif len(hashtags) > 5:
                validation_results["validation_issues"].append({
                    "tweet_id": tweet.get('tweet_id'),
                    "issue": f"Too many hashtags: {len(hashtags)} (maximum 5 allowed)"
                })
            
            text_lengths.append(len(text))
            hashtag_counts.append(len(hashtags))
            
            if sentiment in sentiment_counts:
                sentiment_counts[sentiment] += 1
            
            total_likes += tweet.get('like_count', 0)
            total_retweets += tweet.get('retweet_count', 0)
            total_replies += tweet.get('reply_count', 0)
            total_quotes += tweet.get('quote_count', 0)
        
        # Calculate metrics
        if text_lengths:
            validation_results["quality_metrics"]["avg_text_length"] = sum(text_lengths) / len(text_lengths)
        if hashtag_counts:
            validation_results["quality_metrics"]["avg_hashtag_count"] = sum(hashtag_counts) / len(hashtag_counts)
        
        validation_results["quality_metrics"]["sentiment_distribution"] = sentiment_counts
        
        if recent_tweets:
            validation_results["quality_metrics"]["avg_engagement"] = {
                "likes": total_likes / len(recent_tweets),
                "retweets": total_retweets / len(recent_tweets),
                "replies": total_replies / len(recent_tweets),
                "quotes": total_quotes / len(recent_tweets)
            }
        
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

def verify_results():
    """Verify the generated tweet content"""
    try:
        tweets_with_content = list(twitter_col.find({
            "text": {"$exists": True, "$ne": "", "$ne": None},
            "hashtags": {"$exists": True, "$ne": "", "$ne": None},
            "sentiment": {"$exists": True, "$ne": "", "$ne": None}
        }).limit(5))
        
        logger.info(f"Verification: Found {len(tweets_with_content)} tweets with generated content")
        
        for tweet in tweets_with_content:
            hashtag_count = len(tweet.get('hashtags', []))
            text_length = len(tweet.get('text', ''))
            logger.info(f"   tweet_id: {tweet['tweet_id']} | hashtags: {hashtag_count} | text_length: {text_length} | sentiment: {tweet.get('sentiment', 'N/A')}")
            
        return len(tweets_with_content)
        
    except Exception as e:
        logger.error(f"Error in verification: {e}")
        return 0

# Main execution function
def main():
    """Main function to initialize and run the twitter content generator"""
    logger.info("EU Banking Twitter Content Generator Starting...")
    logger.info(f"Database: {DB_NAME}")
    logger.info(f"Collection: {TWITTER_COLLECTION}")
    logger.info(f"Model: {OLLAMA_MODEL}")
    logger.info(f"Ollama URL: {OLLAMA_BASE_URL}")
    logger.info(f"Max Workers: {MAX_WORKERS}")
    logger.info(f"Batch Size: {BATCH_SIZE}")
    logger.info(f"Log Directory: {LOG_DIR}")
    
    # Setup signal handlers and cleanup
    setup_signal_handlers()
    atexit.register(cleanup_resources)
    
    # Clean up old logs
    cleanup_old_logs()
    
    # Test Ollama connection first
    logger.info("Testing Ollama connection...")
    if not test_ollama_connection():
        logger.error("Cannot proceed without Ollama connection")
        return
    logger.info("Ollama connection test successful!")
    
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
        
        # Run the twitter content generation
        update_tweets_with_content_parallel()
        
        # Show final statistics
        get_collection_stats()
        
        # Show sample generated content
        get_sample_generated_tweets()
        
        # Verify results
        verified_count = verify_results()
        logger.info(f"Verification complete: {verified_count} tweets verified")
        
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