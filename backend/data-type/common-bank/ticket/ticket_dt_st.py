# Import necessary libraries
import os
import json
import logging
import sys
import pandas as pd
import ollama
import re
import time
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from collections import Counter
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import threading
from queue import Queue

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
TICKET_COLLECTION = "tickets"

# Set up logging - modified for MongoDB processing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler('mongodb_ticket_topic_extraction.log', mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class MongoDBTopicExtractionProcessor:
    def __init__(self, model_name='gemma3:27b', base_url="http://34.147.17.26:31100", token="b805213e7b048d21f02dae5922973e9639ef971b0bc6bf804efad9c707527249", mongo_uri=None, db_name=DB_NAME, collection_name=TICKET_COLLECTION, max_workers=5):
        """
        Initialize the MongoDB topic extraction processor
        
        :param model_name: LLM model to use for classification
        :param base_url: Ollama API base URL
        :param token: Authentication token for remote Ollama
        :param mongo_uri: MongoDB connection string
        :param db_name: Database name
        :param collection_name: Collection name
        :param max_workers: Maximum number of concurrent workers for parallel processing
        """
        self.model_name = model_name
        self.base_url = base_url
        self.token = token
        self.ollama_url = f"{base_url}/api/generate"
        self.tags_url = f"{base_url}/api/tags"
        self.prompt_template = self._load_prompt_template()
        self.max_workers = max_workers
        
        # MongoDB setup
        self.mongo_uri = mongo_uri or MONGO_URI
        self.db_name = db_name
        self.collection_name = collection_name
        
        if not self.mongo_uri:
            raise ValueError("MongoDB connection string is required. Set MONGO_CONNECTION_STRING environment variable.")
        
        # Initialize MongoDB connection
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.db_name]
            self.collection = self.db[self.collection_name]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info(f"MongoDB connection established successfully to {self.db_name}.{self.collection_name}")
            
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            raise
        
        # Request configuration
        self.max_retries = 3
        self.request_timeout = 90
        self.retry_delay = 2
        
        # Prepare headers for authenticated requests
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token}' if self.token else None
        }
        # Remove None values
        self.headers = {k: v for k, v in self.headers.items() if v is not None}
        
        # Thread-safe counters and locks for parallel processing
        self.processed_count = 0
        self.error_count = 0
        self.processed_lock = Lock()
        self.response_file_lock = Lock()
        
        # Check API health on initialization
        self._check_api_health()
    
    def _load_prompt_template(self):
        """
        Load the prompt template with examples - Updated for Banking Focus with Granular Topic Extraction
        """
        return """
        # Ticket Topic Extraction System

        - You are an Expert Topic Modeling Analyst specializing in ticket thread analysis for a Banking and Financial Services organization. Your task is to analyze the provided ticket thread and extract only the following information:
        
        - Your ONLY task is to extract a Dominant Topic, exactly 3 Sub-Topics, and an Urgency classification from the provided text. You are NOT analyzing a ticket thread - you are analyzing a self-contained text sample.
        
        ## OUTPUT FORMAT
        
        You MUST use EXACTLY this format with NO additions, explanations, or commentary:
        
        ```
        Dominant Topic: [Topic]
        Sub-Topics: [subtopic 1], [subtopic 2], [subtopic 3]
        Urgency: [True/False]
        ```
        
        ## STRICT RULES
        
        1. DO NOT comment on the text's length, structure, or content
        2. DO NOT address the reader or offer assistance
        3. DO NOT add disclaimers, introductions, or conclusions
        4. DO NOT ask questions or offer options
        5. DO NOT use phrases like "this ticket," "this thread," "this chain," etc.
        6. DO NOT offer to provide more information or analysis
        7. NEVER use first-person or second-person language (I, we, you)
        8. TREAT each sample as a COMPLETE text, not part of a larger conversation
        
        ## GRANULAR ANALYSIS REQUIREMENTS
        
        1. **Dominant Topic**: Extract the MOST SPECIFIC and PRECISE subject that the ticket is discussing. Avoid generic terms. Focus on the exact issue, process, or matter being addressed (MAXIMUM 3 WORDS)
        2. **Sub-Topics**: EXACTLY THREE highly specific aspects, detailed issues, or precise points mentioned in the ticket content. These should be granular elements that drill down into the specific details of the dominant topic (EACH SUB-TOPIC MAXIMUM 3 WORDS) 
        3. **Urgency**: Mark as TRUE ONLY if the text contains:
           - Direct urgency words (urgent, critical, immediate, ASAP, emergency, priority)
           - Time-sensitive language (today, tomorrow, deadline, by end of day)
           - Regulatory compliance deadlines
           - System outages affecting banking operations
           - Security incidents or fraud alerts
           - Customer service escalations
           - Trading/market-related time constraints
           - Escalation language
           - Mark FALSE otherwise

        ## GRANULAR TOPIC EXTRACTION APPROACH
        
        1. READ the entire ticket content carefully
        2. IDENTIFY the most specific action, problem, or request being discussed
        3. AVOID broad categories - focus on the precise issue or process
        4. EXTRACT granular details that make the topic unique to this specific ticket
        5. ENSURE sub-topics are specific elements that provide detailed context about the dominant topic
        
        ## GRANULAR TOPIC EXTRACTION GUIDELINES
        
        ### SPECIFICITY REQUIREMENTS:
        - Replace generic terms with specific ones (e.g., "Account Issues" → "Overdraft Fees", "System Problems" → "ATM Withdrawal Failure")
        - Focus on the exact banking product, service, or process mentioned
        - Include specific actions or problems rather than broad categories
        - Capture the precise nature of the request, complaint, or inquiry
        
        ### AVOID GENERIC TERMS - USE SPECIFIC ALTERNATIVES:
        - Instead of "Account Statement" → Use "Monthly Statement Missing", "Statement Format Error", "PDF Generation Failure"
        - Instead of "System Maintenance" → Use "Core Banking Upgrade", "ATM Network Downtime", "Mobile App Maintenance"
        - Instead of "Loan Application" → Use "Mortgage Pre-approval", "Auto Loan Documentation", "Business Credit Extension"
        - Instead of "Customer Service" → Use "Complaint Escalation", "Service Fee Dispute", "Branch Wait Times"
        - Instead of "Transaction Issues" → Use "Wire Transfer Delay", "Check Deposit Hold", "Card Payment Decline"
        
        ### SUB-TOPIC SPECIFICITY:
        - Sub-topics should be granular details that provide specific context
        - Focus on exact processes, specific errors, particular requirements, or detailed outcomes
        - Avoid broad descriptors - use precise terminology from the ticket content
        - Each sub-topic should add unique, specific information about the dominant topic
        
        ## TOPIC EXTRACTION GUIDELINES
        
        - Extract topics based ONLY on what is actually written in the ticket
        - DO NOT impose predefined categories or assume topics not present in the text
        - The dominant topic should reflect the MOST SPECIFIC subject being discussed
        - Sub-topics should be precise aspects, detailed issues, or specific elements mentioned within that primary subject
        - Topics should be derived from the ticket's actual content with maximum granularity
        - ALL TOPICS AND SUB-TOPICS MUST BE MAXIMUM 3 WORDS EACH
        - Use precise, specific terms that capture the exact nature of the topic
        - Prioritize technical accuracy and specificity over general banking terms

        ## IMPORTANT EXCLUSIONS:
    
        DO NOT EXTRACT as topics or include in your analysis:
        - Specific account numbers or customer IDs
        - Transaction reference numbers
        - Bank routing numbers or SWIFT codes
        - Specific server names or system identifiers
        - Unique identifiers that only appear once
        - Customer names or personal information
        - File paths or URLs
        - Alphanumeric codes or reference numbers
        
        
        ## EXAMPLES OF GRANULAR RESPONSES
        
        Example 1 (OLD - Generic):
        ```
        Dominant Topic: Account Statement
        Sub-Topics: Missing Transactions, Delivery Delay, Customer Service
        Urgency: False
        ```
        
        Example 1 (NEW - Granular):
        ```
        Dominant Topic: PDF Statement Corruption
        Sub-Topics: Image Rendering Error, Email Attachment Failure, Reprint Request
        Urgency: False
        ```
        
        Example 2 (OLD - Generic):
        ```
        Dominant Topic: System Maintenance
        Sub-Topics: Scheduled Downtime, Service Interruption, Alternative Access
        Urgency: True
        ```
        
        Example 2 (NEW - Granular):
        ```
        Dominant Topic: Core Banking Upgrade
        Sub-Topics: Weekend Deployment Window, Branch Terminal Offline, Mobile Banking Redirect
        Urgency: True
        ```
        
        Example 3 (OLD - Generic):
        ```
        Dominant Topic: Loan Application
        Sub-Topics: Documentation Requirements, Processing Timeline, Approval Conditions
        Urgency: False
        ```
        
        Example 3 (NEW - Granular):
        ```
        Dominant Topic: Mortgage Pre-approval Delay
        Sub-Topics: Income Verification Gap, Property Appraisal Pending, Credit Score Recheck
        Urgency: False
        ```
        
        Example 4 (OLD - Generic):
        ```
        Dominant Topic: Account Activity
        Sub-Topics: Transaction Monitoring, Security Verification, Account Restriction
        Urgency: True
        ```
        
        Example 4 (NEW - Granular):
        ```
        Dominant Topic: Fraudulent Wire Transfer
        Sub-Topics: Beneficiary Name Mismatch, AML Alert Triggered, Account Freeze Applied
        Urgency: True
        ```
        
        Text to analyze:
        {cleaned_text}
        
        """

    def _check_api_health(self):
        """
        Check if the Ollama API is responsive
        """
        try:
            # Simple health check - try to list models
            response = requests.get(self.tags_url, headers=self.headers, timeout=15)
            
            if response.status_code == 200:
                logger.info("Remote Ollama API health check: OK")
                return True
            else:
                logger.warning(f"Remote Ollama API health check failed with status code: {response.status_code}")
                return False
        except Exception as e:
            logger.error(f"Remote Ollama API health check failed: {e}")
            return False

    def _recreate_ollama_client(self):
        """
        Recreate connection (for compatibility with existing code)
        """
        try:
            logger.info("Refreshing remote Ollama connection...")
            # Just refresh headers in case token needs updating
            self.headers = {
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {self.token}' if self.token else None
            }
            self.headers = {k: v for k, v in self.headers.items() if v is not None}
            return True
        except Exception as e:
            logger.error(f"Failed to refresh remote Ollama connection: {e}")
            return False

    @retry(
        retry=retry_if_exception_type((requests.exceptions.RequestException, 
                                     TimeoutError, 
                                     ConnectionError,
                                     json.JSONDecodeError,
                                     KeyError,
                                     ValueError)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        before_sleep=lambda retry_state: logger.info(
            f"API request failed, retrying in {retry_state.next_action.sleep} seconds... "
            f"(Attempt {retry_state.attempt_number}/{3})"
        )
    )
    def _execute_ollama_request(self, cleaned_text, prompt):
        """
        Execute Ollama API request with retry logic using direct HTTP calls
        """
        # Check API health before making the request
        if not self._check_api_health():
            logger.info("API seems to be down, waiting 15 seconds before retry...")
            time.sleep(15)
            # Check again
            if not self._check_api_health():
                # Try to refresh the connection
                if self._recreate_ollama_client():
                    logger.info("Remote Ollama connection refreshed successfully")
                else:
                    logger.error("Failed to refresh connection and API is still down")
                    raise ConnectionError("Remote Ollama API is not responding after connection refresh attempt")
            
        # Prepare the payload
        payload = {
            "model": self.model_name,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.3,
                "num_predict": 800,
                "top_k": 20,
                "top_p": 0.8,
                "num_ctx": 4096
            }
        }
        
        try:
            logger.debug(f"🌐 Calling remote Ollama endpoint: {self.ollama_url}")
            response = requests.post(
                self.ollama_url,
                json=payload,
                headers=self.headers,
                timeout=self.request_timeout
            )
            
            logger.debug(f"📡 Response status: {response.status_code}")
            
            # Check if response is empty
            if not response.text.strip():
                raise ValueError("Empty response from remote Ollama API")
            
            response.raise_for_status()
            
            try:
                result = response.json()
            except json.JSONDecodeError as e:
                logger.error(f"JSON decode error. Response text: {response.text[:200]}...")
                raise
            
            if "response" not in result:
                logger.error(f"No 'response' field. Available fields: {list(result.keys())}")
                raise KeyError("No 'response' field in remote Ollama response")
                
            return {"response": result["response"]}
            
        except requests.exceptions.Timeout:
            logger.error(f"Request timed out after {self.request_timeout} seconds")
            raise
        except requests.exceptions.ConnectionError:
            logger.error("Connection error - check remote Ollama endpoint")
            raise
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error: {e.response.status_code} - {e.response.text[:200]}")
            raise
        except Exception as e:
            logger.error(f"Remote Ollama API error: {e}")
            raise

    def summarize_long_text(self, text):
        """
        Summarize long text using the LLM to reduce it to around 1000 words

        :param text: Long text content to summarize
        :return: Summarized text
        """
        logger.info(f"Text exceeds 1000 words. Summarizing before topic extraction...")

        # Create the summarization prompt
        summarize_prompt = f"""
        # Text Summarization Task

        Below is a long text that needs to be summarized. Please create a comprehensive summary of approximately 200-300 words 
        that captures the main points, key details, and overall context. Maintain the essential information that would 
        be needed for topic classification.

        ## RULES:
        1. Preserve key terminology, technical terms, and important references
        2. Keep the most important discussions and topics
        3. Maintain the overall tone and intent of the original
        4. Aim for about 200-300 words in your summary
        5. Include only the summary in your response, no additional commentary

        ## TEXT TO SUMMARIZE:

        {text}
        """

        try:
            # Use the retry-enabled function with the summarization prompt
            response = self._execute_ollama_request(text, summarize_prompt)

            # Extract the summarized text
            summarized_text = response['response'].strip()

            # Log summarization results
            original_word_count = len(text.split())
            summary_word_count = len(summarized_text.split())
            logger.info(f"Text summarized: {original_word_count} words → {summary_word_count} words")

            # Clear the context after summarization (for compatibility)
            self._recreate_ollama_client()

            return summarized_text

        except Exception as e:
            logger.error(f"Error during text summarization: {e}")
            # If summarization fails, truncate the text as a fallback
            truncated_text = " ".join(text.split()[:1000])
            logger.warning(f"Summarization failed, truncating text to 1000 words instead")
            return truncated_text

    def extract_topics(self, cleaned_text, document_id=None):
        """
        Use LLM to extract dominant topic, subtopic, and urgency level
        
        :param cleaned_text: Ticket cleaned text content
        :param document_id: MongoDB document ID for logging
        :return: Dictionary with extracted information
        """
        # Skip empty messages
        if not cleaned_text or pd.isna(cleaned_text) or str(cleaned_text).lower() in ['nan', 'none', '']:
            return {
                "dominant_topic": "Unknown",
                "subtopics": "No subtopics identified",
                "urgency": False,
                "summarized_text": None
            }

        # Check if text exceeds 1000 words - if so, summarize it first
        word_count = len(str(cleaned_text).split())
        summarized_text = None
        
        if word_count > 1000:
            logger.info(f"Document {document_id} has {word_count} words, exceeding 1000 word limit")
            summarized_text = self.summarize_long_text(str(cleaned_text))
            logger.info(f"Using summarized text ({len(summarized_text.split())} words) for topic extraction")
            # Use the summarized text for topic extraction
            text_for_extraction = summarized_text
        else:
            logger.info(f"Document {document_id} has {word_count} words, under limit. Proceeding with direct topic extraction.")
            # Use the original text for topic extraction
            text_for_extraction = str(cleaned_text)

        # Format the prompt with the text to be analyzed
        prompt = self.prompt_template.format(cleaned_text=text_for_extraction)

        try:
            # Use the retry-enabled function
            response = self._execute_ollama_request(text_for_extraction, prompt)

            # Extract the response text
            response_text = response['response'].strip()

            # Thread-safe file writing
            with self.response_file_lock:
                with open('mongodb_ticket_responses.txt', 'a', encoding='utf-8') as file:
                    file.write(f"\n\n--- DOCUMENT ID: {document_id} ---\n\n")
                    file.write(response_text)

            # Log the raw response for debugging
            logger.debug(f"Raw LLM response for document {document_id}: {response_text}")

            # Parse the formatted response
            dominant_topic = self._extract_dominant_topic(response_text)
            subtopics = self._extract_subtopics(response_text)
            urgent = self._extract_urgency(response_text)

            return {
                "dominant_topic": dominant_topic,
                "subtopics": subtopics,
                "urgency": urgent.lower() == 'true' if isinstance(urgent, str) else urgent,
                "summarized_text": summarized_text
            }

        except Exception as e:
            logger.error(f"LLM processing error for document {document_id}: {e}")
            # Clear the context even on error (for compatibility)
            try:
                self._recreate_ollama_client()
                logger.info(f"Connection refreshed after error on document {document_id}")
            except Exception as clear_error:
                logger.warning(f"Failed to refresh connection after error: {clear_error}")

            return {
                "dominant_topic": "Processing Error",
                "subtopics": "Processing Error",
                "urgency": False,
                "summarized_text": None
            }
    
    def _extract_dominant_topic(self, text):
        """Extract dominant topic from formatted response with improved regex"""
        # Try the standard format first
        match = re.search(r'Dominant Topic:\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Try alternative format with different line breaks
        match = re.search(r'Dominant Topic:[\s\n]*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # If all else fails, try to look for something resembling a topic after "Dominant Topic:"
        match = re.search(r'Dominant Topic:.*?(\w[\w\s]+\w)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return "Unknown Topic"  # Default value instead of None

    def _extract_subtopics(self, text):
        """Extract subtopics from formatted response with improved regex"""
        # Standard format
        match = re.search(r'Sub-Topics:\s*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Try alternative format with different separators
        match = re.search(r'Sub-Topics:[\s\n]*(.+?)(?:\n|$)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Try to extract anything after "Sub-Topics:" that might be topics
        match = re.search(r'Sub-Topics:.*?([\w\s,]+(?:,\s*[\w\s]+)*)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        return "No subtopics identified"  # Default value instead of None

    def _extract_urgency(self, text):
        """Extract urgency from formatted response with improved regex"""
        # Try standard format
        match = re.search(r'Urgency:\s*(True|False)(?:\n|$)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # Try alternative format
        match = re.search(r'Urgency:[\s\n]*(True|False)', text, re.IGNORECASE)
        if match:
            return match.group(1).strip()

        # If the word "urgent" appears in the text, default to True
        if re.search(r'\b(urgent|emergency|critical|asap|priority)\b', text, re.IGNORECASE):
            return "True"

        return "False"  # Default to False instead of None
    
    def get_unprocessed_documents(self):
        """
        Get documents that haven't been processed yet (don't have dominant_topic field)
        
        :return: List of unprocessed documents
        """
        query = {
            "$or": [
                {"dominant_topic": {"$exists": False}},
                {"dominant_topic": None},
                {"dominant_topic": ""}
            ]
        }
        return list(self.collection.find(query))
    
    def get_total_document_count(self):
        """
        Get total count of documents in the collection
        
        :return: Total document count
        """
        return self.collection.count_documents({})
    
    def get_unprocessed_count(self):
        """
        Get count of unprocessed documents
        
        :return: Unprocessed document count
        """
        query = {
            "$or": [
                {"dominant_topic": {"$exists": False}},
                {"dominant_topic": None},
                {"dominant_topic": ""}
            ]
        }
        return self.collection.count_documents(query)
    
    def update_document_with_topics(self, document_id, topic_data):
        """
        Update a MongoDB document with extracted topics
        
        :param document_id: MongoDB ObjectId
        :param topic_data: Dictionary with extracted topic information
        """
        update_data = {
            "dominant_topic": topic_data["dominant_topic"],
            "subtopics": topic_data["subtopics"],
            "urgency": topic_data["urgency"],
            "processed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "model_used": self.model_name
        }
        
        # Add summarized text if it exists
        if topic_data.get("summarized_text"):
            update_data["summarized_text"] = topic_data["summarized_text"]
            update_data["was_summarized"] = True
        else:
            update_data["was_summarized"] = False
        
        try:
            result = self.collection.update_one(
                {"_id": document_id},
                {"$set": update_data}
            )
            
            if result.modified_count == 1:
                logger.info(f"Successfully updated document {document_id}")
                return True
            else:
                logger.warning(f"No document was modified for ID {document_id}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to update document {document_id}: {e}")
            return False

    def process_single_document(self, doc, current_index, total_count):
        """
        Process a single document - designed for parallel execution
        
        :param doc: Document to process
        :param current_index: Current document index for progress tracking
        :param total_count: Total number of documents to process
        :return: Tuple of (success_boolean, document_id, error_message)
        """
        try:
            document_id = doc["_id"]
            cleaned_text = doc.get("cleaned_text", "")
            
            # Skip if cleaned_text is empty
            if not cleaned_text or str(cleaned_text).strip() == "":
                logger.info(f"Skipping document {document_id} - empty cleaned_text")
                return False, document_id, "Empty cleaned_text"
            
            logger.info(f"Processing document {document_id} ({current_index + 1}/{total_count}) [Thread: {threading.current_thread().name}]")
            
            # Check API health before processing
            if not self._check_api_health():
                logger.warning("API is not responding, waiting 30 seconds before retrying...")
                time.sleep(30)
                # If still not healthy, try to recreate the client
                if not self._check_api_health():
                    logger.info("Refreshing remote Ollama connection...")
                    self._recreate_ollama_client()
                    # If still not healthy after refresh, mark as error
                    if not self._check_api_health():
                        logger.error(f"Remote API still unresponsive after connection refresh. Marking document {document_id} as error.")
                        error_topic_data = {
                            "dominant_topic": "API Error",
                            "subtopics": "API Error",
                            "urgency": False,
                            "summarized_text": None
                        }
                        self.update_document_with_topics(document_id, error_topic_data)
                        return False, document_id, "API unresponsive"
            
            # Process with retry mechanism
            max_attempts = 3
            topic_data = None
            last_error = None
            
            for attempt in range(max_attempts):
                try:
                    topic_data = self.extract_topics(cleaned_text, document_id)
                    break
                except Exception as e:
                    last_error = str(e)
                    if attempt < max_attempts - 1:
                        wait_time = (attempt + 1) * 10  # Progressive backoff
                        logger.warning(f"Attempt {attempt+1} failed for document {document_id}. Waiting {wait_time}s before retry. Error: {e}")
                        time.sleep(wait_time)
                    else:
                        logger.error(f"All {max_attempts} attempts failed for document {document_id}. Error: {e}")
                        topic_data = {
                            "dominant_topic": "Processing Error",
                            "subtopics": "Processing Error",
                            "urgency": False,
                            "summarized_text": None
                        }
            
            # Update the document in MongoDB
            if self.update_document_with_topics(document_id, topic_data):
                logger.info(f"Successfully processed and updated document {document_id}")
                return True, document_id, None
            else:
                logger.error(f"Failed to update document {document_id}")
                return False, document_id, "Database update failed"
                
        except Exception as e:
            error_msg = f"Error processing document {doc.get('_id', 'unknown')}: {e}"
            logger.error(error_msg)
            return False, doc.get('_id', 'unknown'), error_msg
    
    def process_mongodb_collection(self):
        """
        Process all unprocessed documents in the MongoDB collection using parallel processing
        """
        # Get initial counts
        total_docs = self.get_total_document_count()
        unprocessed_count = self.get_unprocessed_count()
        
        logger.info(f"Total documents in collection: {total_docs}")
        logger.info(f"Unprocessed documents: {unprocessed_count}")
        logger.info(f"Using {self.max_workers} parallel workers")
        
        if unprocessed_count == 0:
            logger.info("No unprocessed documents found. Exiting.")
            return 0, 0
        
        # Get unprocessed documents
        unprocessed_docs = self.get_unprocessed_documents()
        
        # Reset counters
        self.processed_count = 0
        self.error_count = 0
        
        # Process documents in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_doc = {
                executor.submit(self.process_single_document, doc, idx, len(unprocessed_docs)): (doc, idx)
                for idx, doc in enumerate(unprocessed_docs)
            }
            
            # Process completed tasks
            for future in as_completed(future_to_doc):
                doc, idx = future_to_doc[future]
                
                try:
                    success, document_id, error_msg = future.result()
                    
                    # Thread-safe counter updates
                    with self.processed_lock:
                        if success:
                            self.processed_count += 1
                        else:
                            self.error_count += 1
                        
                        # Log progress every 10 documents
                        total_completed = self.processed_count + self.error_count
                        if total_completed % 10 == 0:
                            logger.info(f"Progress: {total_completed}/{len(unprocessed_docs)} documents processed "
                                      f"(Success: {self.processed_count}, Errors: {self.error_count})")
                
                except Exception as e:
                    logger.error(f"Error getting result for document {doc.get('_id', 'unknown')}: {e}")
                    with self.processed_lock:
                        self.error_count += 1
        
        # Final summary
        logger.info(f"Parallel processing completed. Processed: {self.processed_count}, Errors: {self.error_count}")
        return self.processed_count, self.error_count
    
    def process_mongodb_collection_sequential(self):
        """
        Process all unprocessed documents in the MongoDB collection sequentially (fallback method)
        """
        # Get initial counts
        total_docs = self.get_total_document_count()
        unprocessed_count = self.get_unprocessed_count()
        
        logger.info(f"Total documents in collection: {total_docs}")
        logger.info(f"Unprocessed documents: {unprocessed_count}")
        logger.info("Using sequential processing")
        
        if unprocessed_count == 0:
            logger.info("No unprocessed documents found. Exiting.")
            return 0, 0
        
        # Get unprocessed documents
        unprocessed_docs = self.get_unprocessed_documents()
        
        processed_count = 0
        error_count = 0
        
        # Process each document sequentially
        for idx, doc in enumerate(unprocessed_docs):
            try:
                success, document_id, error_msg = self.process_single_document(doc, idx, len(unprocessed_docs))
                
                if success:
                    processed_count += 1
                else:
                    error_count += 1
                
                # Log progress every 5 documents
                if (processed_count + error_count) % 5 == 0:
                    logger.info(f"Progress: {processed_count + error_count}/{len(unprocessed_docs)} documents processed")
                
                # Add a small delay between requests to avoid overloading the API
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing document {doc.get('_id', 'unknown')}: {e}")
                error_count += 1
                
                # Wait longer before continuing after error
                time.sleep(5)
        
        # Final summary
        logger.info(f"Sequential processing completed. Processed: {processed_count}, Errors: {error_count}")
        return processed_count, error_count
    
    def generate_statistics(self):
        """
        Generate and save statistics about the processed tickets
        """
        try:
            # Get all processed documents
            processed_docs = list(self.collection.find({"dominant_topic": {"$exists": True, "$ne": None}}))
            
            if not processed_docs:
                logger.warning("No processed documents found for statistics")
                return
            
            # Count unique topics
            unique_topics = len(set(doc.get("dominant_topic", "") for doc in processed_docs))
            
            # Count urgent messages
            urgent_count = sum(1 for doc in processed_docs if doc.get("urgency", False))
            urgent_percentage = (urgent_count / len(processed_docs)) * 100
            
            # Count summarized messages
            summarized_count = sum(1 for doc in processed_docs if doc.get("was_summarized", False))
            
            # Log statistics
            logger.info(f"Statistics: Found {unique_topics} unique dominant topics")
            logger.info(f"Statistics: {urgent_count} tickets marked as urgent ({urgent_percentage:.1f}%)")
            logger.info(f"Statistics: {summarized_count} tickets were summarized")
            
            # Save statistics to file
            with open('mongodb_ticket_analysis_statistics.txt', 'w', encoding='utf-8') as stats_file:
                stats_file.write(f"# MongoDB Ticket Topic Analysis Statistics\n")
                stats_file.write(f"# Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                stats_file.write(f"# Database: {self.db_name}\n")
                stats_file.write(f"# Collection: {self.collection_name}\n")
                stats_file.write(f"# Model: {self.model_name}\n")
                stats_file.write(f"# Remote Endpoint: {self.base_url}\n")
                stats_file.write(f"# Parallel Workers: {self.max_workers}\n\n")
                stats_file.write(f"Total processed tickets: {len(processed_docs)}\n")
                stats_file.write(f"Unique dominant topics: {unique_topics}\n")
                stats_file.write(f"Tickets marked as urgent: {urgent_count} ({urgent_percentage:.1f}%)\n")
                stats_file.write(f"Tickets that were summarized: {summarized_count}\n")
                
                # Add most common topics
                topic_counts = Counter(doc.get("dominant_topic", "") for doc in processed_docs)
                stats_file.write("\n## Most Common Dominant Topics:\n")
                for topic, count in topic_counts.most_common(10):
                    if topic:  # Skip empty topics
                        stats_file.write(f"- {topic}: {count} tickets\n")
                
                # Add most common subtopics
                stats_file.write("\n## Most Common Subtopics:\n")
                all_subtopics = []
                for doc in processed_docs:
                    subtopics_str = doc.get("subtopics", "")
                    if subtopics_str and subtopics_str != "No subtopics identified":
                        subtopics = [s.strip() for s in subtopics_str.split(',')]
                        all_subtopics.extend(subtopics)
                
                subtopic_counts = Counter(all_subtopics)
                for subtopic, count in subtopic_counts.most_common(10):
                    if subtopic:  # Skip empty subtopics
                        stats_file.write(f"- {subtopic}: {count} occurrences\n")
            
            logger.info("Statistics report generated in 'mongodb_ticket_analysis_statistics.txt'")
            
        except Exception as e:
            logger.error(f"Error generating statistics: {e}")
    
    def close_connection(self):
        """
        Close MongoDB connection
        """
        try:
            self.client.close()
            logger.info("MongoDB connection closed successfully")
        except Exception as e:
            logger.error(f"Error closing MongoDB connection: {e}")

def main():
    """
    Main function to run the MongoDB topic extraction process
    """
    processor = None
    
    try:
        # Initialize the processor with parallel processing
        logger.info("Starting MongoDB Ticket Topic Extraction Process with Parallel Processing")
        
        # You can adjust max_workers based on your system and API limits
        # Recommended: 3-8 workers for most setups
        max_workers = int(os.getenv("MAX_WORKERS", "5"))
        logger.info(f"Using {max_workers} parallel workers")
        
        processor = MongoDBTopicExtractionProcessor(max_workers=max_workers)
        
        # Process the collection using parallel processing
        try:
            processed_count, error_count = processor.process_mongodb_collection()
        except Exception as parallel_error:
            logger.error(f"Parallel processing failed: {parallel_error}")
            logger.info("Falling back to sequential processing...")
            processed_count, error_count = processor.process_mongodb_collection_sequential()
        
        # Generate statistics
        processor.generate_statistics()
        
        logger.info(f"Process completed successfully. Processed: {processed_count}, Errors: {error_count}")
        
        # Performance summary
        total_docs = processed_count + error_count
        if total_docs > 0:
            success_rate = (processed_count / total_docs) * 100
            logger.info(f"Success rate: {success_rate:.1f}% ({processed_count}/{total_docs})")
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error in main process: {e}")
        raise
    finally:
        if processor:
            processor.close_connection()

if __name__ == "__main__":
    main()