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
CHAT_COLLECTION = "chat-chunks"

# Set up logging - modified for MongoDB chat processing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
    handlers=[
        logging.FileHandler('mongodb_chat_topic_extraction.log', mode='w', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class MongoDBChatTopicExtractionProcessor:
    def __init__(self, model_name='gemma3:27b', base_url="http://34.147.17.26:31100", token="b805213e7b048d21f02dae5922973e9639ef971b0bc6bf804efad9c707527249", mongo_uri=None, db_name=DB_NAME, collection_name=CHAT_COLLECTION, max_workers=5):
        """
        Initialize the MongoDB chat topic extraction processor
        
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
        Load the prompt template for chat conversation analysis
        """
        return """
        # Chat Conversation Topic Extraction System

        - You are an Expert Topic Modeling Analyst specializing in chat conversation analysis for a Banking and Financial Services organization. Your task is to analyze the provided chat conversation and extract only the following information:
        
        - Your ONLY task is to extract a Dominant Topic, exactly 3 Sub-Topics, and an Urgency classification from the provided chat conversation text.
        
        ## OUTPUT FORMAT
        
        You MUST use EXACTLY this format with NO additions, explanations, or commentary:
        
        ```
        Dominant Topic: [Topic]
        Sub-Topics: [subtopic 1], [subtopic 2], [subtopic 3]
        Urgency: [True/False]
        ```
        
        ## STRICT RULES
        
        1. DO NOT comment on the conversation's length, structure, or content
        2. DO NOT address the reader or offer assistance
        3. DO NOT add disclaimers, introductions, or conclusions
        4. DO NOT ask questions or offer options
        5. DO NOT use phrases like "this conversation," "this chat," "this discussion," etc.
        6. DO NOT offer to provide more information or analysis
        7. NEVER use first-person or second-person language (I, we, you)
        8. TREAT each conversation as a COMPLETE interaction between banking staff and customers
        
        ## GRANULAR ANALYSIS REQUIREMENTS
        
        1. **Dominant Topic**: Extract the MOST SPECIFIC and PRECISE subject that the conversation is discussing. Focus on the exact banking issue, process, or customer inquiry being addressed (MAXIMUM 3 WORDS)
        2. **Sub-Topics**: EXACTLY THREE highly specific aspects, detailed issues, or precise points mentioned in the conversation. These should be granular elements that drill down into the specific details of the dominant topic (EACH SUB-TOPIC MAXIMUM 3 WORDS) 
        3. **Urgency**: Mark as TRUE ONLY if the conversation contains:
           - Direct urgency words (urgent, critical, immediate, ASAP, emergency, priority)
           - Time-sensitive language (today, tomorrow, deadline, by end of day)
           - Customer escalation language (angry, frustrated, complaint escalation)
           - Account security concerns (fraud, unauthorized transactions)
           - System outages affecting customer services
           - Regulatory compliance deadlines
           - Trading/market-related time constraints
           - Mark FALSE otherwise

        ## GRANULAR TOPIC EXTRACTION APPROACH
        
        1. READ the entire conversation carefully
        2. IDENTIFY the most specific banking service, problem, or request being discussed
        3. AVOID broad categories - focus on the precise issue or process
        4. EXTRACT granular details that make the topic unique to this specific conversation
        5. ENSURE sub-topics are specific elements that provide detailed context about the dominant topic
        
        ## CONVERSATION ANALYSIS GUIDELINES
        
        ### SPECIFICITY REQUIREMENTS:
        - Replace generic terms with specific ones (e.g., "Account Issues" → "Overdraft Inquiry", "Transaction Problems" → "Card Decline Issue")
        - Focus on the exact banking product, service, or process mentioned
        - Include specific actions or problems rather than broad categories
        - Capture the precise nature of the customer request, complaint, or inquiry
        
        ### AVOID GENERIC TERMS - USE SPECIFIC ALTERNATIVES:
        - Instead of "Account Inquiry" → Use "Balance Check Request", "Statement Missing Issue", "Account Closure Process"
        - Instead of "Transaction Issue" → Use "Payment Declined", "Wire Transfer Delay", "Check Deposit Hold"
        - Instead of "Card Problem" → Use "ATM Card Stuck", "PIN Reset Request", "Card Replacement Order"
        - Instead of "Loan Discussion" → Use "Mortgage Rate Quote", "Auto Loan Approval", "Credit Line Increase"
        - Instead of "Customer Service" → Use "Complaint Resolution", "Fee Reversal Request", "Branch Hours Inquiry"
        
        ### SUB-TOPIC SPECIFICITY:
        - Sub-topics should be granular details that provide specific context about the conversation
        - Focus on exact processes, specific customer concerns, particular solutions offered
        - Avoid broad descriptors - use precise terminology from the conversation
        - Each sub-topic should add unique, specific information about the dominant topic
        
        ## TOPIC EXTRACTION GUIDELINES
        
        - Extract topics based ONLY on what is actually discussed in the conversation
        - DO NOT impose predefined categories or assume topics not present in the chat
        - The dominant topic should reflect the MOST SPECIFIC subject being discussed between the participants
        - Sub-topics should be precise aspects, detailed issues, or specific elements mentioned within that primary subject
        - Topics should be derived from the conversation's actual content with maximum granularity
        - ALL TOPICS AND SUB-TOPICS MUST BE MAXIMUM 3 WORDS EACH
        - Use precise, specific terms that capture the exact nature of the banking discussion
        - Prioritize technical accuracy and specificity over general banking terms

        ## IMPORTANT EXCLUSIONS:
    
        DO NOT EXTRACT as topics or include in your analysis:
        - Specific account numbers or customer IDs
        - Transaction reference numbers
        - Customer names or personal information
        - Bank routing numbers or SWIFT codes
        - Specific server names or system identifiers
        - Unique identifiers that only appear once
        - File paths or URLs
        - Alphanumeric codes or reference numbers
        
        ## EXAMPLES OF GRANULAR RESPONSES FOR CHAT CONVERSATIONS
        
        Example 1 (Customer calling about unrecognized charge):
        ```
        Dominant Topic: Unauthorized Charge Dispute
        Sub-Topics: Subscription Service Recognition, Account History Review, Charge Investigation Process
        Urgency: False
        ```
        
        Example 2 (ATM not dispensing cash but account debited):
        ```
        Dominant Topic: ATM Malfunction Claim
        Sub-Topics: Cash Dispensing Failure, Account Debit Reversal, Transaction Dispute Filing
        Urgency: True
        ```
        
        Example 3 (Customer wants to increase credit limit):
        ```
        Dominant Topic: Credit Limit Increase
        Sub-Topics: Income Verification Required, Credit Score Review, Application Processing Timeline
        Urgency: False
        ```
        
        Example 4 (Suspicious transaction alert):
        ```
        Dominant Topic: Fraud Alert Investigation
        Sub-Topics: Transaction Pattern Analysis, Card Security Freeze, Identity Verification Process
        Urgency: True
        ```
        
        Chat conversation to analyze:
        {chat_conversation}
        
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
    def _execute_ollama_request(self, chat_conversation, prompt):
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

    def format_chat_conversation(self, cleaned_segments):
        """
        Format the cleaned_segments into a numbered conversation format
        
        :param cleaned_segments: List of message objects from MongoDB
        :return: Formatted conversation string
        """
        if not cleaned_segments or len(cleaned_segments) == 0:
            return ""
        
        conversation_lines = []
        for i, segment in enumerate(cleaned_segments, 1):
            if isinstance(segment, dict) and 'text' in segment:
                text = segment['text'].strip()
                if text:  # Only add non-empty text
                    conversation_lines.append(f"{i}: {text}")
        
        return "\n".join(conversation_lines)

    def summarize_long_conversation(self, conversation):
        """
        Summarize long conversation using the LLM to reduce it to manageable size

        :param conversation: Long conversation content to summarize
        :return: Summarized conversation
        """
        logger.info(f"Conversation exceeds 1000 words. Summarizing before topic extraction...")

        # Create the summarization prompt
        summarize_prompt = f"""
        # Chat Conversation Summarization Task

        Below is a long chat conversation that needs to be summarized. Please create a comprehensive summary of approximately 200-300 words 
        that captures the main points, key issues discussed, and overall context of the banking conversation. Maintain the essential 
        information that would be needed for topic classification.

        ## RULES:
        1. Preserve key banking terminology, account details, and important references
        2. Keep the most important exchanges and topics discussed
        3. Maintain the conversational flow and participant roles (customer service vs customer)
        4. Aim for about 200-300 words in your summary
        5. Include only the summary in your response, no additional commentary
        6. Focus on the banking issue or request being discussed

        ## CONVERSATION TO SUMMARIZE:

        {conversation}
        """

        try:
            # Use the retry-enabled function with the summarization prompt
            response = self._execute_ollama_request(conversation, summarize_prompt)

            # Extract the summarized text
            summarized_conversation = response['response'].strip()

            # Log summarization results
            original_word_count = len(conversation.split())
            summary_word_count = len(summarized_conversation.split())
            logger.info(f"Conversation summarized: {original_word_count} words → {summary_word_count} words")

            # Clear the context after summarization (for compatibility)
            self._recreate_ollama_client()

            return summarized_conversation

        except Exception as e:
            logger.error(f"Error during conversation summarization: {e}")
            # If summarization fails, truncate the conversation as a fallback
            truncated_conversation = " ".join(conversation.split()[:1000])
            logger.warning(f"Summarization failed, truncating conversation to 1000 words instead")
            return truncated_conversation

    def extract_topics_from_chat(self, cleaned_segments, document_id=None):
        """
        Use LLM to extract dominant topic, subtopic, and urgency level from chat conversation
        
        :param cleaned_segments: List of message segments from MongoDB document
        :param document_id: MongoDB document ID for logging
        :return: Dictionary with extracted information
        """
        # Skip empty or invalid segments
        if not cleaned_segments or len(cleaned_segments) == 0:
            return {
                "dominant_topic": "Empty Conversation",
                "subtopics": "No subtopics identified",
                "urgency": False,
                "summarized_conversation": None
            }

        # Format the conversation
        formatted_conversation = self.format_chat_conversation(cleaned_segments)
        
        if not formatted_conversation:
            return {
                "dominant_topic": "Invalid Conversation",
                "subtopics": "No subtopics identified",
                "urgency": False,
                "summarized_conversation": None
            }

        # Check if conversation exceeds 1000 words - if so, summarize it first
        word_count = len(formatted_conversation.split())
        summarized_conversation = None
        
        if word_count > 1000:
            logger.info(f"Document {document_id} conversation has {word_count} words, exceeding 1000 word limit")
            summarized_conversation = self.summarize_long_conversation(formatted_conversation)
            logger.info(f"Using summarized conversation ({len(summarized_conversation.split())} words) for topic extraction")
            # Use the summarized conversation for topic extraction
            conversation_for_extraction = summarized_conversation
        else:
            logger.info(f"Document {document_id} conversation has {word_count} words, under limit. Proceeding with direct topic extraction.")
            # Use the original conversation for topic extraction
            conversation_for_extraction = formatted_conversation

        # Format the prompt with the conversation to be analyzed
        prompt = self.prompt_template.format(chat_conversation=conversation_for_extraction)

        try:
            # Use the retry-enabled function
            response = self._execute_ollama_request(conversation_for_extraction, prompt)

            # Extract the response text
            response_text = response['response'].strip()

            # Thread-safe file writing
            with self.response_file_lock:
                with open('mongodb_chat_responses.txt', 'a', encoding='utf-8') as file:
                    file.write(f"\n\n--- DOCUMENT ID: {document_id} ---\n\n")
                    file.write(f"ORIGINAL CONVERSATION:\n{formatted_conversation}\n\n")
                    if summarized_conversation:
                        file.write(f"SUMMARIZED CONVERSATION:\n{summarized_conversation}\n\n")
                    file.write(f"LLM RESPONSE:\n{response_text}\n")

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
                "summarized_conversation": summarized_conversation
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
                "summarized_conversation": None
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
        
        # Add summarized conversation if it exists
        if topic_data.get("summarized_conversation"):
            update_data["summarized_conversation"] = topic_data["summarized_conversation"]
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
            cleaned_segments = doc.get("cleaned_segments", [])
            
            # Skip if cleaned_segments is empty
            if not cleaned_segments or len(cleaned_segments) == 0:
                logger.info(f"Skipping document {document_id} - empty cleaned_segments")
                return False, document_id, "Empty cleaned_segments"
            
            logger.info(f"Processing document {document_id} ({current_index + 1}/{total_count}) with {len(cleaned_segments)} segments [Thread: {threading.current_thread().name}]")
            
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
                            "summarized_conversation": None
                        }
                        self.update_document_with_topics(document_id, error_topic_data)
                        return False, document_id, "API unresponsive"
            
            # Process with retry mechanism
            max_attempts = 3
            topic_data = None
            last_error = None
            
            for attempt in range(max_attempts):
                try:
                    topic_data = self.extract_topics_from_chat(cleaned_segments, document_id)
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
                            "summarized_conversation": None
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
        Generate and save statistics about the processed chat conversations
        """
        try:
            # Get all processed documents
            processed_docs = list(self.collection.find({"dominant_topic": {"$exists": True, "$ne": None}}))
            
            if not processed_docs:
                logger.warning("No processed documents found for statistics")
                return
            
            # Count unique topics
            unique_topics = len(set(doc.get("dominant_topic", "") for doc in processed_docs))
            
            # Count urgent conversations
            urgent_count = sum(1 for doc in processed_docs if doc.get("urgency", False))
            urgent_percentage = (urgent_count / len(processed_docs)) * 100
            
            # Count summarized conversations
            summarized_count = sum(1 for doc in processed_docs if doc.get("was_summarized", False))
            
            # Count conversations by number of segments
            segment_counts = []
            for doc in processed_docs:
                segments = doc.get("cleaned_segments", [])
                if segments:
                    segment_counts.append(len(segments))
            
            avg_segments = sum(segment_counts) / len(segment_counts) if segment_counts else 0
            
            # Log statistics
            logger.info(f"Statistics: Found {unique_topics} unique dominant topics")
            logger.info(f"Statistics: {urgent_count} conversations marked as urgent ({urgent_percentage:.1f}%)")
            logger.info(f"Statistics: {summarized_count} conversations were summarized")
            logger.info(f"Statistics: Average conversation length: {avg_segments:.1f} segments")
            
            # Save statistics to file
            with open('mongodb_chat_analysis_statistics.txt', 'w', encoding='utf-8') as stats_file:
                stats_file.write(f"# MongoDB Chat Conversation Topic Analysis Statistics\n")
                stats_file.write(f"# Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                stats_file.write(f"# Database: {self.db_name}\n")
                stats_file.write(f"# Collection: {self.collection_name}\n")
                stats_file.write(f"# Model: {self.model_name}\n")
                stats_file.write(f"# Remote Endpoint: {self.base_url}\n")
                stats_file.write(f"# Parallel Workers: {self.max_workers}\n\n")
                stats_file.write(f"Total processed conversations: {len(processed_docs)}\n")
                stats_file.write(f"Unique dominant topics: {unique_topics}\n")
                stats_file.write(f"Conversations marked as urgent: {urgent_count} ({urgent_percentage:.1f}%)\n")
                stats_file.write(f"Conversations that were summarized: {summarized_count}\n")
                stats_file.write(f"Average conversation length: {avg_segments:.1f} segments\n")
                
                if segment_counts:
                    stats_file.write(f"Shortest conversation: {min(segment_counts)} segments\n")
                    stats_file.write(f"Longest conversation: {max(segment_counts)} segments\n")
                
                # Add most common topics
                topic_counts = Counter(doc.get("dominant_topic", "") for doc in processed_docs)
                stats_file.write("\n## Most Common Dominant Topics:\n")
                for topic, count in topic_counts.most_common(10):
                    if topic and topic not in ["Processing Error", "API Error", "Empty Conversation", "Invalid Conversation"]:  # Skip error topics
                        stats_file.write(f"- {topic}: {count} conversations\n")
                
                # Add most common subtopics
                stats_file.write("\n## Most Common Subtopics:\n")
                all_subtopics = []
                for doc in processed_docs:
                    subtopics_str = doc.get("subtopics", "")
                    if subtopics_str and subtopics_str not in ["No subtopics identified", "Processing Error"]:
                        subtopics = [s.strip() for s in subtopics_str.split(',')]
                        all_subtopics.extend(subtopics)
                
                subtopic_counts = Counter(all_subtopics)
                for subtopic, count in subtopic_counts.most_common(10):
                    if subtopic:  # Skip empty subtopics
                        stats_file.write(f"- {subtopic}: {count} occurrences\n")
                
                # Add conversation length distribution
                stats_file.write("\n## Conversation Length Distribution:\n")
                if segment_counts:
                    length_distribution = Counter(segment_counts)
                    for length, count in sorted(length_distribution.items()):
                        stats_file.write(f"- {length} segments: {count} conversations\n")
            
            logger.info("Statistics report generated in 'mongodb_chat_analysis_statistics.txt'")
            
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
    Main function to run the MongoDB chat topic extraction process
    """
    processor = None
    
    try:
        # Initialize the processor with parallel processing
        logger.info("Starting MongoDB Chat Topic Extraction Process with Parallel Processing")
        
        # You can adjust max_workers based on your system and API limits
        # Recommended: 3-8 workers for most setups
        max_workers = int(os.getenv("MAX_WORKERS", "5"))
        logger.info(f"Using {max_workers} parallel workers")
        
        processor = MongoDBChatTopicExtractionProcessor(max_workers=max_workers)
        
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