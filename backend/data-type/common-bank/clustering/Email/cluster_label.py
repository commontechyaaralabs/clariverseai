import asyncio
import json
import logging
import re
import time
import warnings
import os
from datetime import datetime
from typing import Any, Dict, List, Set
from collections import defaultdict
import numpy as np
import ollama
import requests
from bson import ObjectId
from pymongo import MongoClient
from pymongo.database import Database
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Suppress warnings
warnings.filterwarnings("ignore")

# Ollama client configuration
client = ollama.Client(host="http://localhost:11434")

# Banking subcluster analysis prompt template with uniqueness enforcement
SUBCLUSTER_PROMPT = """
# Banking Subcluster Analysis System with Uniqueness Enforcement

You are an Expert Banking Topic Clustering Analyst specializing in creating UNIQUE, GRANULAR keyphrase clusters for financial institutions.

## EXISTING LABELS TO AVOID (CRITICAL)

EXISTING DOMINANT LABELS:
{existing_dominant_labels}

EXISTING SUBCLUSTER LABELS:
{existing_subcluster_labels}

## TASK DESCRIPTION

Analyze banking keyphrases from cluster ID {cluster_id} and create UNIQUE subclusters.

## INPUT KEYPHRASES

{keyphrases}

## CRITICAL REQUIREMENTS

1. **ABSOLUTE UNIQUENESS**: Every label must be different from existing labels
2. **INCLUDE ALL KEYPHRASES**: Every single keyphrase must appear exactly once
3. **GRANULAR SPECIFICITY**: Use specific banking terminology
4. **JSON FORMAT ONLY**: Respond with valid JSON only, no explanations

## OUTPUT FORMAT - RESPOND WITH VALID JSON ONLY

{{
  "Mortgage Loan Processing System APIs": {{
    "keyphrases": ["keyphrase1", "keyphrase2"]
  }},
  "Credit Risk Assessment ML Models": {{
    "keyphrases": ["keyphrase3", "keyphrase4"]
  }}
}}

## VERIFICATION

Total input keyphrases: {keyphrase_count}
Ensure all {keyphrase_count} keyphrases appear in your output exactly once.

RESPOND WITH VALID JSON ONLY - NO EXPLANATIONS OR ADDITIONAL TEXT.
"""

# Banking dominant topic prompt template with uniqueness enforcement
DOMINANT_TOPIC_PROMPT = """
You are an Expert Banking Cluster Labeling Analyst focused on creating UNIQUE, GRANULAR dominant labels for banking clusters.

## EXISTING LABELS TO AVOID (CRITICAL)

The following dominant labels have already been used and MUST NOT be duplicated:

{existing_dominant_labels}

## INSTRUCTIONS

- Create a HIGHLY SPECIFIC, GRANULAR banking label using EXACTLY 4-5 WORDS
- Must be COMPLETELY UNIQUE from all existing labels
- Focus on specific banking products, systems, processes, or technologies
- Include technical terminology, specific system names, regulation codes
- Use precise banking industry terminology and avoid generic terms

## UNIQUENESS STRATEGIES

- Reference specific banking platforms, technologies, or frameworks
- Include particular regulatory frameworks, compliance standards
- Use exact product names, service types, or operational processes
- Reference specific customer segments, market types, or geographical focus
- Include technical specifications, system architectures, or process workflows

## GRANULARITY EXAMPLES

Instead of generic terms, use specific ones:
- "Digital Banking Platform" → "Core Banking API Integration Framework"
- "Loan Services" → "Commercial Mortgage Underwriting Automation System"
- "Compliance Management" → "FFIEC Cybersecurity Assessment Implementation Protocol"
- "Risk Assessment" → "Credit Portfolio Monte Carlo Risk Simulation"
- "Customer Service" → "Omnichannel Client Relationship Management Platform"

## OUTPUT FORMAT

Provide EXACTLY ONE LINE containing ONLY the 4-5 word unique banking label, with no additional text.

## RULES

1. Label MUST be 4-5 WORDS
2. Must be COMPLETELY UNIQUE from existing labels
3. Must be HIGHLY GRANULAR and SPECIFIC
4. NO explanations or additional text
5. Focus on technical banking terminology
6. Include specific system/product/process names when possible

## BANKING KEYPHRASES TO ANALYZE

{keyphrases}
"""


class UniqueBankingClusterLabeler:
    def __init__(self, db: Database):
        """
        Initialize the unique banking cluster labeling processor
        
        :param db: MongoDB database connection
        """
        self.db = db
        self.model_name = "gemma3:27b"
        self.max_retries = 5
        self.used_dominant_labels: Set[str] = set()
        self.used_subcluster_labels: Set[str] = set()
        self.headers = {
            "Authorization": f"Bearer a2a78f42fbe58ce99fe0e3fec1726748ac434a16ea5cbfa9c1994979df874a0c"
        }

    def _create_indexes(self):
        """Create necessary indexes on collections"""
        try:
            # Create indexes on cluster collection
            self.db["cluster"].create_index("cluster_id")
            self.db["cluster"].create_index("dominant_label")
            
            # Create indexes on emailmessages collection
            self.db["emailmessages"].create_index("kmeans_cluster_keyphrase.cluster_id")
            self.db["emailmessages"].create_index("kmeans_cluster_keyphrase.label")
            
            logger.info("Database indexes created successfully")
        except Exception as e:
            logger.error(f"Error creating indexes: {e}")

    def _load_existing_labels(self):
        """Load existing labels from database to avoid duplications"""
        try:
            # Load existing dominant labels
            dominant_labels = self.db["cluster"].distinct("dominant_label")
            self.used_dominant_labels = set(label for label in dominant_labels if label)
            
            # Load existing subcluster labels
            subcluster_labels = set()
            clusters_with_subclusters = self.db["cluster"].find(
                {"subclusters": {"$exists": True}},
                {"subclusters": 1}
            )
            
            for cluster in clusters_with_subclusters:
                subclusters = cluster.get("subclusters", {})
                for subcluster_info in subclusters.values():
                    if isinstance(subcluster_info, dict) and "label" in subcluster_info:
                        subcluster_labels.add(subcluster_info["label"])
            
            self.used_subcluster_labels = subcluster_labels
            
            logger.info(f"Loaded {len(self.used_dominant_labels)} existing dominant labels")
            logger.info(f"Loaded {len(self.used_subcluster_labels)} existing subcluster labels")
            
        except Exception as e:
            logger.error(f"Error loading existing labels: {e}")
            self.used_dominant_labels = set()
            self.used_subcluster_labels = set()

    def _check_api_health(self):
        """Check if the Ollama API is responsive"""
        try:
            response = requests.get(
                "http://localhost:11434/api/tags",
                timeout=10,
                headers=self.headers
            )

            if response.status_code == 200:
                logger.info("Ollama API health check: OK")
                return True
            else:
                logger.warning(
                    f"Ollama API health check failed with status code: {response.status_code}"
                )
                return False
        except Exception as e:
            logger.error(f"Ollama API health check failed: {e}")
            return False

    def _recreate_ollama_client(self):
        """Recreate the Ollama client"""
        try:
            logger.info("Recreating Ollama client...")
            global client
            client = ollama.Client(host="http://localhost:11434")
            return True
        except Exception as e:
            logger.error(f"Failed to recreate Ollama client: {e}")
            return False

    @retry(
        retry=retry_if_exception_type(
            (
                requests.exceptions.RequestException,
                TimeoutError,
                ConnectionError,
                ollama.ResponseError,
            )
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        before_sleep=lambda retry_state: logger.info(
            f"API request failed, retrying in {retry_state.next_action.sleep} seconds... "
            f"(Attempt {retry_state.attempt_number}/{5})"
        ),
    )
    def _execute_ollama_request(self, prompt, model=None):
        """Execute Ollama API request with retry logic"""
        model_to_use = model if model else self.model_name

        if not self._check_api_health():
            logger.info("API seems to be down, waiting 15 seconds before retry...")
            time.sleep(15)
            if not self._check_api_health():
                if self._recreate_ollama_client():
                    logger.info("Ollama client recreated successfully")
                else:
                    logger.error("Failed to recreate client and API is still down")
                    raise ConnectionError(
                        "Ollama API is not responding after client recreation attempt"
                    )

        try:
            return client.generate(
                model=model_to_use,
                prompt=prompt,
                options={
                    "timeout_ms": 60000,
                    "num_ctx": 30000,
                    "num_predict": -1,
                },
            )
        except Exception as e:
            logger.error(f"Error during Ollama API call with model {model_to_use}: {e}")
            self._recreate_ollama_client()
            raise

    def extract_json_from_response(self, response_text):
        """Extract JSON from the LLM response with improved error handling"""
        logger.debug(f"Raw response text: {repr(response_text)}")
        
        # First, clean the response text
        cleaned_text = response_text.strip()
        
        # Remove any leading/trailing non-JSON content
        cleaned_text = re.sub(r'^[^{]*', '', cleaned_text)
        cleaned_text = re.sub(r'[^}]*$', '', cleaned_text)
        
        # Try to find JSON between triple backticks first
        json_match = re.search(r"```(?:json)?\s*([\s\S]*?)\s*```", cleaned_text)
        
        if json_match:
            json_str = json_match.group(1)
            logger.debug(f"Found JSON in code blocks: {repr(json_str)}")
        else:
            # Look for JSON structure without code blocks
            json_match = re.search(r'({[\s\S]*})', cleaned_text)
            if json_match:
                json_str = json_match.group(1)
                logger.debug(f"Found JSON structure: {repr(json_str)}")
            else:
                json_str = cleaned_text
                logger.debug(f"Using entire cleaned text: {repr(json_str)}")

        # Clean the JSON string
        json_str = json_str.strip()
        
        # Remove escaped newlines and extra whitespace
        json_str = json_str.replace('\\n', ' ').replace('\n', ' ').replace('\r', '')
        json_str = re.sub(r'\s+', ' ', json_str)
        
        # Remove any leading/trailing quotes or escape characters
        json_str = json_str.strip('"\'\\')
        
        # Ensure it starts and ends with braces
        if not json_str.startswith('{'):
            brace_start = json_str.find('{')
            if brace_start != -1:
                json_str = json_str[brace_start:]
        
        if not json_str.endswith('}'):
            brace_end = json_str.rfind('}')
            if brace_end != -1:
                json_str = json_str[:brace_end + 1]

        logger.debug(f"Cleaned JSON string: {repr(json_str)}")

        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON: {e}")
            logger.error(f"Problematic JSON string: {repr(json_str)}")

            # Try more aggressive fixes
            try:
                # Replace single quotes with double quotes
                fixed_json = json_str.replace("'", '"')
                
                # Fix unquoted keys
                fixed_json = re.sub(r'([{,]\s*)([a-zA-Z_][a-zA-Z0-9_\s]*?)(\s*):', r'\1"\2"\3:', fixed_json)
                
                # Remove trailing commas
                fixed_json = re.sub(r',\s*}', '}', fixed_json)
                fixed_json = re.sub(r',\s*]', ']', fixed_json)
                
                # Handle multiple spaces in keys
                fixed_json = re.sub(r'"([^"]*?)\s{2,}([^"]*?)"', r'"\1 \2"', fixed_json)
                
                # Try to fix common issues with nested structures
                fixed_json = re.sub(r':\s*{([^}]*?)}\s*,?\s*}', r': {\1}}', fixed_json)
                
                logger.debug(f"Fixed JSON attempt: {repr(fixed_json)}")
                
                parsed = json.loads(fixed_json)
                logger.info("Successfully parsed JSON after fixes")
                return parsed
                
            except json.JSONDecodeError as fix_error:
                logger.error(f"Failed to fix JSON issues: {fix_error}")
                
                # Last resort: try to extract key-value pairs manually
                try:
                    manual_dict = {}
                    # Look for quoted strings followed by arrays
                    pattern = r'"([^"]+)":\s*\{\s*"keyphrases":\s*\[(.*?)\]\s*\}'
                    matches = re.findall(pattern, json_str, re.DOTALL)
                    
                    for label, keyphrases_str in matches:
                        # Extract keyphrases from the array string
                        kp_matches = re.findall(r'"([^"]+)"', keyphrases_str)
                        manual_dict[label] = {"keyphrases": kp_matches}
                    
                    if manual_dict:
                        logger.info(f"Manually extracted {len(manual_dict)} subclusters")
                        return manual_dict
                        
                except Exception as manual_error:
                    logger.error(f"Manual extraction failed: {manual_error}")
                
                logger.warning("Returning empty dict as fallback")
                return {}

    def _validate_label_uniqueness(self, label, label_type="dominant"):
        """Validate that a label is unique"""
        if label_type == "dominant":
            return label.lower().strip() not in {l.lower().strip() for l in self.used_dominant_labels}
        else:  # subcluster
            return label.lower().strip() not in {l.lower().strip() for l in self.used_subcluster_labels}

    def _ensure_unique_dominant_label(self, keyphrases, max_attempts=5):
        """Generate a unique dominant label with multiple attempts if needed"""
        existing_labels_str = "\n".join([f"- {label}" for label in sorted(self.used_dominant_labels)])
        
        for attempt in range(max_attempts):
            try:
                formatted_keyphrases = "\n".join([f"- {phrase}" for phrase in keyphrases])
                
                prompt = DOMINANT_TOPIC_PROMPT.format(
                    existing_dominant_labels=existing_labels_str,
                    keyphrases=formatted_keyphrases
                )
                
                response = self._execute_ollama_request(prompt, model=self.model_name)
                dominant_topic = response["response"].strip()
                
                # Clean the response
                dominant_topic = re.sub(r"^[^a-zA-Z0-9]+", "", dominant_topic)
                dominant_topic = re.sub(r"[^a-zA-Z0-9]+$", "", dominant_topic)
                dominant_topic = dominant_topic.strip("\"'")
                
                # Validate uniqueness
                if self._validate_label_uniqueness(dominant_topic, "dominant"):
                    logger.info(f"Generated unique dominant topic (attempt {attempt + 1}): {dominant_topic}")
                    return dominant_topic
                else:
                    logger.warning(f"Generated duplicate dominant label (attempt {attempt + 1}): {dominant_topic}")
                    if attempt < max_attempts - 1:
                        # Add the duplicate to the existing labels for the next attempt
                        existing_labels_str += f"\n- {dominant_topic}"
                        
            except Exception as e:
                logger.error(f"Error generating dominant topic (attempt {attempt + 1}): {e}")
        
        # Fallback with timestamp if all attempts fail
        fallback_label = f"Banking Cluster {int(time.time())}"
        logger.warning(f"Using fallback dominant label: {fallback_label}")
        return fallback_label

    async def analyze_subclusters(self, cluster_id, keyphrases, max_attempts=5):
        """Analyze keyphrases within a cluster to create unique subclusters"""
        existing_dominant_str = "\n".join([f"- {label}" for label in sorted(self.used_dominant_labels)])
        existing_subcluster_str = "\n".join([f"- {label}" for label in sorted(self.used_subcluster_labels)])
        
        for attempt in range(max_attempts):
            try:
                formatted_keyphrases = "\n".join([f"- {phrase}" for phrase in keyphrases])
                keyphrase_count = len(keyphrases)

                prompt = SUBCLUSTER_PROMPT.format(
                    cluster_id=cluster_id,
                    keyphrases=formatted_keyphrases,
                    keyphrase_count=keyphrase_count,
                    existing_dominant_labels=existing_dominant_str,
                    existing_subcluster_labels=existing_subcluster_str
                )

                logger.debug(f"Sending prompt for cluster {cluster_id}, attempt {attempt + 1}")
                response = self._execute_ollama_request(prompt)
                response_text = response["response"].strip()
                
                logger.debug(f"Raw response for cluster {cluster_id}, attempt {attempt + 1}: {repr(response_text[:200])}")
                
                # Check if response looks like it contains valid content
                if not response_text or len(response_text.strip()) < 10:
                    logger.warning(f"Empty or too short response for cluster {cluster_id}, attempt {attempt + 1}")
                    continue
                
                subclusters = self.extract_json_from_response(response_text)

                if not subclusters:
                    logger.warning(f"Failed to extract valid JSON for cluster {cluster_id} (attempt {attempt + 1})")
                    if attempt < max_attempts - 1:
                        logger.info(f"Retrying with simplified prompt...")
                        # Add delay before retry
                        await asyncio.sleep(2)
                        continue
                    else:
                        logger.error(f"All attempts failed for cluster {cluster_id}")
                        break
                
                # Validate that we have the expected structure
                valid_subclusters = {}
                for label, content in subclusters.items():
                    if isinstance(content, dict) and "keyphrases" in content:
                        keyphrases_list = content["keyphrases"]
                    elif isinstance(content, list):
                        keyphrases_list = content
                    else:
                        logger.warning(f"Invalid subcluster structure for label '{label}': {content}")
                        continue
                    
                    if isinstance(keyphrases_list, list) and all(isinstance(kp, str) for kp in keyphrases_list):
                        valid_subclusters[label] = {"keyphrases": keyphrases_list}
                    else:
                        logger.warning(f"Invalid keyphrases structure for label '{label}': {keyphrases_list}")
                
                if not valid_subclusters:
                    logger.warning(f"No valid subclusters extracted for cluster {cluster_id} (attempt {attempt + 1})")
                    continue
                
                # Validate uniqueness of all subcluster labels
                all_labels_unique = True
                duplicate_labels = []
                
                for label in valid_subclusters.keys():
                    if not self._validate_label_uniqueness(label, "subcluster"):
                        all_labels_unique = False
                        duplicate_labels.append(label)
                
                if all_labels_unique:
                    logger.info(f"Generated {len(valid_subclusters)} unique subclusters for cluster {cluster_id} (attempt {attempt + 1})")
                    return valid_subclusters
                else:
                    logger.warning(f"Found duplicate subcluster labels (attempt {attempt + 1}): {duplicate_labels}")
                    if attempt < max_attempts - 1:
                        # Add duplicates to existing labels for next attempt
                        for dup_label in duplicate_labels:
                            existing_subcluster_str += f"\n- {dup_label}"
                        # Add delay before retry
                        await asyncio.sleep(2)

            except Exception as e:
                logger.error(f"Error analyzing subclusters for cluster {cluster_id} (attempt {attempt + 1}): {e}")
                if attempt < max_attempts - 1:
                    logger.info(f"Waiting before retry...")
                    await asyncio.sleep(3)
        
        # Fallback if all attempts fail
        logger.warning(f"Using fallback subclusters for cluster {cluster_id}")
        fallback_subclusters = self._create_fallback_subclusters(cluster_id, keyphrases)
        return fallback_subclusters

    def _create_fallback_subclusters(self, cluster_id, keyphrases):
        """Create fallback subclusters when LLM generation fails"""
        fallback_subclusters = {}
        timestamp = int(time.time())
        
        # Create 2-4 subclusters based on keyphrase count
        num_subclusters = min(4, max(2, len(keyphrases) // 3))
        chunk_size = max(1, len(keyphrases) // num_subclusters)
        
        for i in range(num_subclusters):
            chunk_start = i * chunk_size
            chunk_end = chunk_start + chunk_size if i < num_subclusters - 1 else len(keyphrases)
            chunk_keyphrases = keyphrases[chunk_start:chunk_end]
            
            # Create unique fallback label
            fallback_label = f"Banking Operations Subset {timestamp}_{cluster_id}_{i+1}"
            
            # Ensure this fallback label is unique
            counter = 1
            original_label = fallback_label
            while not self._validate_label_uniqueness(fallback_label, "subcluster"):
                fallback_label = f"{original_label}_v{counter}"
                counter += 1
            
            fallback_subclusters[fallback_label] = {"keyphrases": chunk_keyphrases}
            self.used_subcluster_labels.add(fallback_label)
        
        logger.info(f"Created {len(fallback_subclusters)} fallback subclusters for cluster {cluster_id}")
        return fallback_subclusters

    def collect_cluster_data(self):
        """Collect and organize cluster data from the cluster collection"""
        try:
            logger.info("Collecting cluster data from cluster collection...")
            
            # Get all clusters from the cluster collection
            clusters = list(self.db["cluster"].find({}))
            logger.info(f"Found {len(clusters)} clusters in collection")
            
            cluster_data = {}
            
            for cluster in clusters:
                cluster_id = cluster.get("cluster_id")
                keyphrases = cluster.get("keyphrases", [])
                
                if cluster_id is not None and keyphrases:
                    # Filter out empty keyphrases
                    keyphrases = [kp for kp in keyphrases if kp and str(kp).strip()]
                    
                    if keyphrases:
                        cluster_data[cluster_id] = {
                            "keyphrases": keyphrases,
                            "count": len(keyphrases),
                            "cluster_name": cluster.get("cluster_name", ""),
                            "_id": cluster.get("_id")
                        }
                        logger.info(f"Cluster {cluster_id}: {len(keyphrases)} keyphrases")
            
            logger.info(f"Successfully collected data for {len(cluster_data)} clusters")
            return cluster_data
            
        except Exception as e:
            logger.error(f"Error collecting cluster data: {e}")
            return {}

    async def update_emailmessages_with_labels(self):
        """Update emailmessages collection with dominant_label and subcluster labels"""
        try:
            logger.info("Starting to update emailmessages collection with labels...")
            
            # Get all clusters with their labels and subclusters
            clusters_with_labels = list(self.db["cluster"].find(
                {
                    "dominant_label": {"$exists": True},
                    "subclusters": {"$exists": True}
                },
                {
                    "cluster_id": 1,
                    "dominant_label": 1,
                    "subclusters": 1
                }
            ))
            
            logger.info(f"Found {len(clusters_with_labels)} labeled clusters")
            
            # Create mapping dictionaries
            cluster_to_dominant_label = {}
            keyphrase_to_subcluster_label = {}
            
            for cluster in clusters_with_labels:
                cluster_id = cluster["cluster_id"]
                dominant_label = cluster["dominant_label"]
                subclusters = cluster.get("subclusters", {})
                
                # Map cluster_id to dominant_label
                cluster_to_dominant_label[cluster_id] = dominant_label
                
                # Map each keyphrase to its subcluster label
                for subcluster_info in subclusters.values():
                    if isinstance(subcluster_info, dict):
                        subcluster_label = subcluster_info.get("label", "")
                        keyphrases = subcluster_info.get("keyphrases", [])
                        
                        for keyphrase in keyphrases:
                            if keyphrase:
                                keyphrase_to_subcluster_label[keyphrase] = subcluster_label
            
            logger.info(f"Created mappings for {len(cluster_to_dominant_label)} clusters and {len(keyphrase_to_subcluster_label)} keyphrases")
            
            # Update emailmessages collection
            updated_count = 0
            error_count = 0
            
            # Process emailmessages in batches
            batch_size = 1000
            total_messages = self.db["emailmessages"].count_documents({})
            logger.info(f"Processing {total_messages} email messages in batches of {batch_size}")
            
            for skip in range(0, total_messages, batch_size):
                try:
                    # Get batch of email messages
                    email_messages = list(self.db["emailmessages"].find(
                        {},
                        {
                            "_id": 1,
                            "kmeans_cluster_keyphrase": 1,
                            "kmeans_cluster_id": 1  # Include kmeans_cluster_id for mapping
                        }
                    ).skip(skip).limit(batch_size))
                    
                    bulk_operations = []
                    
                    for email_msg in email_messages:
                        try:
                            email_id = email_msg["_id"]
                            kmeans_cluster_keyphrase = email_msg.get("kmeans_cluster_keyphrase", {})
                            kmeans_cluster_id = email_msg.get("kmeans_cluster_id")
                            
                            if not kmeans_cluster_keyphrase or kmeans_cluster_id is None:
                                continue
                            
                            # Get keyphrase and cluster_id from kmeans_cluster_keyphrase
                            keyphrase_label = kmeans_cluster_keyphrase.get("label", "")
                            cluster_id = kmeans_cluster_id
                            
                            # Prepare update document
                            update_doc = {}
                            
                            # Map dominant_label based on kmeans_cluster_id
                            if cluster_id in cluster_to_dominant_label:
                                update_doc["kmeans_cluster_keyphrase.dominant_label"] = cluster_to_dominant_label[cluster_id]
                            
                            # Map subcluster_label based on keyphrase_label
                            if keyphrase_label in keyphrase_to_subcluster_label:
                                update_doc["kmeans_cluster_keyphrase.subcluster_label"] = keyphrase_to_subcluster_label[keyphrase_label]
                            
                            # Only update if we have something to update
                            if update_doc:
                                bulk_operations.append({
                                    "updateOne": {
                                        "filter": {"_id": email_id},
                                        "update": {"$set": update_doc}
                                    }
                                })
                        
                        except Exception as e:
                            logger.error(f"Error processing email message {email_msg.get('_id', 'unknown')}: {e}")
                            error_count += 1
                            continue
                    
                    # Execute bulk operations
                    if bulk_operations:
                        try:
                            result = self.db["emailmessages"].bulk_write(bulk_operations, ordered=False)
                            updated_count += result.modified_count
                            logger.info(f"Updated {result.modified_count} messages in batch {skip//batch_size + 1}")
                        except Exception as e:
                            logger.error(f"Error executing bulk operations for batch {skip//batch_size + 1}: {e}")
                            error_count += len(bulk_operations)
                
                except Exception as e:
                    logger.error(f"Error processing batch {skip//batch_size + 1}: {e}")
                    error_count += batch_size
                    continue
            
            logger.info(f"Completed updating emailmessages collection. Updated: {updated_count}, Errors: {error_count}")
            
            return {
                "status": "success",
                "total_messages_processed": total_messages,
                "messages_updated": updated_count,
                "errors": error_count,
                "clusters_mapped": len(cluster_to_dominant_label),
                "keyphrases_mapped": len(keyphrase_to_subcluster_label)
            }
            
        except Exception as e:
            logger.error(f"Error updating emailmessages collection: {e}")
            return {"status": "error", "message": str(e)}

    async def process_all_clusters(self):
        """Process all clusters to generate unique labels and subclusters"""
        try:
            start_time = time.time()
            
            # Create indexes
            self._create_indexes()
            
            # Load existing labels to ensure uniqueness
            self._load_existing_labels()
            
            # Collect cluster data
            cluster_data = self.collect_cluster_data()
            
            if not cluster_data:
                return {
                    "status": "error",
                    "message": "No cluster data found"
                }
            
            processed_count = 0
            skipped_count = 0
            
            for cluster_id, data in cluster_data.items():
                try:
                    keyphrases = data["keyphrases"]
                    cluster_object_id = data["_id"]
                    
                    # Check if already processed (has both dominant_label and subclusters)
                    existing_cluster = self.db["cluster"].find_one({"cluster_id": cluster_id})
                    
                    if (existing_cluster and 
                        "dominant_label" in existing_cluster and 
                        "subclusters" in existing_cluster):
                        logger.info(f"Skipping already processed cluster {cluster_id}")
                        # Add existing labels to our tracking sets
                        if existing_cluster.get("dominant_label"):
                            self.used_dominant_labels.add(existing_cluster["dominant_label"])
                        
                        subclusters = existing_cluster.get("subclusters", {})
                        for subcluster_info in subclusters.values():
                            if isinstance(subcluster_info, dict) and "label" in subcluster_info:
                                self.used_subcluster_labels.add(subcluster_info["label"])
                        
                        skipped_count += 1
                        continue
                    
                    logger.info(f"Processing cluster {cluster_id} with {len(keyphrases)} keyphrases")
                    
                    # Generate unique dominant topic
                    dominant_topic = self._ensure_unique_dominant_label(keyphrases)
                    self.used_dominant_labels.add(dominant_topic)
                    
                    # Analyze unique subclusters
                    subclusters = await self.analyze_subclusters(cluster_id, keyphrases)
                    
                    if "error" in subclusters:
                        logger.error(f"Error in subcluster analysis for cluster {cluster_id}: {subclusters['error']}")
                        continue
                    
                    # Add new subcluster labels to tracking set
                    for label in subclusters.keys():
                        self.used_subcluster_labels.add(label)
                    
                    # Validate all keyphrases are included
                    original_keyphrases_set = set(keyphrases)
                    all_subcluster_keyphrases = set()
                    
                    for label, content in subclusters.items():
                        if isinstance(content, dict):
                            all_subcluster_keyphrases.update(set(content.get("keyphrases", [])))
                        elif isinstance(content, list):
                            all_subcluster_keyphrases.update(set(content))
                    
                    missing_keyphrases = original_keyphrases_set - all_subcluster_keyphrases
                    
                    # If there are missing keyphrases, add them to a fallback subcluster
                    if missing_keyphrases:
                        logger.warning(f"Found {len(missing_keyphrases)} missing keyphrases in cluster {cluster_id}")
                        fallback_label = f"Additional Banking Operations {int(time.time())}"
                        subclusters[fallback_label] = {"keyphrases": list(missing_keyphrases)}
                        self.used_subcluster_labels.add(fallback_label)
                    
                    # Format subclusters for storage
                    formatted_subclusters = {}
                    for subcluster_idx, (label, content) in enumerate(subclusters.items()):
                        if isinstance(content, dict):
                            keyphrases_list = content.get("keyphrases", [])
                        elif isinstance(content, list):
                            keyphrases_list = content
                        else:
                            keyphrases_list = []
                        
                        formatted_subclusters[str(subcluster_idx)] = {
                            "label": label,
                            "keyphrases": keyphrases_list
                        }
                    
                    # Update the cluster document
                    update_doc = {
                        "dominant_label": dominant_topic,
                        "subclusters": formatted_subclusters,
                        "original_keyphrases_count": len(keyphrases),
                        "processing_date": datetime.now().isoformat(),
                        "uniqueness_validated": True
                    }
                    
                    # Update cluster collection
                    self.db["cluster"].update_one(
                        {"cluster_id": cluster_id},
                        {"$set": update_doc}
                    )
                    
                    processed_count += 1
                    logger.info(f"Successfully processed cluster {cluster_id} as '{dominant_topic}'")
                    
                    # Small delay to avoid overwhelming the API
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"Error processing cluster {cluster_id}: {e}")
                    continue
            
            # Update emailmessages collection with labels
            logger.info("Updating emailmessages collection with labels...")
            email_update_result = await self.update_emailmessages_with_labels()
            logger.info(f"Email messages update result: {email_update_result}")
            
            end_time = time.time()
            execution_time = end_time - start_time
            
            logger.info(f"Processing completed in {execution_time:.2f} seconds")
            
            return {
                "status": "success",
                "total_clusters": len(cluster_data),
                "processed_clusters": processed_count,
                "skipped_clusters": skipped_count,
                "execution_time_seconds": execution_time,
                "unique_dominant_labels": len(self.used_dominant_labels),
                "unique_subcluster_labels": len(self.used_subcluster_labels),
                "email_update_result": email_update_result
            }
            
        except Exception as e:
            logger.error(f"Error in process_all_clusters: {e}", exc_info=True)
            return {"status": "error", "message": str(e)}

    async def get_cluster_summary(self):
        """Get summary of processed clusters with uniqueness information"""
        try:
            # Count clusters with labels
            total_clusters = self.db["cluster"].count_documents({})
            labeled_clusters = self.db["cluster"].count_documents(
                {"dominant_label": {"$exists": True}}
            )
            sublabeled_clusters = self.db["cluster"].count_documents(
                {"subclusters": {"$exists": True}}
            )
            
            # Get unique label counts
            unique_dominant_labels = len(self.db["cluster"].distinct("dominant_label"))
            
            # Count unique subcluster labels
            unique_subcluster_labels = set()
            clusters_with_subclusters = self.db["cluster"].find(
                {"subclusters": {"$exists": True}},
                {"subclusters": 1}
            )
            
            for cluster in clusters_with_subclusters:
                subclusters = cluster.get("subclusters", {})
                for subcluster_info in subclusters.values():
                    if isinstance(subcluster_info, dict) and "label" in subcluster_info:
                        unique_subcluster_labels.add(subcluster_info["label"])
            
            # Check emailmessages collection statistics
            total_emails = self.db["emailmessages"].count_documents({})
            emails_with_dominant_labels = self.db["emailmessages"].count_documents({
                "kmeans_cluster_keyphrase.dominant_label": {"$exists": True}
            })
            emails_with_subcluster_labels = self.db["emailmessages"].count_documents({
                "kmeans_cluster_keyphrase.subcluster_label": {"$exists": True}
            })
            
            # Get sample of cluster labels
            sample_clusters = list(
                self.db["cluster"].find(
                    {"dominant_label": {"$exists": True}},
                    {"cluster_id": 1, "dominant_label": 1, "subclusters": 1}
                ).limit(10)
            )
            
            return {
                "status": "success",
                "cluster_statistics": {
                    "total_clusters": total_clusters,
                    "labeled_clusters": labeled_clusters,
                    "sublabeled_clusters": sublabeled_clusters,
                    "unique_dominant_labels_count": unique_dominant_labels,
                    "unique_subcluster_labels_count": len(unique_subcluster_labels)
                },
                "email_statistics": {
                    "total_emails": total_emails,
                    "emails_with_dominant_labels": emails_with_dominant_labels,
                    "emails_with_subcluster_labels": emails_with_subcluster_labels,
                    "dominant_label_coverage": (emails_with_dominant_labels / total_emails * 100) if total_emails > 0 else 0,
                    "subcluster_label_coverage": (emails_with_subcluster_labels / total_emails * 100) if total_emails > 0 else 0
                },
                "sample_clusters": sample_clusters
            }
            
        except Exception as e:
            logger.error(f"Error getting cluster summary: {e}")
            return {"status": "error", "message": str(e)}

    async def validate_label_uniqueness(self):
        """Validate that all labels in the database are unique"""
        try:
            # Check dominant label uniqueness
            dominant_labels = list(self.db["cluster"].find(
                {"dominant_label": {"$exists": True}},
                {"cluster_id": 1, "dominant_label": 1}
            ))
            
            dominant_label_counts = defaultdict(list)
            for cluster in dominant_labels:
                label = cluster["dominant_label"]
                dominant_label_counts[label].append(cluster["cluster_id"])
            
            dominant_duplicates = {
                label: cluster_ids for label, cluster_ids in dominant_label_counts.items()
                if len(cluster_ids) > 1
            }
            
            # Check subcluster label uniqueness
            clusters_with_subclusters = list(self.db["cluster"].find(
                {"subclusters": {"$exists": True}},
                {"cluster_id": 1, "subclusters": 1}
            ))
            
            subcluster_label_counts = defaultdict(list)
            for cluster in clusters_with_subclusters:
                cluster_id = cluster["cluster_id"]
                subclusters = cluster.get("subclusters", {})
                for subcluster_info in subclusters.values():
                    if isinstance(subcluster_info, dict) and "label" in subcluster_info:
                        label = subcluster_info["label"]
                        subcluster_label_counts[label].append(cluster_id)
            
            subcluster_duplicates = {
                label: cluster_ids for label, cluster_ids in subcluster_label_counts.items()
                if len(cluster_ids) > 1
            }
            
            return {
                "status": "success",
                "dominant_labels_total": len(dominant_label_counts),
                "dominant_duplicates": dominant_duplicates,
                "dominant_duplicates_count": len(dominant_duplicates),
                "subcluster_labels_total": len(subcluster_label_counts),
                "subcluster_duplicates": subcluster_duplicates,
                "subcluster_duplicates_count": len(subcluster_duplicates),
                "all_labels_unique": len(dominant_duplicates) == 0 and len(subcluster_duplicates) == 0
            }
            
        except Exception as e:
            logger.error(f"Error validating label uniqueness: {e}")
            return {"status": "error", "message": str(e)}

    async def regenerate_duplicate_labels(self):
        """Regenerate any duplicate labels found in the database"""
        try:
            logger.info("Checking for and regenerating duplicate labels...")
            
            # First validate current state
            validation_result = await self.validate_label_uniqueness()
            
            if validation_result["status"] != "success":
                return validation_result
            
            regenerated_count = 0
            
            # Handle dominant label duplicates
            dominant_duplicates = validation_result.get("dominant_duplicates", {})
            for duplicate_label, cluster_ids in dominant_duplicates.items():
                logger.warning(f"Found duplicate dominant label '{duplicate_label}' in clusters: {cluster_ids}")
                
                # Keep the first cluster with this label, regenerate for others
                for cluster_id in cluster_ids[1:]:
                    cluster_doc = self.db["cluster"].find_one({"cluster_id": cluster_id})
                    if cluster_doc:
                        keyphrases = cluster_doc.get("keyphrases", [])
                        if keyphrases:
                            # Remove the duplicate label from tracking
                            self.used_dominant_labels.discard(duplicate_label)
                            
                            # Generate new unique label
                            new_label = self._ensure_unique_dominant_label(keyphrases)
                            
                            # Update database
                            self.db["cluster"].update_one(
                                {"cluster_id": cluster_id},
                                {"$set": {
                                    "dominant_label": new_label,
                                    "regenerated_date": datetime.now().isoformat()
                                }}
                            )
                            
                            logger.info(f"Regenerated dominant label for cluster {cluster_id}: '{new_label}'")
                            regenerated_count += 1
            
            # Handle subcluster label duplicates
            subcluster_duplicates = validation_result.get("subcluster_duplicates", {})
            for duplicate_label, cluster_ids in subcluster_duplicates.items():
                logger.warning(f"Found duplicate subcluster label '{duplicate_label}' in clusters: {cluster_ids}")
                
                # Regenerate subclusters for all affected clusters except the first
                for cluster_id in cluster_ids[1:]:
                    cluster_doc = self.db["cluster"].find_one({"cluster_id": cluster_id})
                    if cluster_doc:
                        keyphrases = cluster_doc.get("keyphrases", [])
                        if keyphrases:
                            # Remove duplicate subcluster labels from tracking
                            self.used_subcluster_labels.discard(duplicate_label)
                            
                            # Regenerate subclusters
                            new_subclusters = await self.analyze_subclusters(cluster_id, keyphrases)
                            
                            # Format for storage
                            formatted_subclusters = {}
                            for idx, (label, content) in enumerate(new_subclusters.items()):
                                if isinstance(content, dict):
                                    keyphrases_list = content.get("keyphrases", [])
                                elif isinstance(content, list):
                                    keyphrases_list = content
                                else:
                                    keyphrases_list = []
                                
                                formatted_subclusters[str(idx)] = {
                                    "label": label,
                                    "keyphrases": keyphrases_list
                                }
                            
                            # Update database
                            self.db["cluster"].update_one(
                                {"cluster_id": cluster_id},
                                {"$set": {
                                    "subclusters": formatted_subclusters,
                                    "subclusters_regenerated_date": datetime.now().isoformat()
                                }}
                            )
                            
                            logger.info(f"Regenerated subclusters for cluster {cluster_id}")
                            regenerated_count += 1
            
            # If labels were regenerated, update emailmessages collection
            if regenerated_count > 0:
                logger.info("Updating emailmessages collection after label regeneration...")
                email_update_result = await self.update_emailmessages_with_labels()
                logger.info(f"Email messages update after regeneration: {email_update_result}")
            
            return {
                "status": "success",
                "regenerated_clusters": regenerated_count,
                "original_dominant_duplicates": len(dominant_duplicates),
                "original_subcluster_duplicates": len(subcluster_duplicates)
            }
            
        except Exception as e:
            logger.error(f"Error regenerating duplicate labels: {e}")
            return {"status": "error", "message": str(e)}

    async def validate_emailmessages_mapping(self):
        """Validate the mapping of labels in emailmessages collection"""
        try:
            logger.info("Validating emailmessages label mapping...")
            
            # Get sample of emailmessages with labels
            sample_emails = list(self.db["emailmessages"].find(
                {
                    "kmeans_cluster_keyphrase": {"$exists": True}
                },
                {
                    "_id": 1,
                    "kmeans_cluster_keyphrase": 1,
                    "kmeans_cluster_id": 1
                }
            ).limit(100))
            
            mapping_stats = {
                "total_sampled": len(sample_emails),
                "with_cluster_id": 0,
                "with_keyphrase_label": 0,
                "with_dominant_label": 0,
                "with_subcluster_label": 0,
                "mapping_errors": []
            }
            
            for email in sample_emails:
                kmeans_data = email.get("kmeans_cluster_keyphrase", {})
                
                if "cluster_id" in kmeans_data or email.get("kmeans_cluster_id"):
                    mapping_stats["with_cluster_id"] += 1
                
                if "label" in kmeans_data:
                    mapping_stats["with_keyphrase_label"] += 1
                
                if "dominant_label" in kmeans_data:
                    mapping_stats["with_dominant_label"] += 1
                
                if "subcluster_label" in kmeans_data:
                    mapping_stats["with_subcluster_label"] += 1
                
                # Validate mapping consistency
                cluster_id = email.get("kmeans_cluster_id")
                keyphrase_label = kmeans_data.get("label", "")
                dominant_label = kmeans_data.get("dominant_label", "")
                subcluster_label = kmeans_data.get("subcluster_label", "")
                
                if cluster_id is not None:
                    # Check if dominant_label matches cluster
                    cluster_doc = self.db["cluster"].find_one(
                        {"cluster_id": cluster_id},
                        {"dominant_label": 1, "subclusters": 1}
                    )
                    
                    if cluster_doc:
                        expected_dominant = cluster_doc.get("dominant_label", "")
                        if dominant_label and dominant_label != expected_dominant:
                            mapping_stats["mapping_errors"].append({
                                "email_id": str(email["_id"]),
                                "error_type": "dominant_label_mismatch",
                                "cluster_id": cluster_id,
                                "expected": expected_dominant,
                                "actual": dominant_label
                            })
                        
                        # Check subcluster label mapping
                        if keyphrase_label and subcluster_label:
                            subclusters = cluster_doc.get("subclusters", {})
                            expected_subcluster = None
                            
                            for subcluster_info in subclusters.values():
                                if isinstance(subcluster_info, dict):
                                    if keyphrase_label in subcluster_info.get("keyphrases", []):
                                        expected_subcluster = subcluster_info.get("label", "")
                                        break
                            
                            if expected_subcluster and subcluster_label != expected_subcluster:
                                mapping_stats["mapping_errors"].append({
                                    "email_id": str(email["_id"]),
                                    "error_type": "subcluster_label_mismatch",
                                    "keyphrase": keyphrase_label,
                                    "expected": expected_subcluster,
                                    "actual": subcluster_label
                                })
            
            return {
                "status": "success",
                "mapping_statistics": mapping_stats
            }
            
        except Exception as e:
            logger.error(f"Error validating emailmessages mapping: {e}")
            return {"status": "error", "message": str(e)}


async def main():
    """Main execution function"""
    try:
        # Get MongoDB connection details from environment variables
        mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")
        mongo_database_name = os.getenv("MONGO_DATABASE_NAME")
        
        if not mongo_connection_string or not mongo_database_name:
            logger.error("MongoDB connection string or database name not found in environment variables")
            return {"status": "error", "message": "Missing MongoDB configuration"}
        
        logger.info(f"Connecting to MongoDB database: {mongo_database_name}")
        
        # Connect to MongoDB using environment variables
        mongo_client = MongoClient(mongo_connection_string)
        
        # Test the connection
        try:
            mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return {"status": "error", "message": f"MongoDB connection failed: {str(e)}"}
        
        db = mongo_client[mongo_database_name]
        
        # Initialize unique labeler
        labeler = UniqueBankingClusterLabeler(db)
        
        # Process all clusters with unique labeling
        logger.info("Starting unique cluster processing...")
        result = await labeler.process_all_clusters()
        
        logger.info(f"Processing result: {result}")
        
        # Validate uniqueness after processing
        logger.info("Validating label uniqueness...")
        uniqueness_result = await labeler.validate_label_uniqueness()
        logger.info(f"Uniqueness validation: {uniqueness_result}")
        
        # If duplicates found, attempt to regenerate them
        if (uniqueness_result.get("dominant_duplicates_count", 0) > 0 or 
            uniqueness_result.get("subcluster_duplicates_count", 0) > 0):
            logger.info("Found duplicates, attempting regeneration...")
            regeneration_result = await labeler.regenerate_duplicate_labels()
            logger.info(f"Regeneration result: {regeneration_result}")
            
            # Re-validate after regeneration
            final_validation = await labeler.validate_label_uniqueness()
            logger.info(f"Final validation: {final_validation}")
        
        # Validate emailmessages mapping
        logger.info("Validating emailmessages label mapping...")
        mapping_validation = await labeler.validate_emailmessages_mapping()
        logger.info(f"Emailmessages mapping validation: {mapping_validation}")
        
        # Get final summary
        summary = await labeler.get_cluster_summary()
        logger.info(f"Final cluster summary: {summary}")
        
        # Close MongoDB connection
        mongo_client.close()
        
        return {
            "processing_result": result,
            "uniqueness_validation": uniqueness_result,
            "mapping_validation": mapping_validation,
            "final_summary": summary
        }
        
    except Exception as e:
        logger.error(f"Error in main execution: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}


async def analyze_label_distribution(db: Database):
    """Analyze the distribution and characteristics of generated labels"""
    try:
        # Get all dominant labels
        dominant_labels = list(db["cluster"].distinct("dominant_label"))
        
        # Get all subcluster labels
        subcluster_labels = set()
        clusters = db["cluster"].find({"subclusters": {"$exists": True}}, {"subclusters": 1})
        
        for cluster in clusters:
            subclusters = cluster.get("subclusters", {})
            for subcluster_info in subclusters.values():
                if isinstance(subcluster_info, dict) and "label" in subcluster_info:
                    subcluster_labels.add(subcluster_info["label"])
        
        # Analyze label characteristics
        dominant_word_counts = [len(label.split()) for label in dominant_labels if label]
        subcluster_word_counts = [len(label.split()) for label in subcluster_labels if label]
        
        # Find common terms
        all_words = []
        for label in dominant_labels + list(subcluster_labels):
            if label:
                all_words.extend(label.lower().split())
        
        word_frequency = defaultdict(int)
        for word in all_words:
            word_frequency[word] += 1
        
        common_words = sorted(word_frequency.items(), key=lambda x: x[1], reverse=True)[:20]
        
        return {
            "status": "success",
            "dominant_labels_count": len(dominant_labels),
            "subcluster_labels_count": len(subcluster_labels),
            "dominant_avg_word_count": np.mean(dominant_word_counts) if dominant_word_counts else 0,
            "subcluster_avg_word_count": np.mean(subcluster_word_counts) if subcluster_word_counts else 0,
            "most_common_words": common_words,
            "sample_dominant_labels": dominant_labels[:10],
            "sample_subcluster_labels": list(subcluster_labels)[:10]
        }
        
    except Exception as e:
        logger.error(f"Error analyzing label distribution: {e}")
        return {"status": "error", "message": str(e)}


async def export_cluster_labels(db: Database, output_file="cluster_labels_export.json"):
    """Export all cluster labels to a JSON file"""
    try:
        clusters = list(db["cluster"].find(
            {"dominant_label": {"$exists": True}},
            {"_id": 0, "cluster_id": 1, "cluster_name": 1, "dominant_label": 1, "subclusters": 1, "keyphrases": 1}
        ))
        
        export_data = {
            "export_date": datetime.now().isoformat(),
            "total_clusters": len(clusters),
            "clusters": clusters
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Exported {len(clusters)} clusters to {output_file}")
        
        return {
            "status": "success",
            "exported_clusters": len(clusters),
            "output_file": output_file
        }
        
    except Exception as e:
        logger.error(f"Error exporting cluster labels: {e}")
        return {"status": "error", "message": str(e)}


async def export_emailmessages_with_labels(db: Database, output_file="emailmessages_with_labels_export.json"):
    """Export emailmessages with their mapped labels"""
    try:
        # Get sample of emailmessages with labels
        emails_with_labels = list(db["emailmessages"].find(
            {
                "kmeans_cluster_keyphrase.dominant_label": {"$exists": True}
            },
            {
                "_id": 1,
                "subject": 1,
                "kmeans_cluster_keyphrase": 1,
                "kmeans_cluster_id": 1
            }
        ).limit(1000))  # Limit to prevent huge files
        
        export_data = {
            "export_date": datetime.now().isoformat(),
            "total_emails_exported": len(emails_with_labels),
            "emails": emails_with_labels
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
        
        logger.info(f"Exported {len(emails_with_labels)} email messages to {output_file}")
        
        return {
            "status": "success",
            "exported_emails": len(emails_with_labels),
            "output_file": output_file
        }
        
    except Exception as e:
        logger.error(f"Error exporting emailmessages with labels: {e}")
        return {"status": "error", "message": str(e)}


if __name__ == "__main__":
    # Run the async main function
    result = asyncio.run(main())
    print(f"Final result: {result}")
    
    # Optional: Run additional analysis
    try:
        mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")
        mongo_database_name = os.getenv("MONGO_DATABASE_NAME")
        
        if mongo_connection_string and mongo_database_name:
            mongo_client = MongoClient(mongo_connection_string)
            db = mongo_client[mongo_database_name]
            
            # Analyze label distribution
            distribution_result = asyncio.run(analyze_label_distribution(db))
            print(f"Label distribution analysis: {distribution_result}")
            
            # Export labels
            export_result = asyncio.run(export_cluster_labels(db))
            print(f"Export result: {export_result}")
            
            # Export emailmessages with labels
            email_export_result = asyncio.run(export_emailmessages_with_labels(db))
            print(f"Email export result: {email_export_result}")
            
            mongo_client.close()
            
    except Exception as e:
        logger.error(f"Error in additional analysis: {e}")