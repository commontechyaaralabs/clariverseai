import json
import requests
import random
import time
from typing import List, Dict
import os

class EuropeanBankTopicGenerator:
    def __init__(self):
        self.api_key = "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"
        # Using the specified model
        self.model = "google/gemma-3-27b-it:free"
        self.base_url = "https://openrouter.ai/api/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/your-repo",  # Optional: helps with rate limits
            "X-Title": "European Bank Topic Generator"  # Optional: for tracking
        }
        
        # Initialize progress file
        self.progress_file = "banking_topics_progress.json"
        self.final_file = "banking_topics_with_weights.json"
        self.initialize_progress_file()

    def test_model_availability(self) -> str:
        """Test if the specified model is available and working"""
        print(f"🔍 Testing model availability: {self.model}...")
        
        test_prompt = "Generate a simple JSON array with one banking topic."
        
        try:
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": test_prompt}],
                    "temperature": 0.7,
                    "max_tokens": 100
                },
                timeout=30
            )
            
            if response.status_code == 200:
                print(f"✅ Model {self.model} is available!")
                return self.model
            else:
                print(f"❌ Model {self.model} failed: {response.status_code}")
                print(f"Response: {response.text}")
                raise Exception(f"Model {self.model} is not available")
                
        except Exception as e:
            print(f"❌ Model {self.model} error: {e}")
            raise Exception(f"❌ Model {self.model} is not available!")

    def initialize_progress_file(self):
        """Initialize the progress tracking file"""
        initial_data = {
            "generation_started": time.strftime("%Y-%m-%d %H:%M:%S"),
            "target_topics": 0,
            "current_count": 0,
            "batches_completed": 0,
            "topics": [],
            "used_topic_names": [],
            "used_subtopic_names": [],
            "status": "initializing"
        }
        
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(initial_data, f, indent=2, ensure_ascii=False)
            print(f"📊 Progress tracking initialized: {self.progress_file}")
        except Exception as e:
            print(f"❌ Error initializing progress file: {e}")

    def update_progress_file(self, topics: List[Dict], used_topic_names: set, used_subtopic_names: set, 
                           batch_num: int, target: int, status: str = "generating"):
        """Update the progress file with current status"""
        progress_data = {
            "generation_started": time.strftime("%Y-%m-%d %H:%M:%S") if batch_num == 1 else self.get_start_time(),
            "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
            "target_topics": target,
            "current_count": len(topics),
            "batches_completed": batch_num,
            "topics": topics,
            "used_topic_names": list(used_topic_names),
            "used_subtopic_names": list(used_subtopic_names),
            "status": status,
            "completion_percentage": round((len(topics) / target) * 100, 2) if target > 0 else 0,
            "model_used": self.model
        }
        
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, indent=2, ensure_ascii=False)
            print(f"💾 Progress saved: {len(topics)}/{target} topics ({progress_data['completion_percentage']}%)")
        except Exception as e:
            print(f"❌ Error updating progress file: {e}")

    def get_start_time(self):
        """Get the start time from existing progress file"""
        try:
            with open(self.progress_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("generation_started", time.strftime("%Y-%m-%d %H:%M:%S"))
        except:
            return time.strftime("%Y-%m-%d %H:%M:%S")

    def clean_json_response(self, content: str) -> str:
        """Clean and extract JSON from API response"""
        # Remove common prefixes/suffixes
        content = content.strip()
        
        # Remove markdown code blocks
        if content.startswith('```json'):
            content = content[7:]
        elif content.startswith('```'):
            content = content[3:]
        
        if content.endswith('```'):
            content = content[:-3]
        
        # Find JSON array boundaries
        start = content.find('[')
        end = content.rfind(']') + 1
        
        if start != -1 and end != 0:
            return content[start:end].strip()
        
        return content.strip()

    def validate_topic_structure(self, topic: Dict) -> bool:
        """Validate that a topic has the correct structure"""
        required_fields = ['dominant_topic', 'subtopics', 'topic_weight']
        
        if not all(field in topic for field in required_fields):
            return False
        
        if not isinstance(topic['subtopics'], list) or len(topic['subtopics']) < 3:
            return False
        
        for subtopic in topic['subtopics']:
            if not isinstance(subtopic, dict) or 'name' not in subtopic or 'weight' not in subtopic:
                return False
        
        return True

    def generate_banking_topics_batch(self, batch_size: int = 20, used_topics: set = None, used_subtopics: set = None) -> List[Dict]:
        """Generate a batch of European banking topics with 3-7 subtopics each"""
        
        used_topics = used_topics or set()
        used_subtopics = used_subtopics or set()
        
        # Create exclusion lists for uniqueness
        topic_exclusions = ', '.join(f'"{topic}"' for topic in list(used_topics)[:20])
        subtopic_exclusions = ', '.join(f'"{subtopic}"' for subtopic in list(used_subtopics)[:30])
        
        prompt = f"""You are an Expert Topic Modeling Analyst for European Banking. Generate exactly {batch_size} unique banking topics in valid JSON format.

STRICT REQUIREMENTS:
- Each topic: Maximum 3 words, specific to European banking
- Each subtopic: Maximum 3 words, granular and precise  
- 3-7 subtopics per topic
- Subtopic weights: 0.05 to 0.25 each
- Topic weight = sum of subtopic weights
- ALL names must be completely unique

AVOID THESE USED TOPICS: {topic_exclusions}
AVOID THESE USED SUBTOPICS: {subtopic_exclusions}

EUROPEAN BANKING FOCUS AREAS:
- SEPA Operations: instant payments, direct debits, cross-border transfers
- EU Compliance: GDPR, MiFID II, PSD2, AML5, Basel III
- Euro Banking: TARGET2, ECB operations, euro clearing
- Digital Banking: Open Banking APIs, mobile authentication
- Risk Management: stress testing, capital requirements
- Trade Finance: letters of credit, trade documentation

OUTPUT ONLY VALID JSON:
[
  {{
    "dominant_topic": "SEPA Instant Rejection",
    "subtopics": [
      {{"name": "Timeout Error", "weight": 0.08}},
      {{"name": "Bank Unavailable", "weight": 0.12}},
      {{"name": "Amount Exceeded", "weight": 0.09}},
      {{"name": "Fraud Block", "weight": 0.11}}
    ],
    "topic_weight": 0.40
  }}
]

Generate {batch_size} unique European banking topics. Return only the JSON array."""

        try:
            print(f"🚀 Calling API with model: {self.model}")
            response = requests.post(
                self.base_url,
                headers=self.headers,
                json={
                    "model": self.model,
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.8,
                    "max_tokens": 4000,
                    "top_p": 0.9
                },
                timeout=120
            )
            
            if response.status_code == 200:
                content = response.json()['choices'][0]['message']['content']
                print(f"✅ API Response received ({len(content)} characters)")
                
                # Clean and parse JSON
                json_str = self.clean_json_response(content)
                topics = json.loads(json_str)
                
                # Validate structure
                valid_topics = []
                for topic in topics:
                    if self.validate_topic_structure(topic):
                        # Ensure topic_weight matches sum of subtopic weights
                        subtopic_sum = sum(sub['weight'] for sub in topic['subtopics'])
                        topic['topic_weight'] = round(subtopic_sum, 6)
                        valid_topics.append(topic)
                
                print(f"✅ Successfully parsed {len(valid_topics)} valid topics from {len(topics)} generated")
                return valid_topics
                
            elif response.status_code == 429:
                print("❌ Rate limit exceeded. Waiting longer...")
                time.sleep(30)
                return []
            else:
                print(f"❌ API Error: {response.status_code}")
                print(f"Response: {response.text[:500]}...")
                return []
                
        except json.JSONDecodeError as e:
            print(f"❌ JSON parsing error: {e}")
            print(f"Content that failed to parse: {json_str[:500]}...")
            return []
        except requests.exceptions.Timeout:
            print("❌ Request timeout. Retrying...")
            return []
        except Exception as e:
            print(f"❌ Error generating topics batch: {e}")
            return []

    def generate_comprehensive_topics(self, target_topics: int = 200) -> List[Dict]:
        """Generate comprehensive banking topics with real-time progress tracking"""
        
        # Test model availability first
        try:
            self.test_model_availability()
        except Exception as e:
            print(f"❌ {e}")
            return []
        
        print(f"🎯 Target: {target_topics} unique dominant topics")
        print(f"🤖 Using model: {self.model}")
        print(f"📊 Monitor progress in real-time: {self.progress_file}")
        print("=" * 60)
        
        all_topics = []
        used_topic_names = set()
        used_subtopic_names = set()
        batch_size = 15  # Smaller batches for better success rate
        max_attempts = 30
        consecutive_failures = 0
        
        for attempt in range(max_attempts):
            if len(all_topics) >= target_topics:
                break
                
            # Break if too many consecutive failures
            if consecutive_failures >= 5:
                print("❌ Too many consecutive failures. Stopping...")
                break
                
            remaining = target_topics - len(all_topics)
            current_batch_size = min(batch_size, remaining)
            
            print(f"\n📡 Batch {attempt + 1}: Generating {current_batch_size} topics...")
            print(f"⏱️  {time.strftime('%H:%M:%S')} - Starting API call...")
            
            # Generate batch
            batch_topics = self.generate_banking_topics_batch(
                current_batch_size, 
                used_topic_names, 
                used_subtopic_names
            )
            
            if not batch_topics:
                consecutive_failures += 1
                wait_time = min(10 + (consecutive_failures * 5), 60)  # Exponential backoff
                print(f"❌ Batch {attempt + 1} failed, retrying in {wait_time} seconds...")
                self.update_progress_file(all_topics, used_topic_names, used_subtopic_names, 
                                        attempt + 1, target_topics, "error_retrying")
                time.sleep(wait_time)
                continue
            
            consecutive_failures = 0  # Reset failure counter
            
            # Filter for unique topics and subtopics
            unique_batch_topics = []
            for topic in batch_topics:
                topic_name = topic['dominant_topic']
                
                # Check if topic is unique
                if topic_name not in used_topic_names:
                    # Check if all subtopics are unique
                    topic_subtopics = [sub['name'] for sub in topic['subtopics']]
                    if not any(sub_name in used_subtopic_names for sub_name in topic_subtopics):
                        unique_batch_topics.append(topic)
                        used_topic_names.add(topic_name)
                        used_subtopic_names.update(topic_subtopics)
            
            all_topics.extend(unique_batch_topics)
            
            # Update progress file immediately after each batch
            self.update_progress_file(all_topics, used_topic_names, used_subtopic_names, 
                                    attempt + 1, target_topics, "generating")
            
            print(f"✅ Added {len(unique_batch_topics)} unique topics")
            print(f"📈 Total progress: {len(all_topics)}/{target_topics}")
            
            # Show some new topics added
            if unique_batch_topics:
                print("🆕 New topics added:")
                for i, topic in enumerate(unique_batch_topics[:3]):  # Show first 3
                    subtopic_names = [sub['name'] for sub in topic['subtopics']]
                    print(f"   • {topic['dominant_topic']}: {', '.join(subtopic_names[:2])}...")
                if len(unique_batch_topics) > 3:
                    print(f"   ... and {len(unique_batch_topics) - 3} more topics")
            
            # Rate limiting - be more conservative
            wait_time = 5 if len(unique_batch_topics) > 0 else 10
            print(f"⏳ Waiting {wait_time} seconds before next batch...")
            time.sleep(wait_time)
        
        # Trim to exact target if exceeded
        if len(all_topics) > target_topics:
            all_topics = all_topics[:target_topics]
        
        # Final update with normalization
        print(f"\n🔄 Normalizing weights...")
        self.normalize_weights(all_topics)
        
        # Update progress file with final status
        self.update_progress_file(all_topics, used_topic_names, used_subtopic_names, 
                                attempt + 1, target_topics, "completed")
        
        print(f"\n🎉 Generated {len(all_topics)} unique dominant topics")
        return all_topics

    def normalize_weights(self, topics: List[Dict]):
        """Normalize all topic weights to sum to 1.0"""
        
        if not topics:
            return
            
        # Calculate current total weight
        total_weight = sum(topic['topic_weight'] for topic in topics)
        
        if total_weight == 0:
            # If total weight is 0, assign equal weights
            equal_weight = 1.0 / len(topics)
            for topic in topics:
                topic['topic_weight'] = equal_weight
                # Also normalize subtopic weights
                equal_sub_weight = topic['topic_weight'] / len(topic['subtopics'])
                for subtopic in topic['subtopics']:
                    subtopic['weight'] = equal_sub_weight
        else:
            # Normalize topic weights
            for topic in topics:
                topic['topic_weight'] = round(topic['topic_weight'] / total_weight, 6)
                
                # Normalize subtopic weights within each topic
                subtopic_total = sum(sub['weight'] for sub in topic['subtopics'])
                if subtopic_total > 0:
                    for subtopic in topic['subtopics']:
                        subtopic['weight'] = round(
                            (subtopic['weight'] / subtopic_total) * topic['topic_weight'], 6
                        )
        
        final_total = sum(topic['topic_weight'] for topic in topics)
        print(f"✅ Normalized weights (Total weight: {final_total:.6f})")

    def save_final_topics_to_json(self, topics: List[Dict]):
        """Save final topics structure to JSON file"""
        
        try:
            # Add metadata
            final_data = {
                "metadata": {
                    "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "total_topics": len(topics),
                    "total_subtopics": sum(len(topic['subtopics']) for topic in topics),
                    "model_used": self.model,
                    "total_weight": sum(topic['topic_weight'] for topic in topics)
                },
                "topics": topics
            }
            
            with open(self.final_file, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=2, ensure_ascii=False)
            print(f"✅ Final topics saved to {self.final_file}")
        except Exception as e:
            print(f"❌ Error saving final topics: {e}")

    def display_statistics(self, topics: List[Dict]):
        """Display comprehensive statistics about generated topics"""
        
        if not topics:
            print("No topics to analyze")
            return
        
        # Calculate statistics
        total_topics = len(topics)
        total_subtopics = sum(len(topic['subtopics']) for topic in topics)
        avg_subtopics = total_subtopics / total_topics
        min_subtopics = min(len(topic['subtopics']) for topic in topics)
        max_subtopics = max(len(topic['subtopics']) for topic in topics)
        
        # Weight statistics
        topic_weights = [topic['topic_weight'] for topic in topics]
        max_weight_topic = max(topics, key=lambda x: x['topic_weight'])
        min_weight_topic = min(topics, key=lambda x: x['topic_weight'])
        
        print(f"\n📊 Final Statistics:")
        print("=" * 50)
        print(f"Total Dominant Topics: {total_topics}")
        print(f"Total Subtopics: {total_subtopics}")
        print(f"Average Subtopics per Topic: {avg_subtopics:.1f}")
        print(f"Subtopics Range: {min_subtopics} - {max_subtopics}")
        print(f"Total Combined Weight: {sum(topic_weights):.6f}")
        print(f"Model Used: {self.model}")
        print(f"Highest Weight Topic: '{max_weight_topic['dominant_topic']}' ({max_weight_topic['topic_weight']:.6f})")
        print(f"Lowest Weight Topic: '{min_weight_topic['dominant_topic']}' ({min_weight_topic['topic_weight']:.6f})")

    def display_sample_topics(self, topics: List[Dict], count: int = 3):
        """Display sample topics with their subtopics"""
        
        print(f"\n📝 Sample Topics:")
        print("=" * 50)
        
        for i, topic in enumerate(random.sample(topics, min(count, len(topics)))):
            print(f"\n{i+1}. Dominant Topic: '{topic['dominant_topic']}' (Weight: {topic['topic_weight']:.6f})")
            print(f"   Subtopics ({len(topic['subtopics'])}):")
            for j, subtopic in enumerate(topic['subtopics']):
                print(f"      {j+1}: {subtopic['name']} (Weight: {subtopic['weight']:.6f})")

def main():
    generator = EuropeanBankTopicGenerator()
    
    print("🏦 European Bank Topic & Weight Generator")
    print("📊 Real-Time Progress Monitoring Enabled")
    print("=" * 60)
    
    # Generate 150-200 comprehensive banking topics
    target_topics = random.randint(150, 200)
    print(f"🎯 Generating {target_topics} European banking topics...")
    print(f"📁 Progress file: {generator.progress_file}")
    print(f"📁 Final file: {generator.final_file}")
    print("\n💡 Tip: Open the progress file in another window to monitor real-time updates!")
    
    topics = generator.generate_comprehensive_topics(target_topics)
    
    if topics:
        # Save final version
        generator.save_final_topics_to_json(topics)
        
        # Display statistics
        generator.display_statistics(topics)
        
        # Show sample topics
        generator.display_sample_topics(topics, 5)
        
        print(f"\n🎯 Complete! Generated {len(topics)} banking topics with weights.")
        print(f"📁 Progress file: {generator.progress_file}")
        print(f"📁 Final file: {generator.final_file}")
        
    else:
        print("❌ Failed to generate topics.")

if __name__ == "__main__":
    main()