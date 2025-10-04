# Import required libraries
from pymongo import MongoClient
from collections import defaultdict
import csv
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

def analyze_dominant_topics():
    """
    Analyze dominant topics and their message count distribution
    """
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_CONNECTION_STRING)
        db = client[MONGO_DATABASE_NAME]
        collection = db['email_new']
        
        print("Connected to MongoDB successfully!")
        
        # Dictionary to store topic data
        # Structure: {topic: {total_count: int, message_counts: {count: {'frequency': int, 'stages': {stage: count}, 'categories': {category: count}}}}
        topic_data = defaultdict(lambda: {'total_count': 0, 'message_counts': defaultdict(lambda: {'frequency': 0, 'stages': defaultdict(int), 'categories': defaultdict(int)})})
        
        # Fetch all documents with dominant_topic field
        documents = collection.find(
            {'dominant_topic': {'$exists': True, '$ne': None}},
            {'dominant_topic': 1, 'thread.message_count': 1, 'stages': 1, 'category': 1}
        )
        
        print("Fetching and processing documents...")
        
        # Process each document
        doc_count = 0
        category_found_count = 0
        stages_found_count = 0
        
        for doc in documents:
            dominant_topic = doc.get('dominant_topic')
            
            # Get message_count from thread object
            thread = doc.get('thread', {})
            message_count = thread.get('message_count', 0)
            
            # Get stages and category
            stages = doc.get('stages')
            category = doc.get('category')
            
            if dominant_topic:
                # Increment total count for this topic
                topic_data[dominant_topic]['total_count'] += 1
                
                # Increment the specific message_count frequency and collect unique stages/categories
                topic_data[dominant_topic]['message_counts'][message_count]['frequency'] += 1
                
                # Count individual stages and categories (handle None, empty string, and actual values)
                if stages and stages.strip():
                    stage_value = stages.strip()
                    topic_data[dominant_topic]['message_counts'][message_count]['stages'][stage_value] += 1
                    stages_found_count += 1
                
                if category and category.strip():
                    category_value = category.strip()
                    topic_data[dominant_topic]['message_counts'][message_count]['categories'][category_value] += 1
                    category_found_count += 1
                
                doc_count += 1
        
        print(f"Processed {doc_count} documents")
        print(f"Found stages in {stages_found_count} documents")
        print(f"Found category in {category_found_count} documents")
        
        # Debug: Check a sample document to see what fields exist
        if doc_count > 0:
            sample_doc = collection.find_one({'dominant_topic': {'$exists': True, '$ne': None}})
            if sample_doc:
                print(f"\nSample document fields: {list(sample_doc.keys())}")
                print(f"Sample stages value: '{sample_doc.get('stages', 'NOT_FOUND')}'")
                print(f"Sample category value: '{sample_doc.get('category', 'NOT_FOUND')}'")
                print(f"Sample dominant_topic: '{sample_doc.get('dominant_topic', 'NOT_FOUND')}'")
                if 'thread' in sample_doc:
                    print(f"Sample thread.message_count: '{sample_doc['thread'].get('message_count', 'NOT_FOUND')}'")
        
        # Prepare data for CSV
        csv_data = []
        
        for topic, data in sorted(topic_data.items()):
            total_records = data['total_count']
            
            # Format message split as "count:frequency, count:frequency, ..."
            message_splits = []
            stages_by_count = []
            categories_by_count = []
            
            for msg_count in sorted(data['message_counts'].keys()):
                msg_data = data['message_counts'][msg_count]
                frequency = msg_data['frequency']
                message_splits.append(f"{msg_count}:{frequency}")
                
                # Format stages and categories for this specific message count
                stages_dict = msg_data['stages']
                categories_dict = msg_data['categories']
                
                # Format stages as "count:stage1_count|stage1,stage2_count|stage2"
                stage_parts = []
                for stage, stage_count in sorted(stages_dict.items()):
                    stage_parts.append(f"{stage_count}|{stage}")
                stages_str = f"{msg_count}:{','.join(stage_parts)}" if stage_parts else f"{msg_count}:"
                
                # Format categories as "count:category1_count|category1,category2_count|category2"
                category_parts = []
                for category, category_count in sorted(categories_dict.items()):
                    category_parts.append(f"{category_count}|{category}")
                categories_str = f"{msg_count}:{','.join(category_parts)}" if category_parts else f"{msg_count}:"
                
                stages_by_count.append(stages_str)
                categories_by_count.append(categories_str)
            
            message_split_str = ', '.join(message_splits)
            stages_str = ', '.join(stages_by_count)
            categories_str = ', '.join(categories_by_count)
            
            csv_row = {
                'dominant_topic': topic,
                'total_records': total_records,
                'message_split': message_split_str,
                'stages': stages_str,
                'category': categories_str
            }
            csv_data.append(csv_row)
            
            # Debug: Print first few rows to see what's being saved
            if len(csv_data) <= 3:
                print(f"Debug CSV row {len(csv_data)}: stages='{stages_str}', category='{categories_str}'")
        
        # Write to CSV file
        output_filename = 'dominant_topic_analysis_with_counts.csv'
        
        with open(output_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['dominant_topic', 'total_records', 'message_split', 'stages', 'category']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            writer.writerows(csv_data)
        
        print(f"\nAnalysis complete! Results saved to '{output_filename}'")
        print(f"Total unique topics found: {len(csv_data)}")
        
        # Display sample results
        print("\nSample results:")
        for i, row in enumerate(csv_data[:5]):
            print(f"{row['dominant_topic']}: {row['total_records']} records - {row['message_split']}")
            print(f"  Stages: {row['stages']}")
            print(f"  Categories: {row['category']}")
            if i == 4 and len(csv_data) > 5:
                print(f"... and {len(csv_data) - 5} more topics")
        
        print("\nFormat explanation:")
        print("stages: message_count:stage1_count|stage1,stage2_count|stage2, ...")
        print("category: message_count:category1_count|category1,category2_count|category2, ...")
        print("Example: 1:2|Resolved,1|Update means message_count=1, 2 records have 'Resolved' stage, 1 record has 'Update' stage")
        
        # Close MongoDB connection
        client.close()
        
        return csv_data
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return None

if __name__ == "__main__":
    print("Starting dominant topic analysis...\n")
    analyze_dominant_topics()