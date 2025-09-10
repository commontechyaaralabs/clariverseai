from pymongo import MongoClient
import logging
from datetime import datetime
import json

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'failed_regeneration_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)

def get_failed_records():
    """Identify records missing required fields"""
    try:
        client = MongoClient("mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
        db = client.sparzaai
        collection = db.twitter

        # Find documents missing any required field
        query = {
            '$or': [
                {'text': {'$exists': False}},
                {'hashtags': {'$exists': False}},
                {'sentiment': {'$exists': False}},
                {'urgency': {'$exists': False}},
                {'priority': {'$exists': False}},
                {'like_count': {'$exists': False}},
                {'retweet_count': {'$exists': False}},
                {'reply_count': {'$exists': False}},
                {'quote_count': {'$exists': False}}
            ]
        }
        
        # Get count first
        total_failed = collection.count_documents(query)
        logging.info(f"Found {total_failed} records missing required fields")
        
        if total_failed == 0:
            return []
            
        # Get the failed records
        failed_records = list(collection.find(query))
        failed_data = []
        
        for doc in failed_records:
            if isinstance(doc, dict) and 'tweet_id' in doc:
                record = {
                    'tweet_id': doc['tweet_id'],
                    'username': doc.get('username', 'Unknown User'),
                    'dominant_topic': doc.get('dominant_topic', 'General Banking'),
                    'subtopics': doc.get('subtopics', 'General operations')
                }
                failed_data.append(record)
                logging.info(f"Processing failed record: {doc['tweet_id']}")
            else:
                logging.warning(f"Skipping invalid document: {doc}")

        # Save failed records for reference with counts
        output_data = {
            'total_found': total_failed,
            'valid_records': len(failed_data),
            'records': failed_data
        }
        
        filename = f'failed_tweet_ids_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        with open(filename, 'w') as f:
            json.dump(output_data, f, indent=2)
            
        logging.info(f"Found {len(failed_data)} valid failed records")
        logging.info(f"Saved failed records to {filename}")
        
        # Return the failed_data list directly
        return failed_data
        
    except Exception as e:
        logging.error(f"Error getting failed records: {str(e)}")
        return []
    finally:
        client.close()

if __name__ == "__main__":
    failed_ids = get_failed_records()
    print(f"Total failed records found: {len(failed_ids)}")
