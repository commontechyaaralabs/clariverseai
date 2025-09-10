from pymongo import MongoClient
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import json
from data_generation import generate_eu_banking_tweet_content
from get_failed_records import get_failed_records

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=f'regeneration_progress_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
)

def regenerate_failed_tweets(failed_records, batch_size=10):
    """Regenerate content for failed tweets in batches"""
    client = MongoClient("mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
    db = client.sparzaai
    collection = db.twitter
    
    start_time = datetime.now()
    success_count = 0
    total_records = len(failed_records)
    
    def get_time_estimate(processed, total, elapsed):
        """Calculate time estimates"""
        if processed == 0:
            return "Calculating..."
        
        rate = elapsed / processed
        remaining = total - processed
        eta = rate * remaining
        
        return f"ETA: {str(datetime.now() + eta).split('.')[0]}"
    
    def process_single_failed(record):
        try:
            if not isinstance(record, dict):
                logging.error(f"Invalid record format: {record}")
                return False

            # Generate new content
            tweet_data = {
                'tweet_id': str(record.get('tweet_id')),  # Ensure tweet_id is string
                'username': record.get('username', 'Unknown User'),
                'dominant_topic': record.get('dominant_topic', 'General Banking'),
                'subtopics': record.get('subtopics', 'General operations')
            }
            
            logging.info(f"Processing tweet_id: {tweet_data['tweet_id']}")
            new_content = generate_eu_banking_tweet_content(tweet_data)
            
            logging.info(f"Generated content type: {type(new_content)}")
            if new_content:
                logging.info(f"Content keys: {new_content.keys() if isinstance(new_content, dict) else 'Not a dict'}")
                logging.info(f"Content values: {', '.join(f'{k}: {type(v)}' for k, v in new_content.items()) if isinstance(new_content, dict) else 'Not a dict'}")
            
            if new_content and isinstance(new_content, dict):
                try:
                    # Process the content before updating
                    processed_content = {}
                    for k, v in new_content.items():
                        if k == 'hashtags' and isinstance(v, list):
                            # Keep hashtags as a list
                            processed_content[k] = v
                        elif isinstance(v, (str, int, float, bool)):
                            # Convert numeric values to appropriate types
                            if k in ['like_count', 'retweet_count', 'reply_count', 'quote_count']:
                                processed_content[k] = int(v) if isinstance(v, (str, float)) else v
                            elif k == 'urgency' and isinstance(v, bool):
                                # Convert boolean urgency to string format
                                processed_content[k] = 'High' if v else 'Low'
                            else:
                                processed_content[k] = v
                        else:
                            logging.warning(f"Skipping field {k} with invalid value type: {type(v)}")
                    
                    # Update document
                    if processed_content:
                        result = collection.update_one(
                            {'tweet_id': tweet_data['tweet_id']},
                            {'$set': processed_content}
                        )
                        if result.modified_count > 0:
                            logging.info(f"Successfully regenerated tweet {tweet_data['tweet_id']}")
                            return True
                    else:
                        logging.error(f"No valid content to update for tweet {tweet_data['tweet_id']}")
                        return False
                        
                except (ValueError, TypeError) as e:
                    logging.error(f"Error processing content for tweet {tweet_data['tweet_id']}: {str(e)}")
                    return False
                    
            logging.error(f"Failed to regenerate tweet {tweet_data.get('tweet_id')}")
            return False
                
        except Exception as e:
            logging.error(f"Error in process_single_failed: {str(e)}")
            return False
    
    # Process failed tweets in batches
    try:
        for i in range(0, total_records, batch_size):
            batch = failed_records[i:i + batch_size]
            logging.info(f"Processing batch {i//batch_size + 1}/{(total_records + batch_size - 1)//batch_size}")
            logging.info(f"Batch size: {len(batch)}")
            
            # Validate batch records
            valid_batch = []
            for record in batch:
                if isinstance(record, dict) and 'tweet_id' in record:
                    valid_batch.append(record)
                else:
                    logging.error(f"Skipping invalid batch record: {record}")
            
            if not valid_batch:
                logging.error("No valid records in batch, skipping")
                continue
            
            # Process batch in parallel
            with ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(process_single_failed, valid_batch))
            
            batch_success = sum(1 for r in results if r)
            success_count += batch_success
            logging.info(f"Batch success rate: {batch_success}/{len(valid_batch)}")
            logging.info(f"Overall progress: {success_count}/{total_records}")
            
            # Add batch summary
            elapsed_time = datetime.now() - start_time
            success_rate = (success_count / total_records) * 100
            eta = get_time_estimate(success_count, total_records, elapsed_time.total_seconds())
            
            logging.info(f"""
Batch Summary:
-------------
Total Records: {total_records}
Processed: {success_count}
Success Rate: {success_rate:.2f}%
Remaining: {total_records - success_count}
Time Elapsed: {str(elapsed_time).split('.')[0]}
{eta}
""")
        
        final_elapsed = datetime.now() - start_time
        logging.info(f"Total processing time: {str(final_elapsed).split('.')[0]}")
        return success_count
        
    except Exception as e:
        logging.error(f"Error in batch processing: {str(e)}", exc_info=True)
        return success_count

def verify_regeneration():
    """Verify all records have required fields"""
    try:
        client = MongoClient("mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
        db = client.sparzaai
        collection = db.twitter

        missing_fields = collection.count_documents({
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
        })

        logging.info(f"Records still missing fields: {missing_fields}")
        return missing_fields == 0
    except Exception as e:
        logging.error(f"Error verifying regeneration: {str(e)}")
        return False
    finally:
        client.close()

def main():
    try:
        # Get failed records
        failed_records = get_failed_records()
        
        logging.info(f"Type of failed_records: {type(failed_records)}")
        if failed_records:
            logging.info(f"First record example: {failed_records[0] if failed_records else 'None'}")
        
        if not isinstance(failed_records, list):
            logging.error(f"Expected list but got {type(failed_records)}")
            return
            
        if not failed_records:
            logging.info("No failed records found in database")
            return
            
        logging.info(f"Retrieved {len(failed_records)} records to process")
        
        # Validate records before processing
        valid_records = []
        for record in failed_records:
            logging.debug(f"Validating record: {record}")
            if isinstance(record, dict) and 'tweet_id' in record:
                valid_records.append(record)
                logging.debug(f"Valid record found: {record['tweet_id']}")
            else:
                logging.error(f"Skipping invalid record format: {record}")
        
        if not valid_records:
            logging.error("No valid records to process")
            return
            
        logging.info(f"Starting regeneration of {len(valid_records)} valid records...")
        
        # Regenerate content
        success_count = regenerate_failed_tweets(valid_records)
        logging.info(f"Regeneration complete. Successfully regenerated {success_count}/{len(valid_records)} tweets")
        
        # Verify results
        if verify_regeneration():
            logging.info("All records now have required fields")
        else:
            client = MongoClient("mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
            db = client.sparzaai
            collection = db.twitter
            remaining = collection.count_documents({
                '$or': [
                    {'text': {'$exists': False}},
                    {'hashtags': {'$exists': False}},
                    {'sentiment': {'$exists': False}},
                    {'urgency': {'$exists': False}},
                    {'priority': {'$exists': False}}
                ]
            })
            logging.warning(f"There are still {remaining} records missing required fields")
            client.close()
            
    except Exception as e:
        logging.error(f"Error in main: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
