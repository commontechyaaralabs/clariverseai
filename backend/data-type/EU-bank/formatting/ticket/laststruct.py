import os
from pymongo import MongoClient
from dotenv import load_dotenv
import logging
from collections import OrderedDict

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Get connection details from environment variables
mongo_connection_string = os.getenv('MONGO_CONNECTION_STRING')
mongo_database_name = os.getenv('MONGO_DATABASE_NAME')

def reorder_using_aggregation():
    """
    Reorder fields using aggregation pipeline - this actually reorders fields in MongoDB
    """
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        collection = db['tickets']
        
        # Get total document count
        total_docs = collection.count_documents({})
        logger.info(f"Found {total_docs} documents to reorder")
        
        if total_docs == 0:
            logger.info("No documents found in the collection")
            return
        
        # Create backup collection name with timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f'tickets_backup_{timestamp}'
        temp_name = 'tickets_reordered_temp'
        
        logger.info(f"Creating backup collection: {backup_name}")
        
        # Create backup first
        db.create_collection(backup_name)
        backup_result = collection.aggregate([
            {'$out': backup_name}
        ])
        
        backup_count = db[backup_name].count_documents({})
        logger.info(f"Backup created with {backup_count} documents")
        
        # Get a sample document to identify all fields
        sample_doc = collection.find_one({})
        if not sample_doc:
            logger.error("No documents found to analyze")
            return
            
        all_fields = list(sample_doc.keys())
        logger.info(f"Identified fields: {all_fields}")
        
        # Define the desired field order
        desired_order = ['_id', 'provider', 'thread', 'messages']
        
        # Add remaining fields that aren't in the desired order
        remaining_fields = [field for field in all_fields if field not in desired_order]
        final_order = desired_order + remaining_fields
        
        logger.info(f"Final field order: {final_order}")
        
        # Create aggregation pipeline with $project to reorder fields
        project_stage = {}
        for field in final_order:
            if field in all_fields:
                project_stage[field] = 1
        
        pipeline = [
            {
                '$project': project_stage
            },
            {
                '$out': temp_name
            }
        ]
        
        logger.info("Executing aggregation pipeline to reorder fields...")
        collection.aggregate(pipeline)
        
        # Verify temp collection
        temp_collection = db[temp_name]
        temp_count = temp_collection.count_documents({})
        logger.info(f"Temporary collection created with {temp_count} documents")
        
        if temp_count != total_docs:
            raise Exception(f"Document count mismatch: original={total_docs}, temp={temp_count}")
        
        # Verify field order in temp collection
        logger.info("Verifying field order in temporary collection...")
        temp_sample = temp_collection.find_one({})
        temp_fields = list(temp_sample.keys())
        logger.info(f"Temp collection field order: {temp_fields}")
        
        # Replace original collection
        logger.info("Replacing original collection...")
        collection.drop()
        temp_collection.rename('tickets')
        
        # Final verification
        logger.info("Final verification...")
        new_collection = db['tickets']
        final_count = new_collection.count_documents({})
        final_sample = new_collection.find_one({})
        final_fields = list(final_sample.keys()) if final_sample else []
        
        logger.info(f"Reordering completed!")
        logger.info(f"Final document count: {final_count}")
        logger.info(f"Final field order: {final_fields}")
        logger.info(f"Backup collection preserved as: {backup_name}")
        
    except Exception as e:
        logger.error(f"Error during field reordering: {str(e)}")
        # Try to restore from backup if something went wrong
        try:
            if 'backup_name' in locals() and backup_name in db.list_collection_names():
                logger.info("Attempting to restore from backup...")
                if 'tickets' in db.list_collection_names():
                    db['tickets'].drop()
                db[backup_name].aggregate([{'$out': 'tickets'}])
                logger.info("Restored from backup successfully")
        except Exception as restore_error:
            logger.error(f"Failed to restore from backup: {str(restore_error)}")
        raise
    finally:
        # Clean up temp collection
        if 'temp_name' in locals() and temp_name in db.list_collection_names():
            db[temp_name].drop()
        
        # Close the connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def rename_subject_norm_to_ticket_title():
    """
    Rename 'subject_norm' field to 'ticket_title' in the thread object of all documents
    """
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        collection = db['tickets']
        
        # Get total document count
        total_docs = collection.count_documents({})
        logger.info(f"Found {total_docs} documents to process")
        
        if total_docs == 0:
            logger.info("No documents found in the collection")
            return
        
        # Create backup collection name with timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f'tickets_backup_rename_{timestamp}'
        
        logger.info(f"Creating backup collection: {backup_name}")
        
        # Create backup first
        db.create_collection(backup_name)
        backup_result = collection.aggregate([
            {'$out': backup_name}
        ])
        
        backup_count = db[backup_name].count_documents({})
        logger.info(f"Backup created with {backup_count} documents")
        
        # Use aggregation pipeline to rename the field
        logger.info("Renaming 'subject_norm' to 'ticket_title' in thread object...")
        
        pipeline = [
            {
                '$addFields': {
                    'thread.ticket_title': '$thread.subject_norm'
                }
            },
            {
                '$unset': 'thread.subject_norm'
            },
            {
                '$out': 'tickets'
            }
        ]
        
        # Execute the aggregation pipeline
        collection.aggregate(pipeline)
        
        # Verify the rename operation
        logger.info("Verifying field rename operation...")
        
        # Check if any documents still have subject_norm
        remaining_subject_norm = collection.count_documents({
            'thread.subject_norm': {'$exists': True}
        })
        
        # Check if documents now have ticket_title
        documents_with_ticket_title = collection.count_documents({
            'thread.ticket_title': {'$exists': True}
        })
        
        logger.info(f"Documents still with 'subject_norm': {remaining_subject_norm}")
        logger.info(f"Documents now with 'ticket_title': {documents_with_ticket_title}")
        
        # Show sample document to verify
        sample_doc = collection.find_one({})
        if sample_doc and 'thread' in sample_doc:
            thread_data = sample_doc['thread']
            if 'ticket_title' in thread_data:
                logger.info(f"Sample ticket_title: {thread_data['ticket_title']}")
            if 'subject_norm' in thread_data:
                logger.warning(f"Sample still has subject_norm: {thread_data['subject_norm']}")
        
        if remaining_subject_norm == 0 and documents_with_ticket_title > 0:
            logger.info("✅ Field rename operation completed successfully!")
            logger.info(f"✅ Renamed 'subject_norm' to 'ticket_title' in {documents_with_ticket_title} documents")
        else:
            logger.warning("⚠️ Field rename operation may not have completed successfully")
            logger.warning("Check the verification results above")
        
        logger.info(f"Backup collection preserved as: {backup_name}")
        
    except Exception as e:
        logger.error(f"Error during field rename operation: {str(e)}")
        # Try to restore from backup if something went wrong
        try:
            if 'backup_name' in locals() and backup_name in db.list_collection_names():
                logger.info("Attempting to restore from backup...")
                if 'tickets' in db.list_collection_names():
                    db['tickets'].drop()
                db[backup_name].aggregate([{'$out': 'tickets'}])
                logger.info("Restored from backup successfully")
        except Exception as restore_error:
            logger.error(f"Failed to restore from backup: {str(restore_error)}")
        raise
    finally:
        # Close the connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def set_ticket_title_to_null():
    """
    Set 'ticket_title' field to null in the thread object of all documents
    """
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        collection = db['tickets']
        
        # Get total document count
        total_docs = collection.count_documents({})
        logger.info(f"Found {total_docs} documents to process")
        
        if total_docs == 0:
            logger.info("No documents found in the collection")
            return
        
        # Create backup collection name with timestamp
        import datetime
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f'tickets_backup_null_title_{timestamp}'
        
        logger.info(f"Creating backup collection: {backup_name}")
        
        # Create backup first
        db.create_collection(backup_name)
        backup_result = collection.aggregate([
            {'$out': backup_name}
        ])
        
        backup_count = db[backup_name].count_documents({})
        logger.info(f"Backup created with {backup_count} documents")
        
        # Use aggregation pipeline to set ticket_title to null
        logger.info("Setting 'ticket_title' to null in thread object...")
        
        pipeline = [
            {
                '$set': {
                    'thread.ticket_title': None
                }
            },
            {
                '$out': 'tickets'
            }
        ]
        
        # Execute the aggregation pipeline
        collection.aggregate(pipeline)
        
        # Verify the operation
        logger.info("Verifying ticket_title null operation...")
        
        # Check how many documents have ticket_title as null
        documents_with_null_title = collection.count_documents({
            'thread.ticket_title': None
        })
        
        # Check how many documents have ticket_title with any value
        documents_with_value_title = collection.count_documents({
            'thread.ticket_title': {'$ne': None}
        })
        
        logger.info(f"Documents with ticket_title = null: {documents_with_null_title}")
        logger.info(f"Documents with ticket_title != null: {documents_with_value_title}")
        
        # Show sample document to verify
        sample_doc = collection.find_one({})
        if sample_doc and 'thread' in sample_doc:
            thread_data = sample_doc['thread']
            if 'ticket_title' in thread_data:
                logger.info(f"Sample ticket_title value: {thread_data['ticket_title']}")
        
        if documents_with_value_title == 0 and documents_with_null_title > 0:
            logger.info("✅ Ticket title null operation completed successfully!")
            logger.info(f"✅ Set 'ticket_title' to null in {documents_with_null_title} documents")
        else:
            logger.warning("⚠️ Ticket title null operation may not have completed successfully")
            logger.warning("Check the verification results above")
        
        logger.info(f"Backup collection preserved as: {backup_name}")
        
    except Exception as e:
        logger.error(f"Error during ticket title null operation: {str(e)}")
        # Try to restore from backup if something went wrong
        try:
            if 'backup_name' in locals() and backup_name in db.list_collection_names():
                logger.info("Attempting to restore from backup...")
                if 'tickets' in db.list_collection_names():
                    db['tickets'].drop()
                db[backup_name].aggregate([{'$out': 'tickets'}])
                logger.info("Restored from backup successfully")
        except Exception as restore_error:
            logger.error(f"Failed to restore from backup: {str(restore_error)}")
        raise
    finally:
        # Close the connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def reorder_using_replacement():
    """
    Alternative method: Replace each document entirely with reordered fields
    This method actually updates the field order in place
    """
    try:
        # Connect to MongoDB
        logger.info("Connecting to MongoDB...")
        client = MongoClient(mongo_connection_string)
        db = client[mongo_database_name]
        collection = db['tickets']
        
        # Get total document count
        total_docs = collection.count_documents({})
        logger.info(f"Found {total_docs} documents to process")
        
        if total_docs == 0:
            logger.info("No documents found in the collection")
            return
        
        # Process documents in batches
        batch_size = 100
        processed_count = 0
        
        # Get all documents
        cursor = collection.find({})
        
        for document in cursor:
            try:
                # Create ordered dictionary with desired field order
                reordered_doc = OrderedDict()
                
                # Desired field order
                field_order = ['_id', 'provider', 'thread', 'messages']
                
                # Add fields in desired order
                for field in field_order:
                    if field in document:
                        reordered_doc[field] = document[field]
                
                # Add remaining fields
                for key, value in document.items():
                    if key not in reordered_doc:
                        reordered_doc[key] = value
                
                # Replace the entire document
                collection.replace_one(
                    {'_id': document['_id']},
                    dict(reordered_doc)
                )
                
                processed_count += 1
                
                # Log progress
                if processed_count % 50 == 0:
                    logger.info(f"Processed {processed_count}/{total_docs} documents ({processed_count/total_docs*100:.1f}%)")
                    
            except Exception as e:
                logger.error(f"Error processing document {document.get('_id', 'unknown')}: {str(e)}")
                continue
        
        logger.info(f"Field reordering completed! Processed {processed_count} documents")
        
        # Verify the reordering
        logger.info("Verifying field order...")
        sample_docs = collection.find({}).limit(3)
        for i, doc in enumerate(sample_docs, 1):
            field_order = list(doc.keys())
            logger.info(f"Sample document {i} field order: {field_order}")
        
    except Exception as e:
        logger.error(f"Error during field reordering: {str(e)}")
        raise
    finally:
        # Close the connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def main():
    """
    Main function to execute field reordering
    """
    if not mongo_connection_string or not mongo_database_name:
        logger.error("MongoDB connection string or database name not found in environment variables")
        return
    
    logger.info("Starting MongoDB field reordering process...")
    logger.info(f"Database: {mongo_database_name}")
    logger.info(f"Collection: tickets")
    logger.info("Target field order: _id, provider, thread, messages, [other fields]")
    
    print("\n" + "="*60)
    print("MONGODB TICKETS COLLECTION MANAGEMENT TOOL")
    print("="*60)
    print("Choose operation:")
    print("1. Field Reordering - Aggregation Pipeline Method (Recommended)")
    print("   - Creates backup automatically")
    print("   - Faster for large collections") 
    print("   - Uses MongoDB's native capabilities")
    print("")
    print("2. Field Reordering - Document Replacement Method")
    print("   - Updates documents in place")
    print("   - No backup created")
    print("   - Slower but more granular control")
    print("")
    print("3. Rename Field: subject_norm → ticket_title")
    print("   - Renames 'subject_norm' to 'ticket_title' in thread object")
    print("   - Creates backup automatically")
    print("   - Safe operation with verification")
    print("")
    print("4. Set ticket_title to null")
    print("   - Sets 'ticket_title' field to null in thread object")
    print("   - Creates backup automatically")
    print("   - Safe operation with verification")
    print("")
    print("5. Exit")
    print("="*60)
    
    while True:
        choice = input("Enter your choice (1, 2, 3, 4, or 5): ").strip()
        if choice == '1':
            confirm = input("\nThis will create a backup and reorder fields. Continue? (yes/no): ").strip().lower()
            if confirm == 'yes':
                reorder_using_aggregation()
            else:
                print("Operation cancelled.")
            break
        elif choice == '2':
            confirm = input("\nThis will modify documents in place (NO backup). Continue? (yes/no): ").strip().lower()
            if confirm == 'yes':
                reorder_using_replacement()
            else:
                print("Operation cancelled.")
            break
        elif choice == '3':
            confirm = input("\nThis will rename 'subject_norm' to 'ticket_title' in thread object. Continue? (yes/no): ").strip().lower()
            if confirm == 'yes':
                rename_subject_norm_to_ticket_title()
            else:
                print("Operation cancelled.")
            break
        elif choice == '4':
            confirm = input("\nThis will set 'ticket_title' to null in thread object. Continue? (yes/no): ").strip().lower()
            if confirm == 'yes':
                set_ticket_title_to_null()
            else:
                print("Operation cancelled.")
            break
        elif choice == '5':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main()