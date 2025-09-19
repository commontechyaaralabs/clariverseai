# EU Banking Specific Ticket Reset Script
# This script resets specific ticket object IDs to null format
import os
import json
from datetime import datetime
from pymongo import MongoClient
from bson import ObjectId
from dotenv import load_dotenv
from pathlib import Path
import logging

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
TICKET_COLLECTION = "tickets"

# Logging setup
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

# Create timestamped log file
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE = LOG_DIR / f"reset_specific_tickets_{timestamp}.log"

# Configure logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class SpecificTicketReset:
    """Reset specific tickets to null format"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.ticket_col = None
        
        # Specific ticket IDs to reset (from your terminal output)
        self.target_ticket_ids = [
            "6889ba7ca4f4718f70978ff5",
            "6889ba7ea4f4718f70978ff6", 
            "6889ba7ea4f4718f70978ff7",
            "6889ba7ea4f4718f70978ff8",
            "6889ba7ea4f4718f70978ff9",
            "6889ba7ea4f4718f70978ffa",
            "6889ba7ea4f4718f70978ffb",
            "6889ba7ea4f4718f70978ffc",
            "6889ba7fa4f4718f70978ffd"
        ]
        
        self.reset_operations = []
    
    def init_database(self):
        """Initialize database connection"""
        try:
            self.client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            # Test connection
            self.client.admin.command('ping')
            self.db = self.client[DB_NAME]
            self.ticket_col = self.db[TICKET_COLLECTION]
            logger.info("Database connection established")
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def find_target_tickets(self):
        """Find the specific target tickets"""
        logger.info(f"Finding {len(self.target_ticket_ids)} specific target tickets...")
        
        try:
            # Convert string IDs to ObjectIds for querying
            object_ids = [ObjectId(ticket_id) for ticket_id in self.target_ticket_ids]
            
            # Find the specific tickets
            target_tickets = list(self.ticket_col.find({"_id": {"$in": object_ids}}))
            
            logger.info(f"Found {len(target_tickets)} target tickets in database")
            
            # Log sample tickets
            for i, ticket in enumerate(target_tickets[:5]):
                logger.info(f"Target ticket {i+1}: {ticket['_id']}")
                logger.info(f"  Topic: {ticket.get('dominant_topic', 'N/A')}")
                logger.info(f"  Current subject_norm: {ticket.get('thread', {}).get('subject_norm', 'N/A')}")
                logger.info(f"  Message count: {len(ticket.get('messages', []))}")
            
            return target_tickets
            
        except Exception as e:
            logger.error(f"Error finding target tickets: {e}")
            return []
    
    def prepare_reset_operations(self, target_tickets):
        """Prepare reset operations for specific tickets"""
        logger.info("Preparing reset operations for specific tickets...")
        
        try:
            for ticket in target_tickets:
                ticket_id = ticket['_id']
                
                # Build operations to reset to null format
                set_operations = {}
                
                # Reset thread.subject_norm to null
                set_operations['thread.subject_norm'] = None
                
                # Reset message content and headers to null
                if 'messages' in ticket and ticket['messages']:
                    updated_messages = []
                    for message in ticket['messages']:
                        updated_message = message.copy()
                        
                        # Reset message body content to null
                        if 'body' in updated_message and 'text' in updated_message['body']:
                            updated_message['body']['text']['plain'] = None
                        
                        # Reset ticket title in headers to null
                        if 'headers' in updated_message and 'ticket_title' in updated_message['headers']:
                            updated_message['headers']['ticket_title'] = None
                        
                        # Reset "to" addresses to null in headers
                        if 'headers' in updated_message and 'to' in updated_message['headers']:
                            for to_contact in updated_message['headers']['to']:
                                to_contact['name'] = None
                                to_contact['email'] = None
                        
                        # Reset "from" addresses to null where appropriate (company responses)
                        if 'headers' in updated_message and 'from' in updated_message['headers']:
                            for from_contact in updated_message['headers']['from']:
                                # Keep customer info, but reset company/support team info
                                if from_contact.get('email', '').endswith('@eubank.com'):
                                    from_contact['name'] = None
                                    from_contact['email'] = None
                        
                        updated_messages.append(updated_message)
                    set_operations['messages'] = updated_messages
                
                # Reset thread participants "to" address to null
                if 'thread' in ticket and 'participants' in ticket['thread']:
                    updated_participants = ticket['thread']['participants'].copy()
                    for participant in updated_participants:
                        if participant.get('type') == 'to':
                            participant['name'] = None
                            participant['email'] = None
                    set_operations['thread.participants'] = updated_participants
                
                # Build unset operations for LLM-generated fields
                unset_operations = {}
                llm_fields = [
                    'priority',
                    'urgency', 
                    'stages',
                    'ticket_summary',
                    'action_pending_status',
                    'action_pending_from',
                    'resolution_status',
                    'follow_up_required',
                    'follow_up_date',
                    'follow_up_reason',
                    'next_action_suggestion',
                    'overall_sentiment',
                    'ticket_raised',
                    'ticket_source',
                    'assigned_team',
                    'assigned_team_email'
                ]
                
                for field in llm_fields:
                    unset_operations[field] = ""
                
                operation = {
                    'ticket_id': str(ticket_id),
                    'unset_operations': unset_operations,
                    'set_operations': set_operations,
                    'original_ticket': {
                        'dominant_topic': ticket.get('dominant_topic', 'N/A'),
                        'subtopics': ticket.get('subtopics', 'N/A')[:100] + '...' if len(str(ticket.get('subtopics', ''))) > 100 else ticket.get('subtopics', 'N/A')
                    }
                }
                
                self.reset_operations.append(operation)
            
            logger.info(f"Prepared {len(self.reset_operations)} reset operations")
            return True
            
        except Exception as e:
            logger.error(f"Error preparing reset operations: {e}")
            return False
    
    def execute_reset_operations(self, dry_run=True):
        """Execute the reset operations"""
        if dry_run:
            logger.info("DRY RUN MODE - No changes will be made to database")
        else:
            logger.info("EXECUTING RESET OPERATIONS - Changes will be made to database")
        
        try:
            success_count = 0
            error_count = 0
            
            for i, operation in enumerate(self.reset_operations):
                ticket_id = operation['ticket_id']
                unset_ops = operation['unset_operations']
                set_ops = operation['set_operations']
                
                try:
                    if not dry_run:
                        # Execute operations separately to avoid conflicts
                        # First, unset the LLM-generated fields
                        unset_result = self.ticket_col.update_one(
                            {"_id": ObjectId(ticket_id)},
                            {"$unset": unset_ops}
                        )
                        
                        # Then, set the specific fields that need to be null
                        set_result = self.ticket_col.update_one(
                            {"_id": ObjectId(ticket_id)},
                            {"$set": set_ops}
                        )
                        
                        if unset_result.modified_count > 0 or set_result.modified_count > 0:
                            success_count += 1
                            logger.info(f"Reset ticket {ticket_id} successfully")
                        else:
                            error_count += 1
                            logger.warning(f"No changes made to ticket {ticket_id}")
                    else:
                        # Dry run - just log what would happen
                        success_count += 1
                        logger.info(f"[DRY RUN] Would reset ticket {ticket_id}")
                        logger.info(f"  Topic: {operation['original_ticket']['dominant_topic']}")
                        logger.info(f"  Fields to unset: {list(unset_ops.keys())}")
                        logger.info(f"  Fields to set to null: {list(set_ops.keys())}")
                    
                    # Progress indicator
                    if (i + 1) % 3 == 0:
                        logger.info(f"Progress: {i + 1}/{len(self.reset_operations)} tickets processed")
                        
                except Exception as e:
                    error_count += 1
                    logger.error(f"Error resetting ticket {ticket_id}: {e}")
            
            logger.info(f"Reset operation complete:")
            logger.info(f"  Successful: {success_count}")
            logger.info(f"  Errors: {error_count}")
            logger.info(f"  Total: {len(self.reset_operations)}")
            
            return success_count, error_count
            
        except Exception as e:
            logger.error(f"Error executing reset operations: {e}")
            return 0, len(self.reset_operations)
    
    def verify_reset_results(self):
        """Verify that the reset operations were successful"""
        logger.info("Verifying reset results...")
        
        try:
            # Convert string IDs to ObjectIds for querying
            object_ids = [ObjectId(ticket_id) for ticket_id in self.target_ticket_ids]
            
            # Check the specific tickets
            reset_tickets = list(self.ticket_col.find({"_id": {"$in": object_ids}}))
            
            logger.info(f"Verification results for {len(reset_tickets)} target tickets:")
            
            success_count = 0
            for ticket in reset_tickets:
                ticket_id = str(ticket['_id'])
                
                # Check if ticket is in proper null format
                has_null_subject = ticket.get('thread', {}).get('subject_norm') is None
                has_null_messages = all(
                    msg.get('body', {}).get('text', {}).get('plain') is None 
                    for msg in ticket.get('messages', [])
                )
                has_null_to_addresses = all(
                    all(to.get('name') is None and to.get('email') is None for to in msg.get('headers', {}).get('to', []))
                    for msg in ticket.get('messages', [])
                )
                no_llm_fields = not any([
                    ticket.get('priority'),
                    ticket.get('urgency'),
                    ticket.get('stages'),
                    ticket.get('ticket_summary'),
                    ticket.get('resolution_status'),
                    ticket.get('overall_sentiment'),
                    ticket.get('ticket_raised'),
                    ticket.get('ticket_source'),
                    ticket.get('assigned_team')
                ])
                
                if has_null_subject and has_null_messages and has_null_to_addresses and no_llm_fields:
                    success_count += 1
                    logger.info(f"  ✅ {ticket_id} - Properly reset to null format")
                else:
                    logger.warning(f"  ❌ {ticket_id} - Not properly reset")
                    if not has_null_subject:
                        logger.warning(f"    - subject_norm not null: {ticket.get('thread', {}).get('subject_norm')}")
                    if not has_null_messages:
                        logger.warning(f"    - Some messages not null")
                    if not has_null_to_addresses:
                        logger.warning(f"    - Some to addresses not null")
                    if not no_llm_fields:
                        logger.warning(f"    - Some LLM fields still present")
            
            logger.info(f"Verification complete: {success_count}/{len(reset_tickets)} tickets properly reset")
            return success_count == len(reset_tickets)
            
        except Exception as e:
            logger.error(f"Error verifying reset results: {e}")
            return False
    
    def export_reset_report(self):
        """Export a report of the reset operations"""
        try:
            report_file = f"specific_reset_report_{timestamp}.json"
            
            report_data = {
                'reset_timestamp': datetime.now().isoformat(),
                'database': DB_NAME,
                'collection': TICKET_COLLECTION,
                'target_ticket_ids': self.target_ticket_ids,
                'total_target_tickets': len(self.target_ticket_ids),
                'reset_operations': len(self.reset_operations),
                'reset_fields': [
                    'priority', 'urgency', 'stages', 'ticket_summary',
                    'action_pending_status', 'action_pending_from', 'resolution_status',
                    'follow_up_required', 'follow_up_date', 'follow_up_reason',
                    'next_action_suggestion', 'overall_sentiment', 'ticket_raised',
                    'ticket_source', 'assigned_team', 'assigned_team_email'
                ],
                'set_to_null_fields': [
                    'thread.subject_norm',
                    'messages[].body.text.plain',
                    'messages[].headers.ticket_title',
                    'messages[].headers.to[].name',
                    'messages[].headers.to[].email',
                    'messages[].headers.from[].name (company emails only)',
                    'messages[].headers.from[].email (company emails only)',
                    'thread.participants[].name (to type only)',
                    'thread.participants[].email (to type only)'
                ],
                'sample_tickets': []
            }
            
            # Add sample tickets to report
            for i, operation in enumerate(self.reset_operations[:5]):
                report_data['sample_tickets'].append({
                    'ticket_id': operation['ticket_id'],
                    'dominant_topic': operation['original_ticket']['dominant_topic'],
                    'subtopics': operation['original_ticket']['subtopics']
                })
            
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(report_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Reset report exported to: {report_file}")
            return report_file
            
        except Exception as e:
            logger.error(f"Error exporting reset report: {e}")
            return None
    
    def cleanup(self):
        """Cleanup database connection"""
        if self.client:
            try:
                self.client.close()
                logger.info("Database connection closed")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

def main():
    """Main function to run the specific ticket reset"""
    logger.info("EU Banking Specific Ticket Reset Script Starting...")
    
    reset_tool = SpecificTicketReset()
    
    try:
        # Initialize database
        if not reset_tool.init_database():
            logger.error("Cannot proceed without database connection")
            return
        
        # Find target tickets
        target_tickets = reset_tool.find_target_tickets()
        if not target_tickets:
            logger.error("No target tickets found")
            return
        
        # Prepare reset operations
        if not reset_tool.prepare_reset_operations(target_tickets):
            logger.error("Failed to prepare reset operations")
            return
        
        logger.info("=" * 80)
        logger.info("SPECIFIC TICKET RESET SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Target ticket IDs: {len(reset_tool.target_ticket_ids)}")
        logger.info(f"Tickets found in database: {len(target_tickets)}")
        logger.info(f"Reset operations prepared: {len(reset_tool.reset_operations)}")
        logger.info("")
        
        # Show target ticket IDs
        logger.info("Target Ticket IDs:")
        for i, ticket_id in enumerate(reset_tool.target_ticket_ids):
            logger.info(f"  {i+1}. {ticket_id}")
        
        logger.info("")
        logger.info("RESET OPERATIONS:")
        logger.info("  - Remove all LLM-generated fields (priority, urgency, stages, etc.)")
        logger.info("  - Set thread.subject_norm to null")
        logger.info("  - Set all message content to null")
        logger.info("  - Set all 'to' addresses to null")
        logger.info("  - Set company 'from' addresses to null")
        logger.info("  - Preserve customer information and original fields")
        
        # First run dry run
        logger.info("")
        logger.info("=" * 50)
        logger.info("DRY RUN - Testing reset operations")
        logger.info("=" * 50)
        
        success_count, error_count = reset_tool.execute_reset_operations(dry_run=True)
        
        if error_count > 0:
            logger.error(f"Dry run found {error_count} potential errors. Stopping.")
            return
        
        logger.info("")
        logger.info("Dry run completed successfully!")
        logger.info("")
        
        # Ask for confirmation
        print("\n" + "=" * 80)
        print("CONFIRMATION REQUIRED")
        print("=" * 80)
        print(f"This will reset {len(reset_tool.reset_operations)} specific tickets.")
        print("All LLM-generated content will be removed.")
        print("All message content and 'to' addresses will be set to null.")
        print("Original fields (dominant_topic, subtopics, etc.) will be preserved.")
        print("")
        print("Target ticket IDs:")
        for ticket_id in reset_tool.target_ticket_ids:
            print(f"  - {ticket_id}")
        print("")
        
        confirm = input("Do you want to proceed with the reset? (yes/no): ").strip().lower()
        
        if confirm != 'yes':
            logger.info("Reset cancelled by user.")
            return
        
        # Execute actual reset
        logger.info("")
        logger.info("=" * 50)
        logger.info("EXECUTING RESET OPERATIONS")
        logger.info("=" * 50)
        
        success_count, error_count = reset_tool.execute_reset_operations(dry_run=False)
        
        # Verify results
        logger.info("")
        logger.info("=" * 50)
        logger.info("VERIFYING RESULTS")
        logger.info("=" * 50)
        
        verification_success = reset_tool.verify_reset_results()
        
        # Export report
        logger.info("")
        logger.info("=" * 50)
        logger.info("EXPORTING REPORT")
        logger.info("=" * 50)
        
        report_file = reset_tool.export_reset_report()
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("RESET OPERATION COMPLETE")
        logger.info("=" * 80)
        logger.info(f"Target tickets: {len(reset_tool.target_ticket_ids)}")
        logger.info(f"Tickets processed: {len(reset_tool.reset_operations)}")
        logger.info(f"Successful resets: {success_count}")
        logger.info(f"Errors: {error_count}")
        logger.info(f"Verification: {'PASSED' if verification_success else 'FAILED'}")
        if report_file:
            logger.info(f"Report file: {report_file}")
        logger.info(f"Log file: {LOG_FILE}")
        
    except Exception as e:
        logger.error(f"Unexpected error in main: {e}")
    finally:
        reset_tool.cleanup()

if __name__ == "__main__":
    main()
