# EU Banking Partial Ticket Reset Script
# This script resets partially generated tickets back to their original state
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
LOG_FILE = LOG_DIR / f"reset_partial_tickets_{timestamp}.log"

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

class PartialTicketReset:
    """Reset partially generated tickets back to original state"""
    
    def __init__(self):
        self.client = None
        self.db = None
        self.ticket_col = None
        self.partial_tickets = []
        self.reset_operations = []
        
        # Fields that should be reset to null/empty (LLM-generated content)
        self.reset_fields = [
            'thread.subject_norm',
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
        
        # Fields to preserve (original data)
        self.preserve_fields = [
            'provider',
            'thread.thread_id',
            'thread.thread_key',
            'thread.participants',
            'thread.first_message_at',
            'thread.last_message_at',
            'thread.message_count',
            'messages',
            'domain',
            'dominant_cluster_label',
            'dominant_topic',
            'kmeans_cluster_id',
            'kmeans_cluster_keyphrase',
            'processed_at',
            'subcluster_id',
            'subcluster_label',
            'subtopics'
        ]
    
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
    
    def find_partial_tickets(self):
        """Find tickets with partial LLM-generated content"""
        logger.info("Finding partially generated tickets...")
        
        try:
            # Find tickets that have some LLM-generated fields but not all
            partial_criteria = {
                "$and": [
                    # Must have at least one LLM-generated field
                    {"$or": [
                        {"thread.subject_norm": {"$exists": True, "$ne": "", "$ne": None}},
                        {"priority": {"$exists": True, "$ne": "", "$ne": None}},
                        {"urgency": {"$exists": True}},
                        {"stages": {"$exists": True, "$ne": "", "$ne": None}},
                        {"ticket_summary": {"$exists": True, "$ne": "", "$ne": None}},
                        {"resolution_status": {"$exists": True, "$ne": "", "$ne": None}},
                        {"overall_sentiment": {"$exists": True}},
                        {"ticket_raised": {"$exists": True, "$ne": "", "$ne": None}},
                        {"ticket_source": {"$exists": True, "$ne": "", "$ne": None}},
                        {"assigned_team": {"$exists": True, "$ne": "", "$ne": None}}
                    ]},
                    # But not fully generated (missing at least one required field)
                    {"$or": [
                        {"messages.0.body.text.plain": {"$in": [None, ""]}},
                        {"thread.subject_norm": {"$in": [None, ""]}},
                        {"priority": {"$exists": False}},
                        {"urgency": {"$exists": False}},
                        {"stages": {"$exists": False}},
                        {"ticket_summary": {"$exists": False}},
                        {"resolution_status": {"$exists": False}},
                        {"overall_sentiment": {"$exists": False}},
                        {"ticket_raised": {"$exists": False}},
                        {"ticket_source": {"$exists": False}},
                        {"assigned_team": {"$exists": False}}
                    ]}
                ]
            }
            
            self.partial_tickets = list(self.ticket_col.find(partial_criteria))
            logger.info(f"Found {len(self.partial_tickets)} partially generated tickets")
            
            # Log sample tickets
            for i, ticket in enumerate(self.partial_tickets[:5]):
                logger.info(f"Sample partial ticket {i+1}: {ticket['_id']}")
                logger.info(f"  Topic: {ticket.get('dominant_topic', 'N/A')}")
                logger.info(f"  Present fields: {self._get_present_generated_fields(ticket)}")
                logger.info(f"  Missing fields: {self._get_missing_generated_fields(ticket)}")
            
            return len(self.partial_tickets) > 0
            
        except Exception as e:
            logger.error(f"Error finding partial tickets: {e}")
            return False
    
    def _get_present_generated_fields(self, ticket):
        """Get list of present LLM-generated fields"""
        present = []
        for field in self.reset_fields:
            if '.' in field:
                # Handle nested fields
                parts = field.split('.')
                current = ticket
                try:
                    for part in parts:
                        current = current[part]
                    if current is not None and current != "":
                        present.append(field)
                except (KeyError, TypeError):
                    pass
            else:
                if field in ticket and ticket[field] is not None and ticket[field] != "":
                    present.append(field)
        return present
    
    def _get_missing_generated_fields(self, ticket):
        """Get list of missing LLM-generated fields"""
        missing = []
        for field in self.reset_fields:
            if '.' in field:
                # Handle nested fields
                parts = field.split('.')
                current = ticket
                try:
                    for part in parts:
                        current = current[part]
                    if current is None or current == "":
                        missing.append(field)
                except (KeyError, TypeError):
                    missing.append(field)
            else:
                if field not in ticket or ticket[field] is None or ticket[field] == "":
                    missing.append(field)
        return missing
    
    def prepare_reset_operations(self):
        """Prepare the reset operations for partial tickets"""
        logger.info("Preparing reset operations...")
        
        try:
            for ticket in self.partial_tickets:
                ticket_id = ticket['_id']
                
                # Build the unset operation (exclude fields that will be set to null)
                unset_operations = {}
                for field in self.reset_fields:
                    # Skip fields that we'll set to null instead of unset
                    if field not in ['thread.subject_norm']:
                        unset_operations[field] = ""
                
                # Build the set operation for messages and thread.subject_norm
                set_operations = {}
                
                # Reset message content to null while preserving structure
                if 'messages' in ticket and ticket['messages']:
                    updated_messages = []
                    for message in ticket['messages']:
                        updated_message = message.copy()
                        
                        # Reset message body content
                        if 'body' in updated_message and 'text' in updated_message['body']:
                            updated_message['body']['text']['plain'] = None
                        
                        # Reset ticket title in headers
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
                
                # Reset thread.subject_norm to null
                set_operations['thread.subject_norm'] = None
                
                # Reset thread participants "to" address to null
                if 'thread' in ticket and 'participants' in ticket['thread']:
                    updated_participants = ticket['thread']['participants'].copy()
                    for participant in updated_participants:
                        if participant.get('type') == 'to':
                            participant['name'] = None
                            participant['email'] = None
                    set_operations['thread.participants'] = updated_participants
                
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
                        # First, unset the fields
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
                    if (i + 1) % 5 == 0:
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
            # Check how many partial tickets remain
            remaining_partial = list(self.ticket_col.find({
                "$and": [
                    {"$or": [
                        {"thread.subject_norm": {"$exists": True, "$ne": "", "$ne": None}},
                        {"priority": {"$exists": True, "$ne": "", "$ne": None}},
                        {"urgency": {"$exists": True}},
                        {"stages": {"$exists": True, "$ne": "", "$ne": None}},
                        {"ticket_summary": {"$exists": True, "$ne": "", "$ne": None}},
                        {"resolution_status": {"$exists": True, "$ne": "", "$ne": None}},
                        {"overall_sentiment": {"$exists": True}},
                        {"ticket_raised": {"$exists": True, "$ne": "", "$ne": None}},
                        {"ticket_source": {"$exists": True, "$ne": "", "$ne": None}},
                        {"assigned_team": {"$exists": True, "$ne": "", "$ne": None}}
                    ]},
                    {"$or": [
                        {"messages.0.body.text.plain": {"$in": [None, ""]}},
                        {"thread.subject_norm": {"$in": [None, ""]}},
                        {"priority": {"$exists": False}},
                        {"urgency": {"$exists": False}},
                        {"stages": {"$exists": False}},
                        {"ticket_summary": {"$exists": False}},
                        {"resolution_status": {"$exists": False}},
                        {"overall_sentiment": {"$exists": False}},
                        {"ticket_raised": {"$exists": False}},
                        {"ticket_source": {"$exists": False}},
                        {"assigned_team": {"$exists": False}}
                    ]}
                ]
            }))
            
            # Check tickets with no generated content
            clean_tickets = list(self.ticket_col.find({
                "$and": [
                    {"messages.0.body.text.plain": {"$in": [None, ""]}},
                    {"thread.subject_norm": {"$in": [None, ""]}},
                    {"priority": {"$exists": False}},
                    {"urgency": {"$exists": False}},
                    {"stages": {"$exists": False}},
                    {"ticket_summary": {"$exists": False}},
                    {"resolution_status": {"$exists": False}},
                    {"overall_sentiment": {"$exists": False}},
                    {"ticket_raised": {"$exists": False}},
                    {"ticket_source": {"$exists": False}},
                    {"assigned_team": {"$exists": False}}
                ]
            }))
            
            logger.info(f"Verification results:")
            logger.info(f"  Remaining partial tickets: {len(remaining_partial)}")
            logger.info(f"  Clean tickets (no generated content): {len(clean_tickets)}")
            logger.info(f"  Original partial tickets: {len(self.partial_tickets)}")
            logger.info(f"  Expected clean tickets after reset: {len(self.partial_tickets)}")
            
            if len(remaining_partial) == 0:
                logger.info("  [SUCCESS] All partial tickets have been reset successfully!")
            else:
                logger.warning(f"  [WARNING] {len(remaining_partial)} partial tickets still remain")
                
                # Show sample remaining partial tickets
                for i, ticket in enumerate(remaining_partial[:3]):
                    logger.warning(f"    Sample remaining: {ticket['_id']} - {ticket.get('dominant_topic', 'N/A')}")
            
            return len(remaining_partial) == 0
            
        except Exception as e:
            logger.error(f"Error verifying reset results: {e}")
            return False
    
    def export_reset_report(self):
        """Export a report of the reset operations"""
        try:
            report_file = f"reset_report_{timestamp}.json"
            
            report_data = {
                'reset_timestamp': datetime.now().isoformat(),
                'database': DB_NAME,
                'collection': TICKET_COLLECTION,
                'total_partial_tickets': len(self.partial_tickets),
                'reset_operations': len(self.reset_operations),
                'reset_fields': self.reset_fields,
                'preserve_fields': self.preserve_fields,
                'sample_tickets': []
            }
            
            # Add sample tickets to report
            for i, operation in enumerate(self.reset_operations[:10]):
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
    """Main function to run the partial ticket reset"""
    logger.info("EU Banking Partial Ticket Reset Script Starting...")
    
    reset_tool = PartialTicketReset()
    
    try:
        # Initialize database
        if not reset_tool.init_database():
            logger.error("Cannot proceed without database connection")
            return
        
        # Find partial tickets
        if not reset_tool.find_partial_tickets():
            logger.info("No partial tickets found. Nothing to reset.")
            return
        
        # Prepare reset operations
        if not reset_tool.prepare_reset_operations():
            logger.error("Failed to prepare reset operations")
            return
        
        logger.info("=" * 80)
        logger.info("PARTIAL TICKET RESET SUMMARY")
        logger.info("=" * 80)
        logger.info(f"Found {len(reset_tool.partial_tickets)} partial tickets to reset")
        logger.info(f"Fields to reset: {len(reset_tool.reset_fields)}")
        logger.info(f"Fields to preserve: {len(reset_tool.preserve_fields)}")
        logger.info("")
        
        # Show sample tickets that will be reset
        logger.info("Sample tickets to be reset:")
        for i, operation in enumerate(reset_tool.reset_operations[:5]):
            logger.info(f"  {i+1}. {operation['ticket_id']} - {operation['original_ticket']['dominant_topic']}")
        if len(reset_tool.reset_operations) > 5:
            logger.info(f"  ... and {len(reset_tool.reset_operations) - 5} more")
        
        logger.info("")
        logger.info("RESET FIELDS:")
        for field in reset_tool.reset_fields:
            logger.info(f"  - {field}")
        
        logger.info("")
        logger.info("PRESERVE FIELDS:")
        for field in reset_tool.preserve_fields:
            logger.info(f"  - {field}")
        
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
        print(f"This will reset {len(reset_tool.reset_operations)} partial tickets.")
        print("All LLM-generated content will be removed.")
        print("Original fields (dominant_topic, subtopics, etc.) will be preserved.")
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
