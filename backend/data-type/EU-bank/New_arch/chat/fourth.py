import os
from pymongo import MongoClient, UpdateOne
import logging
from dotenv import load_dotenv
import random
from datetime import datetime, timedelta

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING', "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME', "sparzaai")

# Target distribution
TARGET_FOLLOWUP_PERCENTAGE = 35  # 35% should have "yes"
BATCH_SIZE = 1000

class FollowUpManager:
    def __init__(self):
        self.client = None
        self.db = None
        self.collection = None

    def connect(self):
        """Connect to MongoDB"""
        try:
            self.client = MongoClient(MONGO_CONNECTION_STRING)
            self.db = self.client[MONGO_DATABASE_NAME]
            self.collection = self.db["chatmessages"]
            
            # Test connection
            self.client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {e}")
            return False

    def get_current_followup_stats(self):
        """Get current follow_up_required field statistics"""
        try:
            # Total documents
            total_docs = self.collection.count_documents({})
            
            # Documents with follow_up_required field
            docs_with_field = self.collection.count_documents({"follow_up_required": {"$exists": True}})
            
            # Documents with follow_up_required = "yes"
            docs_with_yes = self.collection.count_documents({"follow_up_required": "yes"})
            
            # Documents with follow_up_required = "no"
            docs_with_no = self.collection.count_documents({"follow_up_required": "no"})
            
            # Documents without the field
            docs_without_field = total_docs - docs_with_field
            
            current_yes_percentage = (docs_with_yes / total_docs * 100) if total_docs > 0 else 0
            current_no_percentage = (docs_with_no / total_docs * 100) if total_docs > 0 else 0
            
            logger.info("=== CURRENT FOLLOW-UP STATISTICS ===")
            logger.info(f"Total documents: {total_docs}")
            logger.info(f"Documents with follow_up_required field: {docs_with_field}")
            logger.info(f"Documents without follow_up_required field: {docs_without_field}")
            logger.info(f"Current 'yes' count: {docs_with_yes} ({current_yes_percentage:.2f}%)")
            logger.info(f"Current 'no' count: {docs_with_no} ({current_no_percentage:.2f}%)")
            
            return {
                "total_docs": total_docs,
                "docs_with_field": docs_with_field,
                "docs_without_field": docs_without_field,
                "docs_with_yes": docs_with_yes,
                "docs_with_no": docs_with_no,
                "current_yes_percentage": current_yes_percentage,
                "current_no_percentage": current_no_percentage
            }
        except Exception as e:
            logger.error(f"Failed to get current stats: {e}")
            return {}

    def calculate_required_distribution(self, stats):
        """Calculate how many documents need to be updated"""
        try:
            total_docs = stats["total_docs"]
            current_yes = stats["docs_with_yes"]
            current_no = stats["docs_with_no"]
            docs_without_field = stats["docs_without_field"]
            
            # Target counts
            target_yes = int(total_docs * TARGET_FOLLOWUP_PERCENTAGE / 100)
            target_no = total_docs - target_yes
            
            # Calculate what we need to do
            need_more_yes = max(0, target_yes - current_yes)
            need_more_no = max(0, target_no - current_no)
            
            # If we have excess yes, some need to be changed to no
            excess_yes = max(0, current_yes - target_yes)
            
            logger.info("\n=== REQUIRED DISTRIBUTION CALCULATION ===")
            logger.info(f"Target 'yes' count: {target_yes} ({TARGET_FOLLOWUP_PERCENTAGE}%)")
            logger.info(f"Target 'no' count: {target_no} ({100 - TARGET_FOLLOWUP_PERCENTAGE}%)")
            logger.info(f"Documents without field to assign: {docs_without_field}")
            logger.info(f"Additional 'yes' needed: {need_more_yes}")
            logger.info(f"Additional 'no' needed: {need_more_no}")
            logger.info(f"Excess 'yes' to change to 'no': {excess_yes}")
            
            return {
                "target_yes": target_yes,
                "target_no": target_no,
                "need_more_yes": need_more_yes,
                "need_more_no": need_more_no,
                "excess_yes": excess_yes,
                "docs_without_field": docs_without_field
            }
        except Exception as e:
            logger.error(f"Failed to calculate distribution: {e}")
            return {}

    def generate_follow_up_data(self):
        """Generate follow-up date and reason for documents with follow_up_required = yes"""
        follow_up_reasons = [
            "Awaiting customer response",
            "Pending additional documentation", 
            "Requires manager approval",
            "Escalated to technical team",
            "Customer requested callback",
            "Pending verification process",
            "Waiting for system update",
            "Requires further investigation",
            "Customer availability constraint",
            "Pending policy clarification"
        ]
        
        # Generate random follow-up date (1-30 days from now)
        base_date = datetime.now()
        random_days = random.randint(1, 30)
        follow_up_date = base_date + timedelta(days=random_days)
        
        return {
            "follow_up_date": follow_up_date,
            "follow_up_reason": random.choice(follow_up_reasons)
        }

    def update_follow_up_fields(self, distribution, dry_run=True):
        """Update follow_up_required field according to the calculated distribution"""
        try:
            operations = []
            
            # Step 1: Handle excess "yes" - change some to "no"
            if distribution["excess_yes"] > 0:
                logger.info(f"\nStep 1: Changing {distribution['excess_yes']} records from 'yes' to 'no'")
                
                # Find documents with follow_up_required = "yes" randomly
                excess_docs = list(self.collection.aggregate([
                    {"$match": {"follow_up_required": "yes"}},
                    {"$sample": {"size": distribution["excess_yes"]}}
                ]))
                
                for doc in excess_docs:
                    operations.append(UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "follow_up_required": "no",
                            "follow_up_date": None,
                            "follow_up_reason": None
                        }}
                    ))
            
            # Step 2: Assign fields to documents without follow_up_required field
            docs_without_field = distribution["docs_without_field"]
            if docs_without_field > 0:
                logger.info(f"\nStep 2: Assigning follow_up_required to {docs_without_field} documents")
                
                # Get documents without the field
                unassigned_docs = list(self.collection.find(
                    {"follow_up_required": {"$exists": False}},
                    {"_id": 1}
                ))
                
                # Shuffle for random distribution
                random.shuffle(unassigned_docs)
                
                # Calculate how many of the unassigned should be "yes"
                remaining_yes_needed = distribution["need_more_yes"]
                remaining_no_needed = distribution["need_more_no"]
                
                yes_from_unassigned = min(remaining_yes_needed, docs_without_field)
                no_from_unassigned = docs_without_field - yes_from_unassigned
                
                logger.info(f"  Assigning 'yes' to: {yes_from_unassigned} documents")
                logger.info(f"  Assigning 'no' to: {no_from_unassigned} documents")
                
                # Assign "yes" to first batch
                for i in range(yes_from_unassigned):
                    follow_up_data = self.generate_follow_up_data()
                    operations.append(UpdateOne(
                        {"_id": unassigned_docs[i]["_id"]},
                        {"$set": {
                            "follow_up_required": "yes",
                            "follow_up_date": follow_up_data["follow_up_date"],
                            "follow_up_reason": follow_up_data["follow_up_reason"]
                        }}
                    ))
                
                # Assign "no" to remaining batch
                for i in range(yes_from_unassigned, docs_without_field):
                    operations.append(UpdateOne(
                        {"_id": unassigned_docs[i]["_id"]},
                        {"$set": {
                            "follow_up_required": "no",
                            "follow_up_date": None,
                            "follow_up_reason": None
                        }}
                    ))
            
            # Step 3: Handle any remaining adjustments needed
            remaining_yes_needed = distribution["need_more_yes"] - (docs_without_field - (docs_without_field - min(distribution["need_more_yes"], docs_without_field)))
            if remaining_yes_needed > 0:
                logger.info(f"\nStep 3: Converting {remaining_yes_needed} 'no' records to 'yes'")
                
                # Find documents with follow_up_required = "no" randomly
                no_docs = list(self.collection.aggregate([
                    {"$match": {"follow_up_required": "no"}},
                    {"$sample": {"size": remaining_yes_needed}}
                ]))
                
                for doc in no_docs:
                    follow_up_data = self.generate_follow_up_data()
                    operations.append(UpdateOne(
                        {"_id": doc["_id"]},
                        {"$set": {
                            "follow_up_required": "yes",
                            "follow_up_date": follow_up_data["follow_up_date"],
                            "follow_up_reason": follow_up_data["follow_up_reason"]
                        }}
                    ))
            
            logger.info(f"\nTotal operations to perform: {len(operations)}")
            
            if not dry_run and operations:
                # Execute operations in batches
                total_updated = 0
                for i in range(0, len(operations), BATCH_SIZE):
                    batch = operations[i:i + BATCH_SIZE]
                    result = self.collection.bulk_write(batch)
                    total_updated += result.modified_count
                    logger.info(f"Processed batch {i//BATCH_SIZE + 1}: {result.modified_count} documents updated")
                
                logger.info(f"Total documents updated: {total_updated}")
                return total_updated
            elif dry_run:
                logger.info("[DRY RUN] Operations planned but not executed")
                return len(operations)
            else:
                logger.info("No operations needed")
                return 0
                
        except Exception as e:
            logger.error(f"Failed to update follow-up fields: {e}")
            return 0

    def verify_final_distribution(self):
        """Verify the final distribution matches the target"""
        try:
            logger.info("\n=== VERIFICATION ===")
            final_stats = self.get_current_followup_stats()
            
            actual_yes_percentage = final_stats["current_yes_percentage"]
            actual_no_percentage = final_stats["current_no_percentage"]
            
            logger.info(f"Target: {TARGET_FOLLOWUP_PERCENTAGE}% yes, {100 - TARGET_FOLLOWUP_PERCENTAGE}% no")
            logger.info(f"Actual: {actual_yes_percentage:.2f}% yes, {actual_no_percentage:.2f}% no")
            
            # Check if within acceptable range (±1%)
            yes_diff = abs(actual_yes_percentage - TARGET_FOLLOWUP_PERCENTAGE)
            success = yes_diff <= 1.0
            
            if success:
                logger.info("✓ Distribution is within acceptable range!")
            else:
                logger.warning(f"⚠ Distribution differs by {yes_diff:.2f}% from target")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to verify distribution: {e}")
            return False

    def close_connection(self):
        """Close MongoDB connection"""
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed")

def main():
    """Main function to execute the follow-up field management"""
    manager = FollowUpManager()
    
    try:
        # Connect to database
        if not manager.connect():
            logger.error("Failed to connect to database. Exiting.")
            return
        
        # Get current statistics
        current_stats = manager.get_current_followup_stats()
        if not current_stats:
            return
        
        # Calculate required distribution
        distribution = manager.calculate_required_distribution(current_stats)
        if not distribution:
            return
        
        # Dry run first
        logger.info("\n=== DRY RUN ===")
        planned_updates = manager.update_follow_up_fields(distribution, dry_run=True)
        
        # Ask for confirmation
        response = input(f"\nProceed with updating {planned_updates} documents? (yes/no): ").lower().strip()
        
        if response == 'yes':
            logger.info("\n=== EXECUTING UPDATES ===")
            actual_updates = manager.update_follow_up_fields(distribution, dry_run=False)
            
            # Verify final distribution
            manager.verify_final_distribution()
            
            logger.info(f"\n✓ Process completed! Updated {actual_updates} documents.")
        else:
            logger.info("Operation cancelled by user.")
            
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        manager.close_connection()

if __name__ == "__main__":
    main()
