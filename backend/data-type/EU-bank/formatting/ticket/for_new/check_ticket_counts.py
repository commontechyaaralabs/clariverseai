#!/usr/bin/env python3
"""
Check the actual ticket counts in the database to understand the processing numbers.
This will help explain why you're seeing 667 batches and 2000 tickets when only 815 should be processed.
"""

import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

# MongoDB setup
MONGO_URI = os.getenv("MONGO_CONNECTION_STRING")
DB_NAME = "sparzaai"
TICKET_COLLECTION = "tickets"

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def check_ticket_counts():
    """Check various ticket counts to understand the processing numbers"""
    try:
        # Connect to MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client[DB_NAME]
        ticket_col = db[TICKET_COLLECTION]
        
        logger.info("Connected to MongoDB successfully")
        
        # Get various counts
        total_tickets = ticket_col.count_documents({})
        
        # Tickets processed by LLM (new tracking)
        tickets_processed_by_llm = ticket_col.count_documents({"llm_processed": True})
        
        # Tickets with some LLM fields
        tickets_with_some_llm_fields = ticket_col.count_documents({
            "$or": [
                {"title": {"$exists": True, "$ne": None, "$ne": ""}},
                {"priority": {"$exists": True, "$ne": None, "$ne": ""}},
                {"assigned_team_email": {"$exists": True, "$ne": None, "$ne": ""}},
                {"ticket_summary": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        
        # Tickets with ALL LLM fields
        tickets_with_all_llm_fields = ticket_col.count_documents({
            "$and": [
                {"title": {"$exists": True, "$ne": None, "$ne": ""}},
                {"priority": {"$exists": True, "$ne": None, "$ne": ""}},
                {"assigned_team_email": {"$exists": True, "$ne": None, "$ne": ""}},
                {"ticket_summary": {"$exists": True, "$ne": None, "$ne": ""}},
                {"resolution_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"overall_sentiment": {"$exists": True, "$ne": None, "$ne": ""}},
                {"ticket_raised": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_status": {"$exists": True, "$ne": None, "$ne": ""}},
                {"action_pending_from": {"$exists": True, "$ne": None, "$ne": ""}},
                {"next_action_suggestion": {"$exists": True, "$ne": None, "$ne": ""}},
                {"sentiment": {"$exists": True, "$ne": None, "$ne": ""}}
            ]
        })
        
        # Tickets that need processing (using the same query as the main script)
        query = {
            "$and": [
                {"_id": {"$exists": True}},
                {"thread": {"$exists": True}},
                # Must be missing ALL core LLM fields
                {
                    "$and": [
                        {"title": {"$exists": False}},
                        {"priority": {"$exists": False}},
                        {"assigned_team_email": {"$exists": False}},
                        {"ticket_summary": {"$exists": False}},
                        {"resolution_status": {"$exists": False}},
                        {"overall_sentiment": {"$exists": False}},
                        {"ticket_raised": {"$exists": False}},
                        {"action_pending_status": {"$exists": False}},
                        {"action_pending_from": {"$exists": False}},
                        {"next_action_suggestion": {"$exists": False}},
                        {"sentiment": {"$exists": False}}
                    ]
                }
            ]
        }
        
        tickets_needing_processing = ticket_col.count_documents(query)
        
        # Calculate batch information
        BATCH_SIZE = 3  # From the script
        total_batches = (tickets_needing_processing + BATCH_SIZE - 1) // BATCH_SIZE
        
        # Print detailed results
        print("="*80)
        print("DETAILED TICKET COUNT ANALYSIS")
        print("="*80)
        print(f"📊 DATABASE TOTALS:")
        print(f"   Total tickets in database: {total_tickets:,}")
        print()
        print(f"🤖 LLM PROCESSING STATUS:")
        print(f"   Tickets processed by LLM (llm_processed=True): {tickets_processed_by_llm:,}")
        print(f"   Tickets with some LLM fields: {tickets_with_some_llm_fields:,}")
        print(f"   Tickets with ALL LLM fields: {tickets_with_all_llm_fields:,}")
        print()
        print(f"🎯 PROCESSING TARGET:")
        print(f"   Tickets needing processing: {tickets_needing_processing:,}")
        print(f"   Batch size: {BATCH_SIZE}")
        print(f"   Total batches needed: {total_batches:,}")
        print()
        
        # Explain the numbers
        print("="*80)
        print("EXPLANATION OF NUMBERS:")
        print("="*80)
        
        if tickets_needing_processing == 815:
            print("✅ CORRECT: 815 tickets need processing")
        else:
            print(f"⚠️  EXPECTED: 815 tickets need processing")
            print(f"   ACTUAL: {tickets_needing_processing} tickets need processing")
        
        print(f"📦 BATCH CALCULATION:")
        print(f"   {tickets_needing_processing} tickets ÷ {BATCH_SIZE} per batch = {total_batches} batches")
        
        if total_batches == 667:
            print("✅ This matches the 667 batches you mentioned")
        else:
            print(f"⚠️  Expected 667 batches, but calculation shows {total_batches}")
        
        print()
        print("🔍 WHY YOU MIGHT SEE 2000 TICKETS:")
        print("   - The old performance monitor had a hardcoded 2000 target")
        print("   - This has been fixed in the updated script")
        print("   - The script now uses the actual ticket count for progress reporting")
        
        # Show some examples
        if tickets_needing_processing > 0:
            print()
            print("="*80)
            print("SAMPLE TICKETS THAT NEED PROCESSING:")
            print("="*80)
            
            sample_tickets = list(ticket_col.find(query).limit(3))
            for i, ticket in enumerate(sample_tickets, 1):
                print(f"\nSample {i}:")
                print(f"   ID: {ticket.get('_id')}")
                print(f"   Dominant Topic: {ticket.get('dominant_topic', 'N/A')}")
                print(f"   Urgency: {ticket.get('urgency', 'N/A')}")
                print(f"   Message Count: {ticket.get('thread', {}).get('message_count', 'N/A')}")
                print(f"   Has LLM Processed Flag: {'llm_processed' in ticket}")
        
        client.close()
        
    except Exception as e:
        logger.error(f"Error checking ticket counts: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("Analyzing ticket counts to explain processing numbers...")
    success = check_ticket_counts()
    if success:
        print("\nAnalysis completed successfully!")
    else:
        print("\nAnalysis failed!")
