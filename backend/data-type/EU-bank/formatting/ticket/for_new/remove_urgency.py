#!/usr/bin/env python3
"""
Removes the `urgency` field from all tickets in the database.
- Unsets the urgency field from all documents
- Uses bulk updates for efficiency

BEFORE RUNNING:
- Set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your environment or .env.
- This script removes the 'urgency' field from all docs when DRY_RUN = False.
"""

import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ---------- CONFIG ----------
DRY_RUN = False   # Set to False to actually update the DB
BATCH_SIZE = 1000  # bulk_write batch size

# ---------- SETUP ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("remove_urgency")

load_dotenv()
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")
mongo_database_name = os.getenv("MONGO_DATABASE_NAME")

if not mongo_connection_string or not mongo_database_name:
    raise SystemExit("Please set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in environment / .env")

client = MongoClient(mongo_connection_string)
db = client[mongo_database_name]
tickets = db["tickets"]

# ---------- helpers ----------
def chunked_iterable(iterable, size):
    it = iter(iterable)
    while True:
        chunk = []
        try:
            for _ in range(size):
                chunk.append(next(it))
        except StopIteration:
            if chunk:
                yield chunk
            break
        yield chunk

# ---------- PLAN ----------
# 1) Count tickets with urgency field
tickets_with_urgency = tickets.count_documents({"urgency": {"$exists": True}})
total_tickets = tickets.count_documents({})

logger.info(f"Total tickets in database: {total_tickets}")
logger.info(f"Tickets with urgency field: {tickets_with_urgency}")

if tickets_with_urgency == 0:
    logger.info("No tickets have urgency field. Nothing to remove.")
else:
    if DRY_RUN:
        logger.info("DRY_RUN = True -> No DB writes will be done.")
        logger.info(f"Would remove urgency field from {tickets_with_urgency} tickets")
    else:
        # 2) Create bulk operations to unset urgency field
        logger.info("Creating bulk update operations...")
        ops = []
        
        # Get all tickets that have urgency field
        cursor = tickets.find(
            {"urgency": {"$exists": True}}, 
            {"_id": 1}
        )
        
        for doc in cursor:
            ops.append(UpdateOne(
                {"_id": doc["_id"]}, 
                {"$unset": {"urgency": ""}}
            ))
        
        logger.info(f"Created {len(ops)} update operations")
        
        # 3) Execute bulk updates
        logger.info("Executing bulk updates...")
        total_matched = 0
        total_modified = 0
        
        for chunk in chunked_iterable(ops, BATCH_SIZE):
            result = tickets.bulk_write(chunk, ordered=False)
            total_matched += result.matched_count
            total_modified += result.modified_count
            logger.info(f"Bulk write executed: matched={result.matched_count} modified={result.modified_count}")
        
        logger.info(f"Total operations: matched={total_matched} modified={total_modified}")
        
        # 4) Post-check: verify removal
        logger.info("Post-update verification...")
        remaining_with_urgency = tickets.count_documents({"urgency": {"$exists": True}})
        logger.info(f"Tickets still with urgency field: {remaining_with_urgency}")
        
        if remaining_with_urgency == 0:
            logger.info("✅ Successfully removed urgency field from all tickets")
        else:
            logger.warning(f"⚠️ {remaining_with_urgency} tickets still have urgency field")

logger.info("Script finished.")

