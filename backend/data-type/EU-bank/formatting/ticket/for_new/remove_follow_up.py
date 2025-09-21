#!/usr/bin/env python3
"""
Removes the 'follow_up_required' field from all tickets in the collection.
This is useful for cleaning up or resetting the follow_up_required assignments.

BEFORE RUNNING:
- Set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your environment or .env.
- This script will remove the 'follow_up_required' field from ALL documents when DRY_RUN = False.
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
logger = logging.getLogger("remove_follow_up")

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

# ---------- MAIN LOGIC ----------
# 1) Count documents that have the follow_up_required field
docs_with_field = tickets.count_documents({"follow_up_required": {"$exists": True}})
total_docs = tickets.count_documents({})

logger.info(f"Total documents in collection: {total_docs}")
logger.info(f"Documents with 'follow_up_required' field: {docs_with_field}")

if docs_with_field == 0:
    logger.info("No documents have the 'follow_up_required' field. Nothing to remove.")
    exit(0)

# 2) Generate update operations to remove the field
logger.info("Generating operations to remove 'follow_up_required' field...")
ops = []

# Get all document IDs that have the follow_up_required field
cursor = tickets.find(
    {"follow_up_required": {"$exists": True}}, 
    {"_id": 1}
)

for doc in cursor:
    doc_id = doc["_id"]
    ops.append(UpdateOne(
        {"_id": doc_id}, 
        {"$unset": {"follow_up_required": ""}}
    ))

logger.info(f"Generated {len(ops)} operations to remove 'follow_up_required' field")

# 3) Dry-run: show plan summary
if DRY_RUN:
    logger.info("DRY_RUN = True -> No DB writes will be done. Review the plan above.")
    logger.info(f"Would remove 'follow_up_required' field from {len(ops)} documents.")
else:
    # 4) Execute bulk updates
    logger.info("Executing bulk updates to remove 'follow_up_required' field...")
    
    total_modified = 0
    for chunk in chunked_iterable(ops, BATCH_SIZE):
        result = tickets.bulk_write(chunk, ordered=False)
        total_modified += result.modified_count
        logger.info(f"Bulk write executed: matched={result.matched_count} modified={result.modified_count}")
    
    logger.info(f"Total documents modified: {total_modified}")

    # 5) Post-check: verify removal
    remaining_docs = tickets.count_documents({"follow_up_required": {"$exists": True}})
    logger.info(f"Post-update verification: {remaining_docs} documents still have 'follow_up_required' field")
    
    if remaining_docs == 0:
        logger.info("SUCCESS: All 'follow_up_required' fields have been removed!")
    else:
        logger.warning(f"WARNING: {remaining_docs} documents still have the 'follow_up_required' field")

logger.info("Script finished.")

