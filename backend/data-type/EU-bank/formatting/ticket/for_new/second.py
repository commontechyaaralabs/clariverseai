#!/usr/bin/env python3
"""
Assigns `follow_up_required` field to tickets based on stage and thread.message_count.
- Uses the follow-ups categorization table pattern to determine which combinations need follow-ups
- Sets follow_up_required: "yes" for combinations that have follow-ups in the table
- Sets follow_up_required: "no" for all other combinations

BEFORE RUNNING:
- Set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your environment or .env.
- This script overwrites the 'follow_up_required' field for matching docs when DRY_RUN = False.
"""

import os
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ---------- CONFIG ----------
DRY_RUN = False   # Set to False to actually update the DB
BATCH_SIZE = 1000  # bulk_write batch size

# Follow-ups categorization matrix based on the provided table
# Keys: stage names. Values: dict {message_count: has_follow_up}
# "yes" means this stage+message_count combination requires follow-ups
FOLLOW_UP_MATRIX = {
    "Receive":      {1: False, 2: False, 4: False, 6: False, 7: False},
    "Authenticate": {1: False, 2: True,  4: True,  6: False, 7: False},
    "Categorize":   {1: False, 2: True,  4: True,  6: False, 7: False},
    "Resolution":   {1: False, 2: True,  4: True,  6: False, 7: False},
    "Escalation":   {1: False, 2: False, 4: True,  6: True,  7: True},
    "Update":       {1: False, 2: False, 4: True,  6: True,  7: True},
    "Resolved":     {1: False, 2: False, 4: True,  6: True,  7: True},
    "Close":        {1: False, 2: True,  4: True,  6: True,  7: True},
    "Report":       {1: False, 2: False, 4: False, 6: True,  7: True},
}

MSG_GROUPS = [1, 2, 4, 6, 7]   # message counts we care about
STAGE_ORDER = list(FOLLOW_UP_MATRIX.keys())  # preserve order for deterministic distribution

# ---------- SETUP ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("assign_follow_up")

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
# 1) Read actual counts per stage and message_count from DB
logger.info("Analyzing current ticket distribution...")
pipeline = [
    {"$group": {"_id": {"stage": "$stages", "msg_count": "$thread.message_count"}, "count": {"$sum": 1}}},
    {"$sort": {"_id.stage": 1, "_id.msg_count": 1}}
]

current_distribution = {}
for doc in tickets.aggregate(pipeline):
    stage = doc['_id']['stage']
    msg_count = doc['_id']['msg_count']
    count = doc['count']
    current_distribution[(stage, msg_count)] = count
    logger.info(f"Current: stage={stage}, msg_count={msg_count}, count={count}")

# 2) Generate update operations based on follow-up matrix
logger.info("Generating follow_up_required assignments...")
ops = []

# Get all tickets that have both stages and message_count
cursor = tickets.find(
    {
        "stages": {"$exists": True, "$ne": None}, 
        "thread.message_count": {"$exists": True, "$ne": None}
    }, 
    {"_id": 1, "stages": 1, "thread.message_count": 1}
)

yes_count = 0
no_count = 0

for doc in cursor:
    doc_id = doc["_id"]
    stage = doc.get("stages")
    msg_count = doc.get("thread", {}).get("message_count")
    
    # Skip if either field is missing or None
    if stage is None or msg_count is None:
        logger.warning(f"Skipping document {doc_id}: missing stage or message_count")
        continue
    
    # Check if this combination requires follow-up
    requires_follow_up = FOLLOW_UP_MATRIX.get(stage, {}).get(msg_count, False)
    follow_up_value = "yes" if requires_follow_up else "no"
    
    if requires_follow_up:
        yes_count += 1
    else:
        no_count += 1
    
    ops.append(UpdateOne(
        {"_id": doc_id}, 
        {"$set": {"follow_up_required": follow_up_value}}
    ))

logger.info(f"Planned assignments: {yes_count} tickets with follow_up_required='yes', {no_count} with 'no'")

# 3) Dry-run: show plan summary
total_planned = len(ops)
logger.info(f"TOTAL documents planned to update: {total_planned}")

if DRY_RUN:
    logger.info("DRY_RUN = True -> No DB writes will be done. Review the plan above.")
    
    # Show detailed breakdown
    logger.info("Detailed breakdown by stage and message_count:")
    for stage in STAGE_ORDER:
        for msg_count in MSG_GROUPS:
            requires_follow_up = FOLLOW_UP_MATRIX.get(stage, {}).get(msg_count, False)
            current_count = current_distribution.get((stage, msg_count), 0)
            follow_up_value = "yes" if requires_follow_up else "no"
            logger.info(f"  {stage} + {msg_count}msgs: {follow_up_value} (current docs: {current_count})")
else:
    # 4) Execute bulk updates
    logger.info("Executing bulk updates...")
    
    for chunk in chunked_iterable(ops, BATCH_SIZE):
        result = tickets.bulk_write(chunk, ordered=False)
        logger.info(f"Bulk write executed: matched={result.matched_count} modified={result.modified_count}")

    # 5) Post-check: verify assignments
    logger.info("Post-update verification: follow_up_required distribution")
    pipeline = [
        {"$group": {"_id": {"follow_up": "$follow_up_required", "stage": "$stages", "msg_count": "$thread.message_count"}, "count": {"$sum": 1}}},
        {"$sort": {"_id.stage": 1, "_id.msg_count": 1}}
    ]
    
    for doc in tickets.aggregate(pipeline):
        follow_up = doc['_id']['follow_up']
        stage = doc['_id']['stage']
        msg_count = doc['_id']['msg_count']
        count = doc['count']
        logger.info(f"stage={stage}, msg_count={msg_count}, follow_up_required={follow_up}, count={count}")

logger.info("Script finished.")
