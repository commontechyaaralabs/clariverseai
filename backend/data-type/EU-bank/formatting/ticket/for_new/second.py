#!/usr/bin/env python3
"""
Assigns `follow_up_required` field to tickets based on stage and thread.message_count.
- Uses a provided follow-up distribution matrix as "desired" counts per stage+message_count combination.
- Assigns follow_up_required: "yes" to the exact number of tickets specified in the distribution.
- Assigns follow_up_required: "no" to all remaining tickets.
- Uses deterministic assignment (seeded) and writes with bulk updates.

BEFORE RUNNING:
- Set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your environment or .env.
- This script overwrites the 'follow_up_required' field for matching docs when DRY_RUN = False.
"""

import os
import math
import random
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ---------- CONFIG ----------
DRY_RUN = False   # Set to False to actually update the DB
RANDOM_SEED = 42
BATCH_SIZE = 1000  # bulk_write batch size

# Follow-ups distribution matrix based on the provided table
# Keys: stage names. Values: dict {message_count: count_of_follow_ups}
# These are the exact counts of tickets that should have follow_up_required: "yes"
FOLLOW_UP_DISTRIBUTION = {
    "Receive":      {1: 0,  2: 0,  4: 0,  6: 0,  7: 0},
    "Authenticate": {1: 0,  2: 40, 4: 5,  6: 0,  7: 0},
    "Categorize":   {1: 0,  2: 15, 4: 2,  6: 0,  7: 0},
    "Resolution":   {1: 0,  2: 8,  4: 180, 6: 0,  7: 0},
    "Escalation":   {1: 0,  2: 0,  4: 70,  6: 35, 7: 20},
    "Update":       {1: 0,  2: 0,  4: 35,  6: 10, 7: 10},
    "Resolved":     {1: 0,  2: 0,  4: 10,  6: 80, 7: 30},
    "Close":        {1: 0,  2: 3,  4: 2,   6: 40, 7: 60},
    "Report":       {1: 0,  2: 0,  4: 0,   6: 10, 7: 30},
}

MSG_GROUPS = [1, 2, 4, 6, 7]   # message counts we care about
STAGE_ORDER = list(FOLLOW_UP_DISTRIBUTION.keys())  # preserve order for deterministic distribution

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

rnd = random.Random(RANDOM_SEED)

# ---------- helpers ----------
def scale_counts(desired_counts, actual_total):
    """
    Scale list of desired_counts (integers) to sum exactly to actual_total.
    Returns a list of integers with same length whose sum == actual_total.
    """
    if actual_total == 0:
        return [0] * len(desired_counts)

    sum_desired = sum(desired_counts)
    if sum_desired == 0:
        # Nothing desired: return all zeros (no assignments)
        return [0] * len(desired_counts)

    # initial float scaled values
    float_scaled = [c * (actual_total / sum_desired) for c in desired_counts]
    floored = [math.floor(x) for x in float_scaled]
    remaining = actual_total - sum(floored)

    # distribute the remaining based on fractional parts (largest fractions first)
    fractions = [(i, float_scaled[i] - floored[i]) for i in range(len(desired_counts))]
    fractions.sort(key=lambda x: x[1], reverse=True)

    for i in range(remaining):
        idx = fractions[i][0]
        floored[idx] += 1

    return floored

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

# 2) For each stage+message_count combination, assign follow_up_required based on distribution
global_plan = {}  # (stage, msg_count) -> count_to_assign_follow_up_yes
total_follow_ups_planned = 0

logger.info("Checking distribution vs actual database counts...")
for stage in STAGE_ORDER:
    for msg_count in MSG_GROUPS:
        desired_follow_ups = FOLLOW_UP_DISTRIBUTION[stage].get(msg_count, 0)
        actual_total = current_distribution.get((stage, msg_count), 0)
        
        if actual_total == 0:
            assigned_follow_ups = 0
            if desired_follow_ups > 0:
                logger.warning(f"MISSING: stage={stage}, msg_count={msg_count} - wanted {desired_follow_ups} follow-ups but 0 tickets exist in DB")
        else:
            # Scale the desired follow-up count to match available documents
            assigned_follow_ups = min(desired_follow_ups, actual_total)
            if desired_follow_ups > actual_total:
                logger.warning(f"LIMITED: stage={stage}, msg_count={msg_count} - wanted {desired_follow_ups} follow-ups but only {actual_total} tickets available")
        
        global_plan[(stage, msg_count)] = assigned_follow_ups
        total_follow_ups_planned += assigned_follow_ups
        
        if assigned_follow_ups > 0:
            logger.info(f"Planned for stage={stage}, msg_count={msg_count}: {assigned_follow_ups} follow_up_required='yes' (available docs: {actual_total})")

logger.info(f"TOTAL follow-up assignments planned: {total_follow_ups_planned}")
logger.info(f"Expected total from distribution table: 795")
logger.info(f"Difference: {795 - total_follow_ups_planned} follow-ups cannot be assigned due to missing tickets")

if DRY_RUN:
    logger.info("DRY_RUN = True -> No DB writes will be done. Review the plan above.")
    
    # Show detailed breakdown
    logger.info("Detailed breakdown by stage and message_count:")
    for stage in STAGE_ORDER:
        for msg_count in MSG_GROUPS:
            planned_follow_ups = global_plan.get((stage, msg_count), 0)
            current_count = current_distribution.get((stage, msg_count), 0)
            logger.info(f"  {stage} + {msg_count}msgs: {planned_follow_ups} follow_up='yes', {current_count - planned_follow_ups} follow_up='no' (total: {current_count})")
else:
    # 3) For each stage+message_count combination, fetch IDs, shuffle, and assign follow_up_required
    logger.info("Executing follow_up_required assignments...")
    
    for stage in STAGE_ORDER:
        for msg_count in MSG_GROUPS:
            planned_follow_ups = global_plan.get((stage, msg_count), 0)
            current_count = current_distribution.get((stage, msg_count), 0)
            
            if current_count == 0:
                continue
            
            # Fetch IDs for this stage+message_count combination
            cursor = tickets.find(
                {"stages": stage, "thread.message_count": msg_count}, 
                {"_id": 1}
            )
            ids = [doc["_id"] for doc in cursor]
            
            if not ids:
                logger.warning(f"No documents found for stage={stage}, msg_count={msg_count}; skipping.")
                continue
            
            if len(ids) < planned_follow_ups:
                logger.warning(f"Not enough documents for stage={stage}, msg_count={msg_count}: {len(ids)} available, {planned_follow_ups} planned. Using available count.")
                planned_follow_ups = len(ids)
            
            # Shuffle deterministically
            rnd.shuffle(ids)
            
            # Create bulk operations
            ops = []
            
            # Assign "yes" to the first planned_follow_ups documents
            for i in range(planned_follow_ups):
                ops.append(UpdateOne(
                    {"_id": ids[i]}, 
                    {"$set": {"follow_up_required": "yes"}}
                ))
            
            # Assign "no" to the remaining documents
            for i in range(planned_follow_ups, len(ids)):
                ops.append(UpdateOne(
                    {"_id": ids[i]}, 
                    {"$set": {"follow_up_required": "no"}}
                ))
            
            # Execute bulk updates
            if ops:
                result = tickets.bulk_write(ops, ordered=False)
                logger.info(f"Bulk write executed for {stage}+{msg_count}: matched={result.matched_count} modified={result.modified_count}")
    
    # Also handle any tickets that don't have stages or message_count set
    logger.info("Handling tickets without proper stage/message_count...")
    cursor = tickets.find(
        {
            "$or": [
                {"stages": {"$exists": False}},
                {"stages": None},
                {"thread.message_count": {"$exists": False}},
                {"thread.message_count": None}
            ]
        },
        {"_id": 1}
    )
    
    orphan_ids = [doc["_id"] for doc in cursor]
    if orphan_ids:
        ops = []
        for _id in orphan_ids:
            ops.append(UpdateOne(
                {"_id": _id}, 
                {"$set": {"follow_up_required": "no"}}
            ))
        
        if ops:
            result = tickets.bulk_write(ops, ordered=False)
            logger.info(f"Bulk write executed for orphan tickets: matched={result.matched_count} modified={result.modified_count}")

    # 4) Post-check: verify assignments
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
