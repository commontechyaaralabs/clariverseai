#!/usr/bin/env python3
"""
Deterministic assignment of `stages` field to tickets based on thread.message_count.
- Uses a provided stage x message_count matrix as "desired" counts.
- Scales per-group desired counts to match the actual number of docs in that group.
- Assigns stages deterministically/randomly (seeded) and writes with bulk updates.

BEFORE RUNNING:
- Set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your environment or .env.
- This script overwrites the 'stages' field for matching docs when DRY_RUN = False.
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

# The stage x message-count matrix you provided (keeps your original numbers).
# Keys: stage names. Values: dict {message_count: count}
STAGE_MATRIX = {
    "Receive":      {1:100, 2:0,   4:0,   6:0,   7:0},
    "Authenticate": {1:0,   2:120, 4:10,  6:0,   7:0},
    "Categorize":   {1:0,   2:60,  4:5,   6:0,   7:0},
    "Resolution":   {1:0,   2:10,  4:455, 6:0,   7:0},
    "Escalation":   {1:0,   2:0,   4:140, 6:50,  7:30},
    "Update":       {1:0,   2:0,   4:70,  6:20,  7:20},
    "Resolved":     {1:0,   2:0,   4:35,  6:300, 7:150},
    "Close":        {1:0,   2:10,  4:5,   6:100, 7:200},
    "Report":       {1:0,   2:0,   4:0,   6:30,  7:100},
}

MSG_GROUPS = [1, 2, 4, 6, 7]   # message counts we care about
STAGE_ORDER = list(STAGE_MATRIX.keys())  # preserve order for deterministic distribution

# ---------- SETUP ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("assign_stages")

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
# 1) Read actual counts per message_count from DB
actual_group_counts = {}
for m in MSG_GROUPS:
    c = tickets.count_documents({"thread.message_count": m})
    actual_group_counts[m] = c
    logger.info(f"Actual docs with thread.message_count={m}: {c}")

# 2) For each message_count, compute desired per-stage counts (from STAGE_MATRIX),
#    scale them to match actual_group_counts[m], then select that many docs and assign stage.
global_plan = {}  # message_count -> list of tuples (stage_name, count_to_assign)
for m in MSG_GROUPS:
    desired_list = [STAGE_MATRIX[st].get(m, 0) for st in STAGE_ORDER]
    actual_total = actual_group_counts[m]
    if actual_total == 0:
        scaled = [0] * len(desired_list)
    else:
        scaled = scale_counts(desired_list, actual_total)

    # Log planned assignments
    stage_assignments = [(STAGE_ORDER[i], scaled[i]) for i in range(len(STAGE_ORDER)) if scaled[i] > 0]
    global_plan[m] = stage_assignments
    logger.info(f"Planned for message_count={m}: {stage_assignments} (sum={sum(scaled)} total_docs={actual_total})")

# 3) Dry-run: show plan summary
total_planned = sum(sum(count for (_, count) in global_plan[m]) for m in MSG_GROUPS)
logger.info(f"TOTAL documents planned to update across all groups: {total_planned}")

if DRY_RUN:
    logger.info("DRY_RUN = True -> No DB writes will be done. Review the plan above.")
else:
    # 4) For each message_count group, fetch IDs, shuffle, and assign segments to stages; write via bulk updates.
    for m in MSG_GROUPS:
        planned = global_plan[m]
        if not planned:
            continue

        # fetch ids for this message_count
        cursor = tickets.find({"thread.message_count": m}, {"_id": 1})
        ids = [doc["_id"] for doc in cursor]
        if not ids:
            logger.warning(f"No documents found for message_count={m}; skipping.")
            continue

        if len(ids) != sum(count for (_, count) in planned):
            logger.warning(f"Mismatch BEFORE assignment for message_count={m}: len(ids)={len(ids)} vs planned_sum={sum(count for (_,count) in planned)}. Proceeding with scaled plan.")

        # shuffle deterministically
        rnd.shuffle(ids)

        ops = []
        idx = 0
        for stage_name, cnt in planned:
            if cnt == 0:
                continue
            segment = ids[idx: idx + cnt]
            idx += cnt
            for _id in segment:
                ops.append(UpdateOne({"_id": _id}, {"$set": {"stages": stage_name}}))

            # flush in batches
            if len(ops) >= BATCH_SIZE:
                result = tickets.bulk_write(ops, ordered=False)
                logger.info(f"Bulk write executed (batch): matched={result.matched_count} modified={result.modified_count}")
                ops = []

        # final flush
        if ops:
            result = tickets.bulk_write(ops, ordered=False)
            logger.info(f"Bulk write executed (final): matched={result.matched_count} modified={result.modified_count}")

    # 5) Post-check: aggregate counts per stage and per message_count
    logger.info("Post-update verification: counts per stage")
    pipeline = [
        {"$group": {"_id": {"stage": "$stages", "msg_count": "$thread.message_count"}, "count": {"$sum": 1}}},
        {"$sort": {"_id.msg_count": 1}}
    ]
    for doc in tickets.aggregate(pipeline):
        logger.info(f"msg_count={doc['_id']['msg_count']}, stage={doc['_id']['stage']}, count={doc['count']}")

logger.info("Script finished.")
