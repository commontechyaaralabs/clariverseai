#!/usr/bin/env python3
"""
Deterministic assignment of `urgency` field to tickets based on stage and thread.message_count.
- Uses a provided stage x message_count matrix as "desired" urgent counts.
- Distributes urgency more evenly across all available combinations like the original target.
- Ensures most combinations get at least some urgent records (if they have documents).

BEFORE RUNNING:
- Set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in your environment or .env.
- This script overwrites the 'urgency' field for matching docs when DRY_RUN = False.
"""

import os
import random
import logging
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne

# ---------- CONFIG ----------
DRY_RUN = False   # Set to False to actually update the DB
RANDOM_SEED = 42
BATCH_SIZE = 1000  # bulk_write batch size

# The urgency matrix based on your table (stage x message_count -> desired urgent count)
URGENCY_MATRIX = {
    "Receive":      {1: 3, 2: 8,  4: 15, 6: 10, 7: 4},
    "Authenticate": {1: 2, 2: 6,  4: 12, 6: 7,  7: 3},
    "Categorize":   {1: 2, 2: 6,  4: 12, 6: 7,  7: 3},
    "Resolution":   {1: 1, 2: 7,  4: 18, 6: 12, 7: 5},
    "Escalation":   {1: 1, 2: 6,  4: 14, 6: 10, 7: 4},
    "Update":       {1: 1, 2: 5,  4: 10, 6: 6,  7: 2},
    "Resolved":     {1: 0, 2: 3,  4: 3,  6: 2,  7: 1},
    "Close":        {1: 0, 2: 2,  4: 2,  6: 0,  7: 0},
    "Report":       {1: 0, 2: 0,  4: 0,  6: 0,  7: 0},
}

MSG_GROUPS = [1, 2, 4, 6, 7]   # message counts we care about
STAGE_ORDER = list(URGENCY_MATRIX.keys())  # preserve order

# Calculate target total urgent count from matrix
TARGET_URGENT_TOTAL = sum(sum(URGENCY_MATRIX[stage].values()) for stage in STAGE_ORDER)

# ---------- SETUP ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("assign_urgency")

load_dotenv()
mongo_connection_string = os.getenv("MONGO_CONNECTION_STRING")
mongo_database_name = os.getenv("MONGO_DATABASE_NAME")

if not mongo_connection_string or not mongo_database_name:
    raise SystemExit("Please set MONGO_CONNECTION_STRING and MONGO_DATABASE_NAME in environment / .env")

client = MongoClient(mongo_connection_string)
db = client[mongo_database_name]
tickets = db["tickets"]

rnd = random.Random(RANDOM_SEED)

# ---------- PLAN ----------
logger.info(f"Target urgent total from matrix: {TARGET_URGENT_TOTAL}")

# 1) Get actual counts per stage and message_count from DB
actual_counts = {}  # (stage, message_count) -> actual_doc_count
available_combinations = []  # List of (stage, message_count, actual_count, desired_urgent)

total_available_docs = 0
for stage in STAGE_ORDER:
    for m in MSG_GROUPS:
        count = tickets.count_documents({"stages": stage, "thread.message_count": m})
        actual_counts[(stage, m)] = count
        desired_urgent = URGENCY_MATRIX[stage].get(m, 0)
        
        if count > 0:
            logger.info(f"Actual docs with stage={stage}, message_count={m}: {count}, desired_urgent: {desired_urgent}")
            available_combinations.append((stage, m, count, desired_urgent))
            total_available_docs += count

logger.info(f"Total available documents: {total_available_docs}")
logger.info(f"Available combinations: {len(available_combinations)}")

# 2) Balanced allocation algorithm - tries to maintain the original distribution pattern
def allocate_urgent_balanced(combinations, target_total):
    """
    Allocate urgent slots trying to maintain the original distribution proportions,
    but ensure most combinations get at least some urgent records.
    """
    allocations = {}  # (stage, message_count) -> urgent_count
    
    # Calculate total desired from available combinations
    total_desired_available = sum(desired for (_, _, _, desired) in combinations)
    total_docs_available = sum(actual for (_, _, actual, _) in combinations)
    
    if total_desired_available == 0:
        # If no desired urgency, distribute evenly
        per_combination = target_total // len(combinations)
        remainder = target_total % len(combinations)
        
        for i, (stage, m, actual_count, _) in enumerate(combinations):
            base_allocation = min(per_combination, actual_count)
            extra = 1 if i < remainder and (base_allocation + 1) <= actual_count else 0
            allocations[(stage, m)] = base_allocation + extra
        
        return allocations
    
    # Calculate scaling factor
    scale_factor = target_total / total_desired_available
    logger.info(f"Initial scale factor: {scale_factor:.3f}")
    
    # Phase 1: Allocate based on scaled desired amounts
    total_allocated = 0
    unallocated_urgent = 0
    
    for stage, m, actual_count, desired_urgent in combinations:
        if desired_urgent == 0:
            # For combinations that originally had 0, give them a small amount
            # if we have excess capacity
            allocations[(stage, m)] = 0
        else:
            scaled_desired = max(1, round(desired_urgent * scale_factor))  # At least 1 if originally > 0
            can_allocate = min(scaled_desired, actual_count)
            allocations[(stage, m)] = can_allocate
            total_allocated += can_allocate
            unallocated_urgent += max(0, scaled_desired - can_allocate)
    
    logger.info(f"After phase 1: allocated {total_allocated}, unallocated {unallocated_urgent}")
    
    # Phase 2: Handle remaining slots
    remaining_slots = target_total - total_allocated
    
    if remaining_slots > 0:
        # First, give at least 1 urgent to combinations that originally had > 0 but got 0
        for stage, m, actual_count, desired_urgent in combinations:
            if remaining_slots <= 0:
                break
            if desired_urgent > 0 and allocations[(stage, m)] == 0 and actual_count > 0:
                allocations[(stage, m)] = 1
                remaining_slots -= 1
                total_allocated += 1
        
        # Then, distribute remaining based on capacity and original proportions
        while remaining_slots > 0:
            distributed_this_round = 0
            
            # Sort by original desired proportion (highest first)
            sorted_combinations = sorted(combinations, 
                                       key=lambda x: (x[3] / x[2] if x[2] > 0 else 0, x[3]), 
                                       reverse=True)
            
            for stage, m, actual_count, desired_urgent in sorted_combinations:
                if remaining_slots <= 0:
                    break
                
                current_allocation = allocations[(stage, m)]
                if current_allocation < actual_count:
                    allocations[(stage, m)] += 1
                    remaining_slots -= 1
                    distributed_this_round += 1
            
            # If we couldn't distribute any slots this round, break to avoid infinite loop
            if distributed_this_round == 0:
                break
        
        logger.info(f"After phase 2: distributed {target_total - total_allocated} additional slots")
    
    # Phase 3: Give minimal urgent to zero-desired combinations if we still have slots
    remaining_slots = target_total - sum(allocations.values())
    if remaining_slots > 0:
        zero_desired_combinations = [(s, m, a, d) for s, m, a, d in combinations 
                                   if d == 0 and allocations[(s, m)] == 0 and a > 0]
        
        slots_per_zero = max(1, remaining_slots // max(1, len(zero_desired_combinations)))
        
        for stage, m, actual_count, _ in zero_desired_combinations[:remaining_slots]:
            if remaining_slots <= 0:
                break
            give_slots = min(slots_per_zero, actual_count, remaining_slots)
            allocations[(stage, m)] = give_slots
            remaining_slots -= give_slots
    
    return allocations

# 3) Allocate urgent slots with balanced approach
urgency_allocations = allocate_urgent_balanced(available_combinations, TARGET_URGENT_TOTAL)

# 4) Create final plan
urgency_plan = {}  # (stage, message_count) -> (urgent_count, non_urgent_count)
total_urgent_planned = 0
total_non_urgent_planned = 0

for stage, m, actual_count, desired_urgent in available_combinations:
    urgent_count = urgency_allocations.get((stage, m), 0)
    non_urgent_count = actual_count - urgent_count
    
    urgency_plan[(stage, m)] = (urgent_count, non_urgent_count)
    
    logger.info(f"Final plan for stage={stage}, message_count={m}: urgent={urgent_count} (desired was {desired_urgent}), non_urgent={non_urgent_count}, total={actual_count}")
    
    total_urgent_planned += urgent_count
    total_non_urgent_planned += non_urgent_count

logger.info(f"FINAL PLAN: urgent={total_urgent_planned} (target was {TARGET_URGENT_TOTAL}), non_urgent={total_non_urgent_planned}")

# Count how many combinations have urgent > 0
combinations_with_urgent = sum(1 for (s, m) in urgency_allocations if urgency_allocations[(s, m)] > 0)
logger.info(f"Combinations with urgent > 0: {combinations_with_urgent} out of {len(available_combinations)}")

if total_urgent_planned != TARGET_URGENT_TOTAL:
    diff = TARGET_URGENT_TOTAL - total_urgent_planned
    if diff > 0:
        logger.warning(f"Could only allocate {total_urgent_planned} urgent slots out of {TARGET_URGENT_TOTAL} target (shortfall: {diff})")
    else:
        logger.info(f"Allocated {total_urgent_planned} urgent slots (exceeded target by {abs(diff)})")

if DRY_RUN:
    logger.info("DRY_RUN = True -> No DB writes will be done. Review the plan above.")
else:
    # 5) Execute the urgency assignments
    all_ops = []
    
    for stage in STAGE_ORDER:
        for m in MSG_GROUPS:
            if (stage, m) not in urgency_plan:
                continue
                
            urgent_count, non_urgent_count = urgency_plan[(stage, m)]
            if urgent_count == 0 and non_urgent_count == 0:
                continue
            
            # Fetch all document IDs for this stage/message_count combination
            cursor = tickets.find({"stages": stage, "thread.message_count": m}, {"_id": 1})
            ids = [doc["_id"] for doc in cursor]
            
            if not ids:
                logger.warning(f"No documents found for stage={stage}, message_count={m}; skipping.")
                continue
            
            # Shuffle deterministically
            rnd.shuffle(ids)
            
            # Assign urgency: true to first urgent_count documents
            for i, doc_id in enumerate(ids):
                if i < urgent_count:
                    urgency_value = True
                else:
                    urgency_value = False
                
                all_ops.append(UpdateOne({"_id": doc_id}, {"$set": {"urgency": urgency_value}}))
                
                # Execute in batches
                if len(all_ops) >= BATCH_SIZE:
                    result = tickets.bulk_write(all_ops, ordered=False)
                    logger.info(f"Bulk write executed (batch): matched={result.matched_count} modified={result.modified_count}")
                    all_ops = []
    
    # Final flush
    if all_ops:
        result = tickets.bulk_write(all_ops, ordered=False)
        logger.info(f"Bulk write executed (final): matched={result.matched_count} modified={result.modified_count}")
    
    # 6) Post-check: aggregate counts per urgency, stage, and message_count
    logger.info("\nPost-update verification: counts per urgency/stage/message_count")
    pipeline = [
        {"$group": {
            "_id": {
                "urgency": "$urgency", 
                "stage": "$stages", 
                "msg_count": "$thread.message_count"
            }, 
            "count": {"$sum": 1}
        }},
        {"$sort": {"_id.stage": 1, "_id.msg_count": 1, "_id.urgency": -1}}
    ]
    
    urgent_verification_total = 0
    combinations_with_urgent_actual = 0
    for doc in tickets.aggregate(pipeline):
        urgency = doc['_id']['urgency']
        stage = doc['_id']['stage']
        msg_count = doc['_id']['msg_count']
        count = doc['count']
        logger.info(f"stage={stage}, msg_count={msg_count}, urgency={urgency}, count={count}")
        
        if urgency == True:
            urgent_verification_total += count
            if count > 0:
                combinations_with_urgent_actual += 1
    
    # Summary by urgency
    logger.info(f"\nSummary by urgency:")
    urgency_summary = tickets.aggregate([
        {"$group": {"_id": "$urgency", "count": {"$sum": 1}}},
        {"$sort": {"_id": -1}}
    ])
    for doc in urgency_summary:
        logger.info(f"urgency={doc['_id']}, total_count={doc['count']}")
    
    logger.info(f"\nFINAL VERIFICATION:")
    logger.info(f"Total urgent documents = {urgent_verification_total} (target was {TARGET_URGENT_TOTAL})")
    logger.info(f"Combinations with urgent > 0 = {combinations_with_urgent_actual}")
    
    if urgent_verification_total == TARGET_URGENT_TOTAL:
        logger.info("✅ SUCCESS: Achieved exact target urgent records with balanced distribution!")
    else:
        diff = urgent_verification_total - TARGET_URGENT_TOTAL
        logger.info(f"❌ Difference from target: {diff} (achieved {urgent_verification_total} out of {TARGET_URGENT_TOTAL})")

logger.info("Script finished.")