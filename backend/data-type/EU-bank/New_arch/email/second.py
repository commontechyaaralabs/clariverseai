import random
import os
from pymongo import MongoClient
import logging
from dotenv import load_dotenv
from collections import Counter

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database configuration
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING', "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME', "sparzaai")

client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]
email_collection = db["email"]

# Stage distribution based on the provided table
distribution = {
    1: {  # message_count = 1 (250 total records)
        "Receive": 5,
        "Authenticate": 8,
        "Categorize": 15,
        "Resolution": 30,
        "Escalation": 5,
        "Update": 25,
        "Resolved": 140,
        "Close": 15,
        "Report": 7
    },
    3: {  # message_count = 3 (418 total records)
        "Receive": 4,
        "Authenticate": 8,
        "Categorize": 32,
        "Resolution": 105,
        "Escalation": 21,
        "Update": 84,
        "Resolved": 138,
        "Close": 16,
        "Report": 10
    },
    4: {  # message_count = 4 (468 total records)
        "Receive": 3,
        "Authenticate": 7,
        "Categorize": 34,
        "Resolution": 95,
        "Escalation": 35,
        "Update": 118,
        "Resolved": 145,
        "Close": 17,
        "Report": 14
    },
    6: {  # message_count = 6 (431 total records)
        "Receive": 4,
        "Authenticate": 6,
        "Categorize": 26,
        "Resolution": 55,
        "Escalation": 115,
        "Update": 105,
        "Resolved": 110,
        "Close": 32,
        "Report": 13
    },
    8: {  # message_count = 8 (433 total records)
        "Receive": 3,
        "Authenticate": 6,
        "Categorize": 25,
        "Resolution": 46,
        "Escalation": 122,
        "Update": 90,
        "Resolved": 82,
        "Close": 35,
        "Report": 0  # No Report stage for 8-message emails
    }
}

# Process each message_count
for msg_count, stages_dict in distribution.items():
    logger.info(f"Processing message_count={msg_count}")
    
    # Calculate total needed for this message_count
    total_needed = sum(stages_dict.values())
    logger.info(f"Total records needed for message_count {msg_count}: {total_needed}")

    for stage, needed_count in stages_dict.items():
        # Skip Report stage for 8-message emails (0 count)
        if needed_count == 0:
            continue
            
        # Find eligible docs (those not already assigned a stage)
        eligible_docs = list(email_collection.find(
            {"thread.message_count": msg_count, "stages": {"$exists": False}},
            {"_id": 1}
        ))

        if len(eligible_docs) < needed_count:
            logger.warning(f"Not enough docs for stage '{stage}' at msg_count {msg_count}. Found {len(eligible_docs)}, need {needed_count}.")
            selected_docs = eligible_docs  # take all available
        else:
            selected_docs = random.sample(eligible_docs, needed_count)

        # Update selected docs with stage
        ids_to_update = [doc["_id"] for doc in selected_docs]
        if ids_to_update:
            result = email_collection.update_many(
                {"_id": {"$in": ids_to_update}},
                {"$set": {"stages": stage}}
            )
            logger.info(f"Updated {result.modified_count} docs to stage '{stage}' for msg_count {msg_count}")

logger.info("Stage assignment complete.")

# ------------------ FINAL CHECK & STATS ------------------
logger.info("Verifying stage assignment...")

# Count total docs with stages field
total_with_stages = email_collection.count_documents({"stages": {"$exists": True}})
total_docs = email_collection.count_documents({})
missing_stages = total_docs - total_with_stages

logger.info(f"Total docs in email collection: {total_docs}")
logger.info(f"Docs with 'stages' field: {total_with_stages}")
logger.info(f"Docs missing 'stages' field: {missing_stages}")

# Stage-wise breakdown
pipeline = [
    {"$match": {"stages": {"$exists": True}}},
    {"$group": {"_id": "$stages", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
stage_counts = list(email_collection.aggregate(pipeline))

logger.info("Stage-wise stats:")
for item in stage_counts:
    logger.info(f"{item['_id']}: {item['count']}")

# Message count distribution with stages
logger.info("\nMessage count distribution with stages:")
message_count_pipeline = [
    {"$match": {"stages": {"$exists": True}}},
    {"$group": {"_id": "$thread.message_count", "count": {"$sum": 1}}},
    {"$sort": {"_id": 1}}
]
message_count_stats = list(email_collection.aggregate(message_count_pipeline))

for item in message_count_stats:
    logger.info(f"Message count {item['_id']}: {item['count']} docs with stages")

# Cross-tabulation: Stage vs Message Count
logger.info("\nCross-tabulation (Stage vs Message Count):")
cross_tab_pipeline = [
    {"$match": {"stages": {"$exists": True}}},
    {"$group": {
        "_id": {
            "stage": "$stages",
            "message_count": "$thread.message_count"
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id.stage": 1, "_id.message_count": 1}}
]
cross_tab_stats = list(email_collection.aggregate(cross_tab_pipeline))

# Group by stage for better readability
stage_groups = {}
for item in cross_tab_stats:
    stage = item["_id"]["stage"]
    msg_count = item["_id"]["message_count"]
    count = item["count"]
    
    if stage not in stage_groups:
        stage_groups[stage] = {}
    stage_groups[stage][msg_count] = count

for stage in sorted(stage_groups.keys()):
    logger.info(f"\n{stage}:")
    for msg_count in sorted(stage_groups[stage].keys()):
        logger.info(f"  {msg_count}-Msg: {stage_groups[stage][msg_count]}")

logger.info("Verification complete.")
