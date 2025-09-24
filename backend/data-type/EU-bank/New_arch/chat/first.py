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
chat_collection = db["chat"]

# Stage distribution (based on your table)
distribution = {
    15: {  # message_count = 15
        "Receive": 16,
        "Authenticate": 12,
        "Categorize": 14,
        "Resolution": 44,
        "Escalation": 20,
        "Update": 18,
        "Resolved": 50,
        "Close": 20,
        "Report": 6
    },
    17: {  # message_count = 17
        "Receive": 17,
        "Authenticate": 12,
        "Categorize": 14,
        "Resolution": 45,
        "Escalation": 21,
        "Update": 19,
        "Resolved": 52,
        "Close": 21,
        "Report": 6
    },
    18: {  # message_count = 18
        "Receive": 15,
        "Authenticate": 12,
        "Categorize": 14,
        "Resolution": 42,
        "Escalation": 19,
        "Update": 17,
        "Resolved": 48,
        "Close": 19,
        "Report": 6
    }
}

# Process each message_count
for msg_count, stages_dict in distribution.items():
    logger.info(f"Processing message_count={msg_count}")

    for stage, needed_count in stages_dict.items():
        # Find eligible docs (those not already assigned a stage)
        eligible_docs = list(chat_collection.find(
            {"chat.message_count": msg_count, "stages": {"$exists": False}},
            {"_id": 1}
        ))

        if len(eligible_docs) < needed_count:
            logger.warning(f"Not enough docs for stage '{stage}' at msg_count {msg_count}. Found {len(eligible_docs)}, need {needed_count}.")
            selected_docs = eligible_docs  # take all
        else:
            selected_docs = random.sample(eligible_docs, needed_count)

        # Update selected docs with stage
        ids_to_update = [doc["_id"] for doc in selected_docs]
        if ids_to_update:
            result = chat_collection.update_many(
                {"_id": {"$in": ids_to_update}},
                {"$set": {"stages": stage}}
            )
            logger.info(f"Updated {result.modified_count} docs to stage '{stage}' for msg_count {msg_count}")

logger.info("Stage assignment complete.")

# ------------------ FINAL CHECK & STATS ------------------
logger.info("Verifying stage assignment...")

# Count total docs with stages field
total_with_stages = chat_collection.count_documents({"stages": {"$exists": True}})
total_docs = chat_collection.count_documents({})
missing_stages = total_docs - total_with_stages

logger.info(f"Total docs in collection: {total_docs}")
logger.info(f"Docs with 'stages' field: {total_with_stages}")
logger.info(f"Docs missing 'stages' field: {missing_stages}")

# Stage-wise breakdown
pipeline = [
    {"$match": {"stages": {"$exists": True}}},
    {"$group": {"_id": "$stages", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
stage_counts = list(chat_collection.aggregate(pipeline))

logger.info("Stage-wise stats:")
for item in stage_counts:
    logger.info(f"{item['_id']}: {item['count']}")

logger.info("Verification complete.")
