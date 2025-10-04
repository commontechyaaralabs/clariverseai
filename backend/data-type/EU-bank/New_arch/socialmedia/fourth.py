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
social_media_collection = db["social_media"]

# Stage distribution based on channel from the provided table
channel_stage_distribution = {
    "Trustpilot": {
        "Receive": 200,
        "Authenticate": 90,
        "Categorize": 150,
        "Resolution": 250,
        "Escalation": 100,
        "Update": 120,
        "Resolved": 160,
        "Close": 50,
        "Report": 27
    },
    "Twitter": {
        "Receive": 55,
        "Authenticate": 20,
        "Categorize": 35,
        "Resolution": 40,
        "Escalation": 50,
        "Update": 25,
        "Resolved": 20,
        "Close": 7,
        "Report": 5
    },
    "Reddit": {
        "Receive": 30,
        "Authenticate": 10,
        "Categorize": 25,
        "Resolution": 15,
        "Escalation": 10,
        "Update": 8,
        "Resolved": 12,
        "Close": 7,
        "Report": 5
    },
    "App Store/Google Play": {
        "Receive": 120,
        "Authenticate": 60,
        "Categorize": 95,
        "Resolution": 140,
        "Escalation": 60,
        "Update": 80,
        "Resolved": 90,
        "Close": 30,
        "Report": 26
    }
}

# Process each channel
for channel, stages_dict in channel_stage_distribution.items():
    logger.info(f"Processing channel: {channel}")
    
    # Calculate total needed for this channel
    total_needed = sum(stages_dict.values())
    logger.info(f"Total records needed for channel '{channel}': {total_needed}")

    for stage, needed_count in stages_dict.items():
        # Skip if no records needed for this stage
        if needed_count == 0:
            continue
            
        # Find eligible docs (those not already assigned a stage for this channel)
        eligible_docs = list(social_media_collection.find(
            {"channel": channel, "stages": {"$exists": False}},
            {"_id": 1}
        ))

        if len(eligible_docs) < needed_count:
            logger.warning(f"Not enough docs for stage '{stage}' in channel '{channel}'. Found {len(eligible_docs)}, need {needed_count}.")
            selected_docs = eligible_docs  # take all available
        else:
            selected_docs = random.sample(eligible_docs, needed_count)

        # Update selected docs with stage
        ids_to_update = [doc["_id"] for doc in selected_docs]
        if ids_to_update:
            result = social_media_collection.update_many(
                {"_id": {"$in": ids_to_update}},
                {"$set": {"stages": stage}}
            )
            logger.info(f"Updated {result.modified_count} docs to stage '{stage}' for channel '{channel}'")

logger.info("Stage assignment complete.")

# ------------------ FINAL CHECK & STATS ------------------
logger.info("Verifying stage assignment...")

# Count total docs with stages field
total_with_stages = social_media_collection.count_documents({"stages": {"$exists": True}})
total_docs = social_media_collection.count_documents({})
missing_stages = total_docs - total_with_stages

logger.info(f"Total docs in social_media collection: {total_docs}")
logger.info(f"Docs with 'stages' field: {total_with_stages}")
logger.info(f"Docs missing 'stages' field: {missing_stages}")

# Stage-wise breakdown
pipeline = [
    {"$match": {"stages": {"$exists": True}}},
    {"$group": {"_id": "$stages", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
stage_counts = list(social_media_collection.aggregate(pipeline))

logger.info("Stage-wise stats:")
for item in stage_counts:
    logger.info(f"{item['_id']}: {item['count']}")

# Channel distribution with stages
logger.info("\nChannel distribution with stages:")
channel_pipeline = [
    {"$match": {"stages": {"$exists": True}}},
    {"$group": {"_id": "$channel", "count": {"$sum": 1}}},
    {"$sort": {"count": -1}}
]
channel_stats = list(social_media_collection.aggregate(channel_pipeline))

for item in channel_stats:
    logger.info(f"Channel {item['_id']}: {item['count']} docs with stages")

# Cross-tabulation: Stage vs Channel
logger.info("\nCross-tabulation (Stage vs Channel):")
cross_tab_pipeline = [
    {"$match": {"stages": {"$exists": True}}},
    {"$group": {
        "_id": {
            "stage": "$stages",
            "channel": "$channel"
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id.stage": 1, "_id.channel": 1}}
]
cross_tab_stats = list(social_media_collection.aggregate(cross_tab_pipeline))

# Group by stage for better readability
stage_groups = {}
for item in cross_tab_stats:
    stage = item["_id"]["stage"]
    channel = item["_id"]["channel"]
    count = item["count"]
    
    if stage not in stage_groups:
        stage_groups[stage] = {}
    stage_groups[stage][channel] = count

logger.info("\nDetailed breakdown by stage and channel:")
for stage in sorted(stage_groups.keys()):
    logger.info(f"\n{stage}:")
    for channel in sorted(stage_groups[stage].keys()):
        logger.info(f"  {channel}: {stage_groups[stage][channel]}")

# Compare with expected distribution
logger.info("\nComparison with expected distribution:")
logger.info("Expected vs Actual:")
logger.info("Channel\t\tStage\t\tExpected\tActual\t\tDifference")
logger.info("-" * 80)

for channel, stages_dict in channel_stage_distribution.items():
    for stage, expected_count in stages_dict.items():
        actual_count = stage_groups.get(stage, {}).get(channel, 0)
        difference = actual_count - expected_count
        logger.info(f"{channel:<15}\t{stage:<12}\t{expected_count:<8}\t{actual_count:<8}\t{difference:+}")

logger.info("Verification complete.")