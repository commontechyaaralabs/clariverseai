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

# First, remove urgency field from all documents
logger.info("Removing urgency field from all documents...")
result_remove = email_collection.update_many(
    {"urgency": {"$exists": True}},
    {"$unset": {"urgency": ""}}
)
logger.info(f"Removed urgency field from {result_remove.modified_count} documents")

# Get total count of documents in email collection
total_docs = email_collection.count_documents({})
logger.info(f"Total documents in email collection: {total_docs}")

# Urgency distribution based on the provided table
# Format: {message_count: {stage: urgent_count}}
urgency_distribution = {
    1: {  # 1-msg (10 urgent total)
        "Receive": 5,
        "Authenticate": 1,
        "Categorize": 2,
        "Resolution": 1,
        "Escalation": 1,
        "Update": 0,
        "Resolved": 0,
        "Close": 0,
        "Report": 0
    },
    3: {  # 3-msg (25 urgent total)
        "Receive": 2,
        "Authenticate": 2,
        "Categorize": 3,
        "Resolution": 3,
        "Escalation": 6,
        "Update": 5,
        "Resolved": 3,
        "Close": 0,
        "Report": 1
    },
    4: {  # 4-msg (35 urgent total)
        "Receive": 2,
        "Authenticate": 2,
        "Categorize": 4,
        "Resolution": 4,
        "Escalation": 12,
        "Update": 6,
        "Resolved": 4,
        "Close": 1,
        "Report": 0
    },
    6: {  # 6-msg (40 urgent total)
        "Receive": 1,
        "Authenticate": 2,
        "Categorize": 3,
        "Resolution": 6,
        "Escalation": 18,
        "Update": 7,
        "Resolved": 2,
        "Close": 1,
        "Report": 0
    },
    8: {  # 8-msg (50 urgent total)
        "Receive": 1,
        "Authenticate": 1,
        "Categorize": 3,
        "Resolution": 6,
        "Escalation": 28,
        "Update": 7,
        "Resolved": 3,
        "Close": 1,
        "Report": 0
    }
}

# Calculate total urgent documents
total_urgent = sum(sum(stages.values()) for stages in urgency_distribution.values())
logger.info(f"Total urgent documents to assign: {total_urgent}")

# Process each message_count and stage for urgency assignment
urgent_assigned = 0
for msg_count, stages_dict in urgency_distribution.items():
    logger.info(f"Processing urgency for message_count={msg_count}")
    
    for stage, urgent_count in stages_dict.items():
        if urgent_count == 0:
            continue  # Skip stages with 0 urgent documents
            
        # Find documents with this message_count and stage that don't have urgency field
        eligible_docs = list(email_collection.find(
            {
                "thread.message_count": msg_count,
                "stages": stage,
                "urgency": {"$exists": False}
            },
            {"_id": 1}
        ))
        
        if len(eligible_docs) < urgent_count:
            logger.warning(f"Not enough docs for urgent stage '{stage}' at msg_count {msg_count}. Found {len(eligible_docs)}, need {urgent_count}.")
            selected_docs = eligible_docs  # take all available
        else:
            selected_docs = random.sample(eligible_docs, urgent_count)
        
        # Update selected docs with urgency: true
        ids_to_update = [doc["_id"] for doc in selected_docs]
        if ids_to_update:
            result = email_collection.update_many(
                {"_id": {"$in": ids_to_update}},
                {"$set": {"urgency": True}}
            )
            urgent_assigned += result.modified_count
            logger.info(f"Updated {result.modified_count} docs to urgent for stage '{stage}' at msg_count {msg_count}")

# Set all remaining documents to urgency: false
logger.info("Setting remaining documents to urgency: false...")
result_false = email_collection.update_many(
    {"urgency": {"$exists": False}},
    {"$set": {"urgency": False}}
)
logger.info(f"Updated {result_false.modified_count} documents with urgency: false")

logger.info("Urgency field assignment complete.")

# ------------------ VERIFICATION & STATS ------------------
logger.info("Verifying urgency field assignment...")

# Count documents with urgency field
total_with_urgency = email_collection.count_documents({"urgency": {"$exists": True}})
total_docs_final = email_collection.count_documents({})
missing_urgency = total_docs_final - total_with_urgency

logger.info(f"Total docs in email collection: {total_docs_final}")
logger.info(f"Docs with 'urgency' field: {total_with_urgency}")
logger.info(f"Docs missing 'urgency' field: {missing_urgency}")

# Urgency distribution
urgency_true_count_final = email_collection.count_documents({"urgency": True})
urgency_false_count_final = email_collection.count_documents({"urgency": False})

logger.info(f"\nFinal urgency distribution:")
logger.info(f"  urgency: true  = {urgency_true_count_final} documents")
logger.info(f"  urgency: false = {urgency_false_count_final} documents")

# Calculate actual percentage
if total_with_urgency > 0:
    true_percentage = (urgency_true_count_final / total_with_urgency) * 100
    false_percentage = (urgency_false_count_final / total_with_urgency) * 100
    logger.info(f"  urgency: true  = {true_percentage:.2f}%")
    logger.info(f"  urgency: false = {false_percentage:.2f}%")

# Urgency distribution by stage and message count (matching the provided table)
logger.info(f"\nUrgency distribution by stage and message count:")
urgency_by_stage_msg_pipeline = [
    {"$match": {"urgency": True}},
    {"$group": {
        "_id": {
            "stage": "$stages",
            "message_count": "$thread.message_count"
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id.message_count": 1, "_id.stage": 1}}
]
urgency_by_stage_msg_stats = list(email_collection.aggregate(urgency_by_stage_msg_pipeline))

# Create a matrix to match the provided table format
stage_order = ["Receive", "Authenticate", "Categorize", "Resolution", "Escalation", "Update", "Resolved", "Close", "Report"]
msg_counts = [1, 3, 4, 6, 8]

# Initialize matrix
urgency_matrix = {}
for stage in stage_order:
    urgency_matrix[stage] = {}
    for msg_count in msg_counts:
        urgency_matrix[stage][msg_count] = 0

# Fill matrix with actual counts
for item in urgency_by_stage_msg_stats:
    stage = item["_id"]["stage"]
    msg_count = item["_id"]["message_count"]
    count = item["count"]
    if stage in urgency_matrix and msg_count in urgency_matrix[stage]:
        urgency_matrix[stage][msg_count] = count

# Print table header
logger.info("Stage\t1-msg\t3-msg\t4-msg\t6-msg\t8-msg\tUrgent total")
logger.info("-" * 60)

# Print each stage row
for stage in stage_order:
    row_total = sum(urgency_matrix[stage].values())
    logger.info(f"{stage}\t{urgency_matrix[stage][1]}\t{urgency_matrix[stage][3]}\t{urgency_matrix[stage][4]}\t{urgency_matrix[stage][6]}\t{urgency_matrix[stage][8]}\t{row_total}")

# Print totals row
total_row = [sum(urgency_matrix[stage][msg_count] for stage in stage_order) for msg_count in msg_counts]
grand_total = sum(total_row)
logger.info("-" * 60)
logger.info(f"TOTAL (urgent)\t{total_row[0]}\t{total_row[1]}\t{total_row[2]}\t{total_row[3]}\t{total_row[4]}\t{grand_total}")

# Verify against expected totals
expected_totals = {1: 10, 3: 25, 4: 35, 6: 40, 8: 50}
logger.info(f"\nVerification against expected totals:")
for msg_count in msg_counts:
    actual = total_row[msg_counts.index(msg_count)]
    expected = expected_totals[msg_count]
    status = "✓" if actual == expected else "✗"
    logger.info(f"  {msg_count}-msg: {actual}/{expected} {status}")

# Sample urgent documents for verification
logger.info(f"\nSample urgent documents:")
urgent_docs = list(email_collection.find(
    {"urgency": True},
    {"_id": 1, "thread.message_count": 1, "stages": 1, "urgency": 1}
).limit(10))

for doc in urgent_docs:
    logger.info(f"  ID: {doc['_id']}, Message Count: {doc['thread']['message_count']}, Stage: {doc['stages']}, Urgency: {doc['urgency']}")

logger.info("Verification complete.")
