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

# First, remove follow_up_required field from all documents
logger.info("Removing follow_up_required field from all documents...")
result_remove = email_collection.update_many(
    {"follow_up_required": {"$exists": True}},
    {"$unset": {"follow_up_required": ""}}
)
logger.info(f"Removed follow_up_required field from {result_remove.modified_count} documents")

# Get total count of documents in email collection
total_docs = email_collection.count_documents({})
logger.info(f"Total documents in email collection: {total_docs}")

# Follow-up required distribution based on the provided table
# Format: {message_count: {stage: follow_up_yes_count}}
follow_up_distribution = {
    1: {  # 1-msg (250 total)
        "Receive": 18,
        "Authenticate": 9,
        "Categorize": 28,
        "Resolution": 12,
        "Escalation": 6,
        "Update": 0,
        "Resolved": 0,
        "Close": 0,
        "Report": 0
    },
    3: {  # 3-msg (418 total)
        "Receive": 6,
        "Authenticate": 9,
        "Categorize": 32,
        "Resolution": 30,
        "Escalation": 28,
        "Update": 12,
        "Resolved": 3,
        "Close": 1,
        "Report": 1
    },
    4: {  # 4-msg (468 total)
        "Receive": 6,
        "Authenticate": 10,
        "Categorize": 38,
        "Resolution": 45,
        "Escalation": 50,
        "Update": 18,
        "Resolved": 5,
        "Close": 2,
        "Report": 1
    },
    6: {  # 6-msg (431 total)
        "Receive": 4,
        "Authenticate": 6,
        "Categorize": 22,
        "Resolution": 40,
        "Escalation": 85,
        "Update": 15,
        "Resolved": 3,
        "Close": 3,
        "Report": 1
    },
    8: {  # 8-msg (433 total)
        "Receive": 2,
        "Authenticate": 4,
        "Categorize": 18,
        "Resolution": 30,
        "Escalation": 110,
        "Update": 15,
        "Resolved": 3,
        "Close": 3,
        "Report": 1
    }
}

# Calculate total follow-up yes documents
total_follow_up_yes = sum(sum(stages.values()) for stages in follow_up_distribution.values())
logger.info(f"Total follow_up_required: 'yes' documents to assign: {total_follow_up_yes}")

# Process each message_count and stage for follow_up_required assignment
follow_up_yes_assigned = 0
for msg_count, stages_dict in follow_up_distribution.items():
    logger.info(f"Processing follow_up_required for message_count={msg_count}")
    
    for stage, follow_up_yes_count in stages_dict.items():
        if follow_up_yes_count == 0:
            continue  # Skip stages with 0 follow-up yes documents
        
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # Find documents with this message_count and stage that don't have follow_up_required field
                eligible_docs = list(email_collection.find(
                    {
                        "thread.message_count": msg_count,
                        "stages": stage,
                        "follow_up_required": {"$exists": False}
                    },
                    {"_id": 1}
                ))
                
                if len(eligible_docs) < follow_up_yes_count:
                    logger.warning(f"Not enough docs for follow_up_yes stage '{stage}' at msg_count {msg_count}. Found {len(eligible_docs)}, need {follow_up_yes_count}.")
                    selected_docs = eligible_docs  # take all available
                else:
                    selected_docs = random.sample(eligible_docs, follow_up_yes_count)
                
                # Update selected docs with follow_up_required: "yes"
                ids_to_update = [doc["_id"] for doc in selected_docs]
                if ids_to_update:
                    result = email_collection.update_many(
                        {"_id": {"$in": ids_to_update}},
                        {"$set": {"follow_up_required": "yes"}}
                    )
                    follow_up_yes_assigned += result.modified_count
                    logger.info(f"Updated {result.modified_count} docs to follow_up_required: 'yes' for stage '{stage}' at msg_count {msg_count}")
                
                break  # Success, exit retry loop
                
            except Exception as e:
                retry_count += 1
                logger.warning(f"Attempt {retry_count} failed for stage '{stage}' at msg_count {msg_count}: {str(e)}")
                
                if retry_count < max_retries:
                    logger.info(f"Retrying in 2 seconds...")
                    import time
                    time.sleep(2)
                    
                    # Reconnect to database
                    try:
                        client = MongoClient(MONGO_CONNECTION_STRING)
                        db = client[MONGO_DATABASE_NAME]
                        email_collection = db["email"]
                        logger.info("Reconnected to database")
                    except Exception as reconnect_error:
                        logger.error(f"Failed to reconnect: {str(reconnect_error)}")
                else:
                    logger.error(f"Max retries reached for stage '{stage}' at msg_count {msg_count}. Skipping...")
                    break

# Set all remaining documents to follow_up_required: "no"
logger.info("Setting remaining documents to follow_up_required: 'no'...")
max_retries = 3
retry_count = 0

while retry_count < max_retries:
    try:
        result_no = email_collection.update_many(
            {"follow_up_required": {"$exists": False}},
            {"$set": {"follow_up_required": "no"}}
        )
        logger.info(f"Updated {result_no.modified_count} documents with follow_up_required: 'no'")
        break  # Success, exit retry loop
        
    except Exception as e:
        retry_count += 1
        logger.warning(f"Attempt {retry_count} failed for setting remaining docs to 'no': {str(e)}")
        
        if retry_count < max_retries:
            logger.info(f"Retrying in 2 seconds...")
            import time
            time.sleep(2)
            
            # Reconnect to database
            try:
                client = MongoClient(MONGO_CONNECTION_STRING)
                db = client[MONGO_DATABASE_NAME]
                email_collection = db["email"]
                logger.info("Reconnected to database")
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect: {str(reconnect_error)}")
        else:
            logger.error(f"Max retries reached for setting remaining docs to 'no'. Manual intervention may be required.")
            break

logger.info("Follow-up required field assignment complete.")

# ------------------ VERIFICATION & STATS ------------------
logger.info("Verifying follow_up_required field assignment...")

# Count documents with follow_up_required field
total_with_follow_up = email_collection.count_documents({"follow_up_required": {"$exists": True}})
total_docs_final = email_collection.count_documents({})
missing_follow_up = total_docs_final - total_with_follow_up

logger.info(f"Total docs in email collection: {total_docs_final}")
logger.info(f"Docs with 'follow_up_required' field: {total_with_follow_up}")
logger.info(f"Docs missing 'follow_up_required' field: {missing_follow_up}")

# Follow-up required distribution
follow_up_yes_count_final = email_collection.count_documents({"follow_up_required": "yes"})
follow_up_no_count_final = email_collection.count_documents({"follow_up_required": "no"})

logger.info(f"\nFinal follow_up_required distribution:")
logger.info(f"  follow_up_required: 'yes' = {follow_up_yes_count_final} documents")
logger.info(f"  follow_up_required: 'no'  = {follow_up_no_count_final} documents")

# Calculate actual percentage
if total_with_follow_up > 0:
    yes_percentage = (follow_up_yes_count_final / total_with_follow_up) * 100
    no_percentage = (follow_up_no_count_final / total_with_follow_up) * 100
    logger.info(f"  follow_up_required: 'yes' = {yes_percentage:.2f}%")
    logger.info(f"  follow_up_required: 'no'  = {no_percentage:.2f}%")

# Follow-up required distribution by stage and message count (matching the provided table)
logger.info(f"\nFollow-up required distribution by stage and message count:")
follow_up_by_stage_msg_pipeline = [
    {"$match": {"follow_up_required": "yes"}},
    {"$group": {
        "_id": {
            "stage": "$stages",
            "message_count": "$thread.message_count"
        },
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id.message_count": 1, "_id.stage": 1}}
]
follow_up_by_stage_msg_stats = list(email_collection.aggregate(follow_up_by_stage_msg_pipeline))

# Create a matrix to match the provided table format
stage_order = ["Receive", "Authenticate", "Categorize", "Resolution", "Escalation", "Update", "Resolved", "Close", "Report"]
msg_counts = [1, 3, 4, 6, 8]

# Initialize matrix
follow_up_matrix = {}
for stage in stage_order:
    follow_up_matrix[stage] = {}
    for msg_count in msg_counts:
        follow_up_matrix[stage][msg_count] = 0

# Fill matrix with actual counts
for item in follow_up_by_stage_msg_stats:
    stage = item["_id"].get("stage", "No Stage")
    msg_count = item["_id"]["message_count"]
    count = item["count"]
    if stage in follow_up_matrix and msg_count in follow_up_matrix[stage]:
        follow_up_matrix[stage][msg_count] = count

# Print table header
logger.info("Stage\t1-msg\t3-msg\t4-msg\t6-msg\t8-msg\tFollow-up True Total")
logger.info("-" * 70)

# Print each stage row
for stage in stage_order:
    row_total = sum(follow_up_matrix[stage].values())
    logger.info(f"{stage}\t{follow_up_matrix[stage][1]}\t{follow_up_matrix[stage][3]}\t{follow_up_matrix[stage][4]}\t{follow_up_matrix[stage][6]}\t{follow_up_matrix[stage][8]}\t{row_total}")

# Print totals row
total_row = [sum(follow_up_matrix[stage][msg_count] for stage in stage_order) for msg_count in msg_counts]
grand_total = sum(total_row)
logger.info("-" * 70)
logger.info(f"TOTAL True\t{total_row[0]}\t{total_row[1]}\t{total_row[2]}\t{total_row[3]}\t{total_row[4]}\t{grand_total}")

# Verify against expected totals
expected_totals = {1: 73, 3: 122, 4: 175, 6: 179, 8: 186}
logger.info(f"\nVerification against expected totals:")
for msg_count in msg_counts:
    actual = total_row[msg_counts.index(msg_count)]
    expected = expected_totals[msg_count]
    status = "✓" if actual == expected else "✗"
    logger.info(f"  {msg_count}-msg: {actual}/{expected} {status}")

# Sample follow-up yes documents for verification
logger.info(f"\nSample follow-up yes documents:")
follow_up_yes_docs = list(email_collection.find(
    {"follow_up_required": "yes"},
    {"_id": 1, "thread.message_count": 1, "stages": 1, "follow_up_required": 1}
).limit(10))

for doc in follow_up_yes_docs:
    logger.info(f"  ID: {doc['_id']}, Message Count: {doc['thread']['message_count']}, Stage: {doc.get('stages', 'N/A')}, Follow-up Required: {doc['follow_up_required']}")

logger.info("Verification complete.")
