# Import required libraries
from pymongo import MongoClient
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Connect to MongoDB using environment variables
MONGO_CONNECTION_STRING = os.getenv('MONGO_CONNECTION_STRING')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME')

# Connect to MongoDB
client = MongoClient(MONGO_CONNECTION_STRING)
db = client[MONGO_DATABASE_NAME]

# Find unique dominant_topic where follow_up_required:"yes" and action_pending_status:"yes"
pipeline = [
    {
        "$match": {
            "follow_up_required": "yes",
            "action_pending_status": "yes"
        }
    },
    {
        "$group": {
            "_id": "$dominant_topic",
            "count": {"$sum": 1}
        }
    },
    {
        "$sort": {"count": -1}
    }
]

results = list(db.email_new.aggregate(pipeline))

print("Unique dominant_topic values where follow_up_required='yes' and action_pending_status='yes':")
print("-" * 70)
for result in results:
    topic = result["_id"] if result["_id"] else "None/Empty"
    count = result["count"]
    print(f"Topic: {topic:<30} Count: {count}")

print(f"\nTotal unique topics found: {len(results)}")

# Close the connection
client.close()
