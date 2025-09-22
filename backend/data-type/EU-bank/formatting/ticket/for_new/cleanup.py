#!/usr/bin/env python3
"""
Script to clean up existing dates in the database
- Sets date fields to null instead of removing them
"""

import os
import asyncio
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# MongoDB connection from environment
MONGODB_URI = os.getenv("MONGO_CONNECTION_STRING")
DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION_NAME", "tickets")  # default if not set

# Ensure logs folder exists
os.makedirs("logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/date_cleanup.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


async def cleanup_dates():
    """Clean up existing dates in the database by setting them to null"""
    try:
        client = AsyncIOMotorClient(MONGODB_URI)
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]

        logger.info("Connected to MongoDB")

        total_tickets = await collection.count_documents({})
        logger.info(f"Total tickets to process: {total_tickets}")

        tickets_with_dates = await collection.count_documents({
            "$or": [
                {"thread.first_message_at": {"$exists": True, "$ne": None}},
                {"thread.last_message_at": {"$exists": True, "$ne": None}},
                {"messages.headers.date": {"$exists": True, "$ne": None}},
            ]
        })
        logger.info(f"Tickets with existing dates: {tickets_with_dates}")

        if tickets_with_dates == 0:
            logger.info("No tickets with dates found. Nothing to clean up.")
            return

        # Update tickets to set date fields to null
        update_operations = {
            "$set": {
                "thread.first_message_at": None,
                "thread.last_message_at": None,
                "messages.$[].headers.date": None,
            }
        }

        result = await collection.update_many({}, update_operations)
        logger.info(f"Updated {result.modified_count} tickets")

        # Verify cleanup
        remaining_dates = await collection.count_documents({
            "$or": [
                {"thread.first_message_at": {"$ne": None}},
                {"thread.last_message_at": {"$ne": None}},
                {"messages.headers.date": {"$ne": None}},
            ]
        })
        logger.info(f"Tickets with remaining dates after cleanup: {remaining_dates}")

        logger.info("Date cleanup completed successfully!")

    except Exception as e:
        logger.error(f"Error during date cleanup: {e}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")


async def main():
    await cleanup_dates()


if __name__ == "__main__":
    asyncio.run(main())
