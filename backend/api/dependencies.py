import os
import logging
from pymongo import MongoClient
from pymongo.database import Database
from typing import Generator
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

logger = logging.getLogger(__name__)

# MongoDB connection settings
MONGO_CONNECTION_STRING = os.getenv("MONGO_CONNECTION_STRING", "mongodb://ranjith:Ranjith@34.68.23.71:27017/admin")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "sparzaai")

# Global MongoDB client
mongodb_client: MongoClient = None


def get_mongodb_client() -> MongoClient:
    """
    Get MongoDB client instance
    """
    global mongodb_client
    
    if mongodb_client is None:
        try:
            mongodb_client = MongoClient(MONGO_CONNECTION_STRING)
            # Test the connection
            mongodb_client.admin.command('ping')
            logger.info(f"Connected to MongoDB at {MONGO_CONNECTION_STRING.split('@')[1] if '@' in MONGO_CONNECTION_STRING else MONGO_CONNECTION_STRING}")
        except Exception as e:
            logger.error(f"Failed to connect to MongoDB: {str(e)}")
            raise
    
    return mongodb_client


def get_database() -> Generator[Database, None, None]:
    """
    Dependency to get database instance
    
    Yields:
        Database: MongoDB database instance
    """
    try:
        client = get_mongodb_client()
        database = client[MONGO_DATABASE_NAME]
        yield database
    except Exception as e:
        logger.error(f"Error getting database: {str(e)}")
        raise


def close_mongodb_connection():
    """
    Close MongoDB connection
    """
    global mongodb_client
    
    if mongodb_client is not None:
        mongodb_client.close()
        mongodb_client = None
        logger.info("MongoDB connection closed")