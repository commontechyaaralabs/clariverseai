import logging
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException
from pymongo import database
from dependencies import get_database
from datetime import datetime

# Import authentication dependencies
from auth.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(tags=["statistics"])

@router.get("/home/stats")
async def get_home_statistics(
    data_type: Literal["email", "chat", "ticket", "socialmedia", "voice"],
    domain: Literal["banking"] = "banking",
    channel: str = None,
    db: database.Database = Depends(get_database),
    current_user: dict = Depends(get_current_user)
):
    """
    Get home page statistics for a specific data type and domain
    
    Args:
        data_type: Type of data (email, chat, ticket, socialmedia, voice)
        domain: Domain filter (banking)
        channel: Optional channel filter for socialmedia (twitter, reddit, trustpilot, app store/google play)
    
    Returns:
        Summary statistics for the dashboard
    """
    try:
        # Collection mapping
        collection_map = {
            "email": "emailmessages",
            "chat": "chat-chunks",
            "ticket": "tickets",
            "socialmedia": "socialmedia",
            "voice": "voice"
        }
        
        # Get collection - try different access patterns
        collection = None
        collection_name = collection_map[data_type]
        
        try:
            # Try accessing as direct database collection
            collection = db[collection_name]
            logger.info(f"Accessing collection directly: {collection_name}")
        except Exception as e1:
            logger.warning(f"Direct access failed: {e1}")
            try:
                # Try accessing through sparzaai database
                collection = db["sparzaai"][collection_name]
                logger.info(f"Accessing through sparzaai database: {collection_name}")
            except Exception as e2:
                logger.warning(f"sparzaai access failed: {e2}")
                try:
                    # Try get_collection method
                    collection = db.get_collection(collection_name)
                    logger.info(f"Using get_collection method: {collection_name}")
                except Exception as e3:
                    logger.error(f"All collection access methods failed: {e1}, {e2}, {e3}")
                    raise HTTPException(status_code=500, detail="Cannot access database collection")

        if data_type == "email":
            # First, let's check what's actually in the collection
            total_docs_in_collection = collection.count_documents({})
            logger.info(f"Total documents in {collection_name}: {total_docs_in_collection}")
            
            if total_docs_in_collection == 0:
                logger.warning(f"No documents found in collection {collection_name}")
                return {
                    "status": "success",
                    "statistics": {
                        "data_type": data_type,
                        "domain": domain,
                        "total_no_of_emails": 0,
                        "total_urgent_messages": 0,
                        "urgent_percentage": 0,
                        "total_dominant_clusters": 0,
                        "total_subclusters": 0,
                        "last_run_date": None
                    }
                }
            
            # Check what domains exist in the collection
            domain_pipeline = [
                {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            domain_results = list(collection.aggregate(domain_pipeline))
            logger.info(f"Available domains: {domain_results}")
            
            # Base query with domain filter
            base_query = {"domain": domain}
            
            # Total No of Emails - sum of all records from the collection with domain filter
            total_emails = collection.count_documents(base_query)
            logger.info(f"Documents with domain '{domain}': {total_emails}")
            
            # If no documents found with domain filter, check if domain field exists
            if total_emails == 0:
                # Sample a document to check structure
                sample_doc = collection.find_one({})
                logger.info(f"Sample document fields: {list(sample_doc.keys()) if sample_doc else 'No documents'}")
                
                # Try without domain filter to see if we have any data
                total_emails_no_filter = collection.count_documents({})
                if total_emails_no_filter > 0:
                    logger.warning(f"Found {total_emails_no_filter} documents without domain filter, but 0 with domain='{domain}'")
                    # You might want to proceed without domain filter or use a different domain value
                    # For now, let's try without domain filter
                    base_query = {}
                    total_emails = total_emails_no_filter
                else:
                    logger.error("No documents found even without domain filter")
            
            # Total Urgent Messages - sum of True records in urgency field
            urgent_query = {**base_query, "urgency": True}
            total_urgent_messages = collection.count_documents(urgent_query)
            logger.info(f"Urgent messages: {total_urgent_messages}")
            
            # Also try with urgency as string "true" in case it's stored differently
            if total_urgent_messages == 0 and total_emails > 0:
                urgent_query_str = {**base_query, "urgency": "true"}
                total_urgent_messages_str = collection.count_documents(urgent_query_str)
                if total_urgent_messages_str > 0:
                    total_urgent_messages = total_urgent_messages_str
                    logger.info(f"Found urgent messages with string 'true': {total_urgent_messages}")
            
            # Urgent % - calculate percentage using Total No of emails and Total urgent messages
            urgent_percentage = round((total_urgent_messages / total_emails * 100), 2) if total_emails > 0 else 0
            
            # Total Dominant Clusters - unique numbers from kmeans_cluster_id field
            dominant_clusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$kmeans_cluster_id"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            dominant_clusters_result = list(collection.aggregate(dominant_clusters_pipeline))
            total_dominant_clusters = dominant_clusters_result[0]["total"] if dominant_clusters_result else 0
            logger.info(f"Dominant clusters: {total_dominant_clusters}")
            
            # Total Subclusters - unique labels from subcluster_label field
            subclusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$subcluster_label"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            subclusters_result = list(collection.aggregate(subclusters_pipeline))
            total_subclusters = subclusters_result[0]["total"] if subclusters_result else 0
            logger.info(f"Subclusters: {total_subclusters}")
            
            # Last Run Date - latest date and time from processed_at field
            last_run_pipeline = [
                {"$match": base_query},
                {"$match": {"processed_at": {"$ne": None}}},  # Exclude null values
                {"$sort": {"processed_at": -1}},
                {"$limit": 1},
                {"$project": {"processed_at": 1, "_id": 0}}
            ]
            last_run_result = list(collection.aggregate(last_run_pipeline))
            last_run_date = last_run_result[0]["processed_at"] if last_run_result else None
            logger.info(f"Last run date raw: {last_run_date}")
            
            # Format last_run_date if it exists
            formatted_last_run_date = None
            if last_run_date:
                try:
                    # Handle different date formats
                    if isinstance(last_run_date, str):
                        # Parse the string date format "2025-08-01 13:06:59"
                        dt = datetime.strptime(last_run_date, "%Y-%m-%d %H:%M:%S")
                        formatted_last_run_date = dt.strftime("%Y-%m-%d %H:%M")
                    elif isinstance(last_run_date, datetime):
                        formatted_last_run_date = last_run_date.strftime("%Y-%m-%d %H:%M")
                except Exception as date_error:
                    logger.warning(f"Error formatting date: {date_error}")
                    formatted_last_run_date = str(last_run_date)
            
            return {
                "status": "success",
                "statistics": {
                    "data_type": data_type,
                    "domain": domain,
                    "total_no_of_emails": total_emails,
                    "total_urgent_messages": total_urgent_messages,
                    "urgent_percentage": urgent_percentage,
                    "total_dominant_clusters": total_dominant_clusters,
                    "total_subclusters": total_subclusters,
                    "last_run_date": formatted_last_run_date
                }
            }
            
        elif data_type == "chat":
            # First, let's check what's actually in the collection
            total_docs_in_collection = collection.count_documents({})
            logger.info(f"Total documents in {collection_name}: {total_docs_in_collection}")
            
            if total_docs_in_collection == 0:
                logger.warning(f"No documents found in collection {collection_name}")
                return {
                    "status": "success",
                    "statistics": {
                        "data_type": data_type,
                        "domain": domain,
                        "total_no_of_emails": 0,
                        "total_urgent_messages": 0,
                        "urgent_percentage": 0,
                        "total_dominant_clusters": 0,
                        "total_subclusters": 0,
                        "last_run_date": None
                    }
                }
            
            # Check what domains exist in the collection
            domain_pipeline = [
                {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            domain_results = list(collection.aggregate(domain_pipeline))
            logger.info(f"Available domains in chat: {domain_results}")
            
            # Base query with domain filter
            base_query = {"domain": domain}
            
            # Total No of Chat Messages - sum of all records from the collection with domain filter
            total_messages = collection.count_documents(base_query)
            logger.info(f"Chat documents with domain '{domain}': {total_messages}")
            
            # If no documents found with domain filter, check if domain field exists
            if total_messages == 0:
                # Sample a document to check structure
                sample_doc = collection.find_one({})
                logger.info(f"Sample chat document fields: {list(sample_doc.keys()) if sample_doc else 'No documents'}")
                
                # Try without domain filter to see if we have any data
                total_messages_no_filter = collection.count_documents({})
                if total_messages_no_filter > 0:
                    logger.warning(f"Found {total_messages_no_filter} chat documents without domain filter, but 0 with domain='{domain}'")
                    base_query = {}
                    total_messages = total_messages_no_filter
                else:
                    logger.error("No chat documents found even without domain filter")
            
            # Total Urgent Messages - sum of True records in urgency field
            urgent_query = {**base_query, "urgency": True}
            total_urgent_messages = collection.count_documents(urgent_query)
            logger.info(f"Urgent chat messages: {total_urgent_messages}")
            
            # Also try with urgency as string "true" in case it's stored differently
            if total_urgent_messages == 0 and total_messages > 0:
                urgent_query_str = {**base_query, "urgency": "true"}
                total_urgent_messages_str = collection.count_documents(urgent_query_str)
                if total_urgent_messages_str > 0:
                    total_urgent_messages = total_urgent_messages_str
                    logger.info(f"Found urgent chat messages with string 'true': {total_urgent_messages}")
            
            # Urgent % - calculate percentage using Total No of messages and Total urgent messages
            urgent_percentage = round((total_urgent_messages / total_messages * 100), 2) if total_messages > 0 else 0
            
            # Total Dominant Clusters - unique numbers from kmeans_cluster_id field
            dominant_clusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$kmeans_cluster_id"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            dominant_clusters_result = list(collection.aggregate(dominant_clusters_pipeline))
            total_dominant_clusters = dominant_clusters_result[0]["total"] if dominant_clusters_result else 0
            logger.info(f"Chat dominant clusters: {total_dominant_clusters}")
            
            # Total Subclusters - unique labels from subcluster_label field
            subclusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$subcluster_label"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            subclusters_result = list(collection.aggregate(subclusters_pipeline))
            total_subclusters = subclusters_result[0]["total"] if subclusters_result else 0
            logger.info(f"Chat subclusters: {total_subclusters}")
            
            # Last Run Date - latest date and time from processed_at field
            last_run_pipeline = [
                {"$match": base_query},
                {"$match": {"processed_at": {"$ne": None}}},  # Exclude null values
                {"$sort": {"processed_at": -1}},
                {"$limit": 1},
                {"$project": {"processed_at": 1, "_id": 0}}
            ]
            last_run_result = list(collection.aggregate(last_run_pipeline))
            last_run_date = last_run_result[0]["processed_at"] if last_run_result else None
            logger.info(f"Chat last run date raw: {last_run_date}")
            
            # Format last_run_date if it exists
            formatted_last_run_date = None
            if last_run_date:
                try:
                    # Handle different date formats
                    if isinstance(last_run_date, str):
                        # Parse the string date format "2025-08-01 13:06:59"
                        dt = datetime.strptime(last_run_date, "%Y-%m-%d %H:%M:%S")
                        formatted_last_run_date = dt.strftime("%Y-%m-%d %H:%M")
                    elif isinstance(last_run_date, datetime):
                        formatted_last_run_date = last_run_date.strftime("%Y-%m-%d %H:%M")
                except Exception as date_error:
                    logger.warning(f"Error formatting chat date: {date_error}")
                    formatted_last_run_date = str(last_run_date)
            
            return {
                "status": "success",
                "statistics": {
                    "data_type": data_type,
                    "domain": domain,
                    "total_no_of_emails": total_messages,
                    "total_urgent_messages": total_urgent_messages,
                    "urgent_percentage": urgent_percentage,
                    "total_dominant_clusters": total_dominant_clusters,
                    "total_subclusters": total_subclusters,
                    "last_run_date": formatted_last_run_date
                }
            }
            
        elif data_type == "ticket":
            # First, let's check what's actually in the collection
            total_docs_in_collection = collection.count_documents({})
            logger.info(f"Total documents in {collection_name}: {total_docs_in_collection}")
            
            if total_docs_in_collection == 0:
                logger.warning(f"No documents found in collection {collection_name}")
                return {
                    "status": "success",
                    "statistics": {
                        "data_type": data_type,
                        "domain": domain,
                        "total_no_of_emails": 0,
                        "total_urgent_messages": 0,
                        "urgent_percentage": 0,
                        "total_dominant_clusters": 0,
                        "total_subclusters": 0,
                        "last_run_date": None
                    }
                }
            
            # Check what domains exist in the collection
            domain_pipeline = [
                {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            domain_results = list(collection.aggregate(domain_pipeline))
            logger.info(f"Available domains in tickets: {domain_results}")
            
            # Base query with domain filter
            base_query = {"domain": domain}
            
            # Total No of Tickets - sum of all records from the collection with domain filter
            total_tickets = collection.count_documents(base_query)
            logger.info(f"Ticket documents with domain '{domain}': {total_tickets}")
            
            # If no documents found with domain filter, check if domain field exists
            if total_tickets == 0:
                # Sample a document to check structure
                sample_doc = collection.find_one({})
                logger.info(f"Sample ticket document fields: {list(sample_doc.keys()) if sample_doc else 'No documents'}")
                
                # Try without domain filter to see if we have any data
                total_tickets_no_filter = collection.count_documents({})
                if total_tickets_no_filter > 0:
                    logger.warning(f"Found {total_tickets_no_filter} ticket documents without domain filter, but 0 with domain='{domain}'")
                    base_query = {}
                    total_tickets = total_tickets_no_filter
                else:
                    logger.error("No ticket documents found even without domain filter")
            
            # Total Urgent Messages - sum of True records in urgency field
            urgent_query = {**base_query, "urgency": True}
            total_urgent_messages = collection.count_documents(urgent_query)
            logger.info(f"Urgent tickets: {total_urgent_messages}")
            
            # Also try with urgency as string "true" in case it's stored differently
            if total_urgent_messages == 0 and total_tickets > 0:
                urgent_query_str = {**base_query, "urgency": "true"}
                total_urgent_messages_str = collection.count_documents(urgent_query_str)
                if total_urgent_messages_str > 0:
                    total_urgent_messages = total_urgent_messages_str
                    logger.info(f"Found urgent tickets with string 'true': {total_urgent_messages}")
            
            # Urgent % - calculate percentage using Total No of tickets and Total urgent messages
            urgent_percentage = round((total_urgent_messages / total_tickets * 100), 2) if total_tickets > 0 else 0
            
            # Total Dominant Clusters - unique numbers from kmeans_cluster_id field
            dominant_clusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$kmeans_cluster_id"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            dominant_clusters_result = list(collection.aggregate(dominant_clusters_pipeline))
            total_dominant_clusters = dominant_clusters_result[0]["total"] if dominant_clusters_result else 0
            logger.info(f"Ticket dominant clusters: {total_dominant_clusters}")
            
            # Total Subclusters - unique labels from subcluster_label field
            subclusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$subcluster_label"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            subclusters_result = list(collection.aggregate(subclusters_pipeline))
            total_subclusters = subclusters_result[0]["total"] if subclusters_result else 0
            logger.info(f"Ticket subclusters: {total_subclusters}")
            
            # Last Run Date - latest date and time from processed_at field
            last_run_pipeline = [
                {"$match": base_query},
                {"$match": {"processed_at": {"$ne": None}}},  # Exclude null values
                {"$sort": {"processed_at": -1}},
                {"$limit": 1},
                {"$project": {"processed_at": 1, "_id": 0}}
            ]
            last_run_result = list(collection.aggregate(last_run_pipeline))
            last_run_date = last_run_result[0]["processed_at"] if last_run_result else None
            logger.info(f"Ticket last run date raw: {last_run_date}")
            
            # Format last_run_date if it exists
            formatted_last_run_date = None
            if last_run_date:
                try:
                    # Handle different date formats
                    if isinstance(last_run_date, str):
                        # Parse the string date format "2025-08-01 13:06:59"
                        dt = datetime.strptime(last_run_date, "%Y-%m-%d %H:%M:%S")
                        formatted_last_run_date = dt.strftime("%Y-%m-%d %H:%M")
                    elif isinstance(last_run_date, datetime):
                        formatted_last_run_date = last_run_date.strftime("%Y-%m-%d %H:%M")
                except Exception as date_error:
                    logger.warning(f"Error formatting ticket date: {date_error}")
                    formatted_last_run_date = str(last_run_date)
            
            return {
                "status": "success",
                "statistics": {
                    "data_type": data_type,
                    "domain": domain,
                    "total_no_of_emails": total_tickets,
                    "total_urgent_messages": total_urgent_messages,
                    "urgent_percentage": urgent_percentage,
                    "total_dominant_clusters": total_dominant_clusters,
                    "total_subclusters": total_subclusters,
                    "last_run_date": formatted_last_run_date
                }
            }
            
        elif data_type == "socialmedia":
            # First, let's check what's actually in the collection
            total_docs_in_collection = collection.count_documents({})
            logger.info(f"Total documents in {collection_name}: {total_docs_in_collection}")
            
            if total_docs_in_collection == 0:
                logger.warning(f"No documents found in collection {collection_name}")
                # Build empty statistics response - only include channel for socialmedia
                statistics = {
                    "data_type": data_type,
                    "domain": domain,
                    "total_no_of_emails": 0,
                    "total_urgent_messages": 0,
                    "urgent_percentage": 0,
                    "total_dominant_clusters": 0,
                    "total_subclusters": 0,
                    "last_run_date": None
                }
                
                # Only add channel field for socialmedia data type
                if data_type == "socialmedia":
                    statistics["channel"] = channel
                
                return {
                    "status": "success",
                    "statistics": statistics
                }
            
            # Check what domains exist in the collection
            domain_pipeline = [
                {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            domain_results = list(collection.aggregate(domain_pipeline))
            logger.info(f"Available domains in socialmedia: {domain_results}")
            
            # Check what channels exist in the collection
            channel_pipeline = [
                {"$group": {"_id": "$channel", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            channel_results = list(collection.aggregate(channel_pipeline))
            logger.info(f"Available channels in socialmedia: {channel_results}")
            
            # Base query with domain filter
            base_query = {"domain": domain}
            
            # Add channel filter if provided
            if channel:
                # Normalize channel name for comparison
                channel_normalized = channel.lower().strip()
                # Map common variations to standard channel names
                channel_mapping = {
                    "app store": "App Store/Google Play",
                    "google play": "App Store/Google Play",
                    "app store/google play": "App Store/Google Play",
                    "twitter": "Twitter",
                    "reddit": "Reddit",
                    "trustpilot": "Trustpilot"
                }
                
                # Use mapped channel name or original channel
                mapped_channel = channel_mapping.get(channel_normalized, channel)
                base_query["channel"] = mapped_channel
                logger.info(f"Filtering by channel: {mapped_channel}")
            
            # Total No of Social Media Messages - sum of all records from the collection with domain and channel filters
            total_social_messages = collection.count_documents(base_query)
            logger.info(f"Social media documents with domain '{domain}' and channel '{channel}': {total_social_messages}")
            
            # If no documents found with domain filter, check if domain field exists
            if total_social_messages == 0:
                # Sample a document to check structure
                sample_doc = collection.find_one({})
                logger.info(f"Sample social media document fields: {list(sample_doc.keys()) if sample_doc else 'No documents'}")
                
                # Try without domain filter to see if we have any data
                total_social_messages_no_filter = collection.count_documents({})
                if total_social_messages_no_filter > 0:
                    logger.warning(f"Found {total_social_messages_no_filter} social media documents without domain filter, but 0 with domain='{domain}'")
                    # Try with just domain filter (no channel)
                    base_query = {"domain": domain}
                    total_social_messages = collection.count_documents(base_query)
                    if total_social_messages == 0:
                        # If still no results, try without any filters
                        base_query = {}
                        total_social_messages = total_social_messages_no_filter
                else:
                    logger.error("No social media documents found even without domain filter")
            
            # Total Urgent Messages - sum of True records in urgency field
            urgent_query = {**base_query, "urgency": True}
            total_urgent_messages = collection.count_documents(urgent_query)
            logger.info(f"Urgent social media messages: {total_urgent_messages}")
            
            # Also try with urgency as string "true" in case it's stored differently
            if total_urgent_messages == 0 and total_social_messages > 0:
                urgent_query_str = {**base_query, "urgency": "true"}
                total_urgent_messages_str = collection.count_documents(urgent_query_str)
                if total_urgent_messages_str > 0:
                    total_urgent_messages = total_urgent_messages_str
                    logger.info(f"Found urgent social media messages with string 'true': {total_urgent_messages}")
            
            # Urgent % - calculate percentage using Total No of social media messages and Total urgent messages
            urgent_percentage = round((total_urgent_messages / total_social_messages * 100), 2) if total_social_messages > 0 else 0
            
            # Total Dominant Clusters - unique numbers from kmeans_cluster_id field
            dominant_clusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$kmeans_cluster_id"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            dominant_clusters_result = list(collection.aggregate(dominant_clusters_pipeline))
            total_dominant_clusters = dominant_clusters_result[0]["total"] if dominant_clusters_result else 0
            logger.info(f"Social media dominant clusters: {total_dominant_clusters}")
            
            # Total Subclusters - unique labels from subcluster_label field
            subclusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$subcluster_label"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            subclusters_result = list(collection.aggregate(subclusters_pipeline))
            total_subclusters = subclusters_result[0]["total"] if subclusters_result else 0
            logger.info(f"Social media subclusters: {total_subclusters}")
            
            # Last Run Date - latest date and time from content_generated_at field
            last_run_pipeline = [
                {"$match": base_query},
                {"$match": {"content_generated_at": {"$ne": None}}},  # Exclude null values
                {"$sort": {"content_generated_at": -1}},
                {"$limit": 1},
                {"$project": {"content_generated_at": 1, "_id": 0}}
            ]
            last_run_result = list(collection.aggregate(last_run_pipeline))
            last_run_date = last_run_result[0]["content_generated_at"] if last_run_result else None
            logger.info(f"Social media last run date raw: {last_run_date}")
            
            # Format last_run_date if it exists
            formatted_last_run_date = None
            if last_run_date:
                try:
                    # Handle different date formats
                    if isinstance(last_run_date, str):
                        # Parse ISO format or other string formats
                        try:
                            dt = datetime.fromisoformat(last_run_date.replace('Z', '+00:00'))
                        except:
                            # Try other common formats
                            dt = datetime.strptime(last_run_date, "%Y-%m-%d %H:%M:%S")
                        formatted_last_run_date = dt.strftime("%Y-%m-%d %H:%M")
                    elif isinstance(last_run_date, datetime):
                        formatted_last_run_date = last_run_date.strftime("%Y-%m-%d %H:%M")
                except Exception as date_error:
                    logger.warning(f"Error formatting social media date: {date_error}")
                    formatted_last_run_date = str(last_run_date)
            
            # Get cluster counts from clusters collection for socialmedia
            try:
                # Access clusters collection
                try:
                    clusters_collection = db["cluster"]
                    logger.info("Accessing cluster collection directly for socialmedia")
                except Exception as e1:
                    try:
                        clusters_collection = db["sparzaai"]["cluster"]
                        logger.info("Accessing cluster collection through sparzaai database for socialmedia")
                    except Exception as e2:
                        try:
                            clusters_collection = db.get_collection("cluster")
                            logger.info("Using get_collection method for cluster for socialmedia")
                        except Exception as e3:
                            logger.error(f"All cluster collection access methods failed for socialmedia: {e1}, {e2}, {e3}")
                            clusters_collection = None
                
                if clusters_collection:
                    # Count clusters for socialmedia data
                    cluster_query = {"data": "socialmedia"}
                    if domain:
                        cluster_query["domains"] = domain
                    
                    total_dominant_clusters = clusters_collection.count_documents(cluster_query)
                    logger.info(f"Social media dominant clusters from cluster collection: {total_dominant_clusters}")
                    
                    # Count subclusters
                    subcluster_pipeline = [
                        {"$match": cluster_query},
                        {"$unwind": "$subclusters"},
                        {"$count": "total"}
                    ]
                    subcluster_result = list(clusters_collection.aggregate(subcluster_pipeline))
                    total_subclusters = subcluster_result[0]["total"] if subcluster_result else 0
                    logger.info(f"Social media subclusters from cluster collection: {total_subclusters}")
                else:
                    logger.warning("Could not access cluster collection for socialmedia, using document-based counts")
            except Exception as cluster_error:
                logger.warning(f"Error accessing cluster collection for socialmedia: {cluster_error}, using document-based counts")
            
            # Build statistics response - only include channel for socialmedia
            statistics = {
                "data_type": data_type,
                "domain": domain,
                "total_no_of_emails": total_social_messages,
                "total_urgent_messages": total_urgent_messages,
                "urgent_percentage": urgent_percentage,
                "total_dominant_clusters": total_dominant_clusters,
                "total_subclusters": total_subclusters,
                "last_run_date": formatted_last_run_date
            }
            
            # Only add channel field for socialmedia data type
            if data_type == "socialmedia":
                statistics["channel"] = channel
            
            return {
                "status": "success",
                "statistics": statistics
            }
            
        elif data_type == "voice":
            # First, let's check what's actually in the collection
            total_docs_in_collection = collection.count_documents({})
            logger.info(f"Total documents in {collection_name}: {total_docs_in_collection}")
            
            if total_docs_in_collection == 0:
                logger.warning(f"No documents found in collection {collection_name}")
                return {
                    "status": "success",
                    "statistics": {
                        "data_type": data_type,
                        "domain": domain,
                        "total_no_of_emails": 0,
                        "total_urgent_messages": 0,
                        "urgent_percentage": 0,
                        "total_dominant_clusters": 0,
                        "total_subclusters": 0,
                        "last_run_date": None
                    }
                }
            
            # Check what domains exist in the collection
            domain_pipeline = [
                {"$group": {"_id": "$domain", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            domain_results = list(collection.aggregate(domain_pipeline))
            logger.info(f"Available domains in voice: {domain_results}")
            
            # Base query with domain filter
            base_query = {"domain": domain}
            
            # Total No of Voice Messages - sum of all records from the collection with domain filter
            total_voice_messages = collection.count_documents(base_query)
            logger.info(f"Voice documents with domain '{domain}': {total_voice_messages}")
            
            # If no documents found with domain filter, check if domain field exists
            if total_voice_messages == 0:
                # Sample a document to check structure
                sample_doc = collection.find_one({})
                logger.info(f"Sample voice document fields: {list(sample_doc.keys()) if sample_doc else 'No documents'}")
                
                # Try without domain filter to see if we have any data
                total_voice_messages_no_filter = collection.count_documents({})
                if total_voice_messages_no_filter > 0:
                    logger.warning(f"Found {total_voice_messages_no_filter} voice documents without domain filter, but 0 with domain='{domain}'")
                    base_query = {}
                    total_voice_messages = total_voice_messages_no_filter
                else:
                    logger.error("No voice documents found even without domain filter")
            
            # Total Urgent Messages - sum of True records in urgency field
            urgent_query = {**base_query, "urgency": True}
            total_urgent_messages = collection.count_documents(urgent_query)
            logger.info(f"Urgent voice messages: {total_urgent_messages}")
            
            # Also try with urgency as string "true" in case it's stored differently
            if total_urgent_messages == 0 and total_voice_messages > 0:
                urgent_query_str = {**base_query, "urgency": "true"}
                total_urgent_messages_str = collection.count_documents(urgent_query_str)
                if total_urgent_messages_str > 0:
                    total_urgent_messages = total_urgent_messages_str
                    logger.info(f"Found urgent voice messages with string 'true': {total_urgent_messages}")
            
            # Urgent % - calculate percentage using Total No of voice messages and Total urgent messages
            urgent_percentage = round((total_urgent_messages / total_voice_messages * 100), 2) if total_voice_messages > 0 else 0
            
            # Total Dominant Clusters - unique numbers from kmeans_cluster_id field
            dominant_clusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$kmeans_cluster_id"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            dominant_clusters_result = list(collection.aggregate(dominant_clusters_pipeline))
            total_dominant_clusters = dominant_clusters_result[0]["total"] if dominant_clusters_result else 0
            logger.info(f"Voice dominant clusters: {total_dominant_clusters}")
            
            # Total Subclusters - unique labels from subcluster_label field
            subclusters_pipeline = [
                {"$match": base_query},
                {"$group": {"_id": "$subcluster_label"}},
                {"$match": {"_id": {"$ne": None}}},  # Exclude null values
                {"$count": "total"}
            ]
            subclusters_result = list(collection.aggregate(subclusters_pipeline))
            total_subclusters = subclusters_result[0]["total"] if subclusters_result else 0
            logger.info(f"Voice subclusters: {total_subclusters}")
            
            # Last Run Date - latest date and time from processed_at field
            last_run_pipeline = [
                {"$match": base_query},
                {"$match": {"processed_at": {"$ne": None}}},  # Exclude null values
                {"$sort": {"processed_at": -1}},
                {"$limit": 1},
                {"$project": {"processed_at": 1, "_id": 0}}
            ]
            last_run_result = list(collection.aggregate(last_run_pipeline))
            last_run_date = last_run_result[0]["processed_at"] if last_run_result else None
            logger.info(f"Voice last run date raw: {last_run_date}")
            
            # Format last_run_date if it exists
            formatted_last_run_date = None
            if last_run_date:
                try:
                    # Handle different date formats
                    if isinstance(last_run_date, str):
                        # Parse the string date format "2025-08-01 13:06:59"
                        dt = datetime.strptime(last_run_date, "%Y-%m-%d %H:%M:%S")
                        formatted_last_run_date = dt.strftime("%Y-%m-%d %H:%M")
                    elif isinstance(last_run_date, datetime):
                        formatted_last_run_date = last_run_date.strftime("%Y-%m-%d %H:%M")
                except Exception as date_error:
                    logger.warning(f"Error formatting voice date: {date_error}")
                    formatted_last_run_date = str(last_run_date)
            
            return {
                "status": "success",
                "statistics": {
                    "data_type": data_type,
                    "domain": domain,
                    "total_no_of_emails": total_voice_messages,
                    "total_urgent_messages": total_urgent_messages,
                    "urgent_percentage": urgent_percentage,
                    "total_dominant_clusters": total_dominant_clusters,
                    "total_subclusters": total_subclusters,
                    "last_run_date": formatted_last_run_date
                }
            }
            
    except Exception as e:
        logger.error(f"Error retrieving home statistics for {data_type}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving home statistics: {str(e)}"
        )