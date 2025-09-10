import logging
from typing import Literal, Optional, List, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query
from pymongo import database
from dependencies import get_database
from pydantic import BaseModel, Field
from datetime import datetime

# Import authentication dependencies
from auth.dependencies import get_current_user

logger = logging.getLogger(__name__)

router = APIRouter(tags=["topic-analysis"])

# Pydantic models for request/response
class ChatMember(BaseModel):
    id: Optional[str] = None
    roles: Optional[List[str]] = []
    display_name: Optional[str] = None
    user_id: Optional[str] = None
    email: Optional[str] = None
    tenant_id: Optional[str] = None

# Base document response with common fields
class BaseDocumentResponse(BaseModel):
    id: str = Field(alias="_id")
    domain: Optional[str] = None
    cleaned_text: Optional[str] = None
    lemmatized_text: Optional[str] = None
    preprocessed_text: Optional[str] = None
    dominant_topic: Optional[str] = None
    model_used: Optional[str] = None
    processed_at: Optional[str] = None
    subtopics: Optional[str] = None
    urgency: Optional[bool] = None
    was_summarized: Optional[bool] = None
    clustering_method: Optional[str] = None
    clustering_updated_at: Optional[float] = None
    kmeans_cluster_id: Optional[int] = None
    kmeans_cluster_keyphrase: Optional[str] = None
    dominant_cluster_label: Optional[str] = None
    subcluster_label: Optional[str] = None
    subcluster_id: Optional[str] = None

    class Config:
        populate_by_name = True

# Email-specific document response
class EmailDocumentResponse(BaseDocumentResponse):
    message_id: Optional[str] = None
    conversation_id: Optional[str] = None
    sender_id: Optional[str] = None
    sender_name: Optional[str] = None
    receiver_ids: Optional[List[str]] = []
    receiver_names: Optional[List[str]] = []
    timestamp: Optional[str] = None
    subject: Optional[str] = None
    message_text: Optional[str] = None
    time_taken: Optional[float] = None

# Chat-specific document response
class ChatDocumentResponse(BaseDocumentResponse):
    chat_id: Optional[str] = None
    chat_members: Optional[List[ChatMember]] = []
    raw_segments: Optional[List[Dict[str, Any]]] = []
    cleaned_segments: Optional[List[Dict[str, Any]]] = []
    total_messages: Optional[int] = None
    created_at: Optional[str] = None

# Ticket-specific document response
class TicketDocumentResponse(BaseDocumentResponse):
    ticket_number: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    priority: Optional[str] = None
    created: Optional[str] = None
    ticket_id: Optional[str] = None
    ticket_status: Optional[str] = None
    ticket_priority: Optional[str] = None
    ticket_category: Optional[str] = None
    ticket_assignee: Optional[str] = None
    ticket_created_at: Optional[str] = None
    ticket_updated_at: Optional[str] = None

# Twitter-specific document response
class TwitterDocumentResponse(BaseDocumentResponse):
    tweet_id: Optional[str] = None
    user_id: Optional[str] = None
    username: Optional[str] = None
    email_id: Optional[str] = None
    text: Optional[str] = None
    created_at: Optional[str] = None
    retweet_count: Optional[int] = None
    like_count: Optional[int] = None
    reply_count: Optional[int] = None
    quote_count: Optional[int] = None
    hashtags: Optional[List[str]] = []
    priority: Optional[str] = None
    sentiment: Optional[str] = None

# Voice-specific document response
class VoiceDocumentResponse(BaseDocumentResponse):
    call_id: Optional[str] = None
    timestamp: Optional[str] = None
    customer_name: Optional[str] = None
    customer_id: Optional[str] = None
    email: Optional[str] = None
    call_purpose: Optional[str] = None
    conversation: Optional[List[dict]] = None
    priority: Optional[str] = None
    resolution_status: Optional[str] = None
    sentiment: Optional[str] = None

class SocialMediaDocumentResponse(BaseDocumentResponse):
    # Common fields for all social media platforms
    channel: Optional[str] = None
    username: Optional[str] = None
    email_id: Optional[str] = None
    user_id: Optional[str] = None
    text: Optional[str] = None
    sentiment: Optional[str] = None
    priority: Optional[str] = None
    urgency: Optional[bool] = None
    created_at: Optional[str] = None
    content_generated_at: Optional[str] = None
    dominant_topic: Optional[str] = None
    subtopics: Optional[str] = None
    dominant_cluster_label: Optional[str] = None
    kmeans_cluster_id: Optional[int] = None
    subcluster_id: Optional[str] = None
    subcluster_label: Optional[str] = None
    kmeans_cluster_keyphrase: Optional[str] = None
    
    # Platform-specific fields
    # Twitter fields
    tweet_id: Optional[str] = None
    hashtags: Optional[List[str]] = []
    like_count: Optional[int] = None
    retweet_count: Optional[int] = None
    reply_count: Optional[int] = None
    quote_count: Optional[int] = None
    
    # Reddit fields
    post_id: Optional[str] = None
    subreddit: Optional[str] = None
    comment_count: Optional[int] = None
    share_count: Optional[int] = None
    
    # Trustpilot fields
    review_id: Optional[str] = None
    rating: Optional[int] = None
    useful_count: Optional[int] = None
    date_of_experience: Optional[str] = None
    review_title: Optional[str] = None
    
    # App Store/Google Play fields
    platform: Optional[str] = None
    review_helpful: Optional[int] = None

# Union type for all document responses
DocumentResponse = EmailDocumentResponse | ChatDocumentResponse | TicketDocumentResponse | TwitterDocumentResponse | VoiceDocumentResponse | SocialMediaDocumentResponse

# Request model for email search (for POST - keeping for reference but not used in GET)
class EmailSearchRequest(BaseModel):
    sender_id: str = Field(..., description="Email address to search for (e.g., 'jamesdavidson5023@gmail.com')")

# Response model for email search
class EmailSearchResponse(BaseModel):
    status: str
    email_address: str
    total_documents: int
    email_documents: List[EmailDocumentResponse]
    chat_documents: List[ChatDocumentResponse]

class DominantClusterOption(BaseModel):
    kmeans_cluster_id: int
    dominant_cluster_label: str
    cluster_name: Optional[str] = None
    keyphrases: List[str]
    keyphrase_count: int
    document_count: Optional[int] = None
    urgent_count: Optional[int] = None
    urgent_percentage: Optional[float] = None

class SubclusterOption(BaseModel):
    kmeans_cluster_id: int
    dominant_cluster_label: str
    subcluster_id: str
    subcluster_label: str
    keyphrases: List[str]
    keyphrase_count: int
    document_count: Optional[int] = None
    urgent_count: Optional[int] = None
    urgent_percentage: Optional[float] = None

class ClusterOptionsResponse(BaseModel):
    status: str
    data_type: str
    domain: str
    channel: Optional[str] = None
    dominant_clusters: List[Dict[str, Any]]
    subclusters: List[Dict[str, Any]]

class TopicAnalysisResponse(BaseModel):
    status: str
    data_type: str
    domain: str
    channel: Optional[str] = None
    filters: Dict[str, Any]
    pagination: Dict[str, Any]
    documents: List[DocumentResponse]  # Use union type to handle all document types

def get_collection(db: database.Database, data_type: str):
    """Helper function to get collection with multiple access patterns"""
    collection_map = {
        "email": "emailmessages",
        "chat": "chat-chunks",
        "ticket": "tickets",
        "socialmedia": "socialmedia",
        "voice": "voice"
    }
    
    collection_name = collection_map[data_type]
    
    # Try different access patterns for collection
    try:
        collection = db[collection_name]
        logger.info(f"Accessing collection directly: {collection_name}")
        return collection
    except Exception as e1:
        logger.warning(f"Direct access failed: {e1}")
        try:
            collection = db["sparzaai"][collection_name]
            logger.info(f"Accessing through sparzaai database: {collection_name}")
            return collection
        except Exception as e2:
            logger.warning(f"sparzaai access failed: {e2}")
            try:
                collection = db.get_collection(collection_name)
                logger.info(f"Using get_collection method: {collection_name}")
                return collection
            except Exception as e3:
                logger.error(f"All collection access methods failed: {e1}, {e2}, {e3}")
                raise HTTPException(status_code=500, detail="Cannot access database collection")

def get_base_query(collection, domain: str):
    """Helper function to determine base query with domain fallback"""
    base_query = {"domain": domain}
    
    # Check if collection has data with domain filter
    total_docs = collection.count_documents(base_query)
    if total_docs == 0:
        logger.warning(f"No documents found with domain filter '{domain}'")
        # Try without domain filter
        total_docs_no_filter = collection.count_documents({})
        if total_docs_no_filter > 0:
            base_query = {}
            logger.info(f"Using query without domain filter, found {total_docs_no_filter} documents")
    
    return base_query

@router.get("/topic-analysis/clusters")
async def get_cluster_options(
    data_type: Literal["email", "chat", "ticket", "socialmedia", "voice"],
    domain: Literal["banking"] = "banking",
    channel: str = None,
    db: database.Database = Depends(get_database),
    current_user: dict = Depends(get_current_user)
):
    """
    Get available dominant clusters and subclusters for dropdown menus
    Retrieves keyphrases and keyphrase counts from clusters collection
    
    Args:
        data_type: Type of data (email, chat, ticket, socialmedia, voice)
        domain: Domain filter (banking)
        channel: Optional channel filter for socialmedia (twitter, reddit, trustpilot, app store/google play)
    
    Returns:
        Available clusters and subclusters with their IDs, labels, keyphrases and counts
    """
    try:
        # Use clusters collection for keyphrase data
        try:
            clusters_collection = db["cluster"]  # Use "cluster" collection name
            logger.info("Accessing cluster collection directly")
        except Exception as e1:
            try:
                clusters_collection = db["sparzaai"]["cluster"]  # Use "cluster" collection name
                logger.info("Accessing cluster collection through sparzaai database")
            except Exception as e2:
                try:
                    clusters_collection = db.get_collection("cluster")  # Use "cluster" collection name
                    logger.info("Using get_collection method for cluster")
                except Exception as e3:
                    logger.error(f"All cluster collection access methods failed: {e1}, {e2}, {e3}")
                    raise HTTPException(status_code=500, detail="Cannot access cluster collection")

        # Map data_type to data field value in clusters collection
        data_type_map = {
            "email": "email",
            "chat": "chat-chunks", 
            "ticket": "tickets",
            "socialmedia": "socialmedia",
            "voice": "voice"
        }
        
        # Collection mapping for document collections
        collection_map = {
            "email": "emailmessages",
            "chat": "chat-chunks",
            "ticket": "tickets",
            "socialmedia": "socialmedia",
            "voice": "voice"
        }
        
        data_field_value = data_type_map[data_type]
        document_collection_name = collection_map[data_type]
        
        # Build base query for clusters collection
        base_query = {"data": data_field_value}
        
        # Add domain filter if needed
        if domain:
            base_query["domains"] = {"$in": [domain]}
        
        # Add channel filter for socialmedia data type
        if data_type == "socialmedia" and channel:
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
            
            # Filter clusters that have documents for the specific channel
            # Check if the specific channel exists in socialmedia_ids and has documents (non-empty array)
            base_query[f"socialmedia_ids.{mapped_channel}"] = {"$exists": True, "$ne": []}
            
            logger.info(f"Filtering socialmedia clusters by channel: {mapped_channel}")
            logger.info(f"Channel filter query: {base_query}")

        logger.info(f"Querying clusters collection with query: {base_query}")
        
        # Debug: Show what clusters exist for the specific channel
        if data_type == "socialmedia" and channel:
            channel_clusters_pipeline = [
                {"$match": base_query},
                {"$project": {
                    "cluster_id": 1,
                    "dominant_label": 1,
                    "socialmedia_ids": 1
                }}
            ]
            channel_clusters = list(clusters_collection.aggregate(channel_clusters_pipeline))
            logger.info(f"Found {len(channel_clusters)} clusters for channel '{mapped_channel}'")
            for cluster in channel_clusters[:3]:  # Show first 3 clusters
                logger.info(f"Cluster {cluster.get('cluster_id')}: {cluster.get('dominant_label')} - {mapped_channel} docs: {len(cluster.get('socialmedia_ids', {}).get(mapped_channel, []))}")

        # Get dominant clusters with keyphrases from clusters collection
        dominant_clusters_pipeline = [
            {"$match": base_query},
            {"$match": {
                "cluster_id": {"$ne": None, "$exists": True},  # Fixed: cluster_id not clusterid
                "dominant_label": {"$ne": None, "$exists": True},
                "keyphrases": {"$exists": True},
                "keyphrase_count": {"$exists": True}
            }},
            {"$project": {
                "_id": 0,
                "kmeans_cluster_id": "$cluster_id",  # Fixed: cluster_id not clusterid
                "dominant_cluster_label": "$dominant_label",
                "cluster_name": "$cluster_name",
                "keyphrases": "$keyphrases",
                "keyphrase_count": "$keyphrase_count"
            }},
            {"$sort": {"kmeans_cluster_id": 1}}
        ]
        
        dominant_clusters = list(clusters_collection.aggregate(dominant_clusters_pipeline))
        logger.info(f"Found {len(dominant_clusters)} dominant clusters from clusters collection")

        # Get subclusters with keyphrases from clusters collection
        subclusters_pipeline = [
            {"$match": base_query},
            {"$match": {
                "cluster_id": {"$ne": None, "$exists": True},  # Fixed: cluster_id not clusterid
                "subclusters": {"$exists": True, "$ne": {}}
            }},
            {"$project": {
                "kmeans_cluster_id": "$cluster_id",  # Fixed: cluster_id not clusterid
                "dominant_cluster_label": "$dominant_label",
                "subclusters": {"$objectToArray": "$subclusters"}
            }},
            {"$unwind": "$subclusters"},
            {"$project": {
                "_id": 0,
                "kmeans_cluster_id": "$kmeans_cluster_id",
                "dominant_cluster_label": "$dominant_cluster_label",
                "subcluster_id": "$subclusters.k",  # subcluster key as ID
                "subcluster_label": "$subclusters.v.label",
                "keyphrases": "$subclusters.v.keyphrases",
                "keyphrase_count": {"$size": {"$ifNull": ["$subclusters.v.keyphrases", []]}}
            }},
            {"$match": {
                "subcluster_label": {"$ne": None, "$exists": True}
            }},
            {"$sort": {"kmeans_cluster_id": 1, "subcluster_id": 1}}
        ]
        
        subclusters = list(clusters_collection.aggregate(subclusters_pipeline))
        logger.info(f"Found {len(subclusters)} subclusters from clusters collection")

        # Get the actual document collection to count documents
        try:
            document_collection = get_collection(db, data_type)
            logger.info(f"Accessing document collection: {data_type}")
        except Exception as e:
            logger.error(f"Error accessing document collection: {e}")
            raise HTTPException(status_code=500, detail="Cannot access document collection")

        # Get base query for domain filtering (handles cases where domain might not exist)
        base_query = get_base_query(document_collection, domain)
        logger.info(f"Using base query for document counting: {base_query}")
        logger.info(f"Data type: {data_type}, Domain: {domain}")
        
        # Debug: Check total documents in collection for this data type
        total_docs_in_collection = document_collection.count_documents({})
        logger.info(f"Total documents in {data_type} collection: {total_docs_in_collection}")
        
        # Debug: Check documents with base query
        docs_with_base_query = document_collection.count_documents(base_query)
        logger.info(f"Documents matching base query: {docs_with_base_query}")

        # Filter keyphrases by channel for socialmedia data type
        if data_type == "socialmedia" and channel:
            logger.info(f"Filtering keyphrases by channel: {mapped_channel}")
            
            # Get all cluster IDs for efficient querying
            cluster_ids = [cluster["kmeans_cluster_id"] for cluster in dominant_clusters]
            
            # Get actual keyphrases from socialmedia collection for this channel
            keyphrase_pipeline = [
                {"$match": {
                    "domain": domain,
                    "kmeans_cluster_id": {"$in": cluster_ids},
                    "channel": mapped_channel
                }},
                {"$group": {
                    "_id": "$kmeans_cluster_id",
                    "actual_keyphrases": {"$addToSet": "$dominant_topic"}
                }}
            ]
            
            keyphrase_results = list(document_collection.aggregate(keyphrase_pipeline))
            keyphrase_dict = {result["_id"]: result["actual_keyphrases"] for result in keyphrase_results}
            
            logger.info(f"Found keyphrases for {len(keyphrase_dict)} clusters in channel {mapped_channel}")
            
            # Update dominant clusters with filtered keyphrases
            for cluster in dominant_clusters:
                cluster_id = cluster["kmeans_cluster_id"]
                if cluster_id in keyphrase_dict:
                    # Filter original keyphrases to only include those that exist in socialmedia collection
                    original_keyphrases = cluster.get("keyphrases", [])
                    actual_keyphrases = keyphrase_dict[cluster_id]
                    
                    # Find intersection of original keyphrases and actual keyphrases
                    filtered_keyphrases = [kp for kp in original_keyphrases if kp in actual_keyphrases]
                    
                    cluster["keyphrases"] = filtered_keyphrases
                    cluster["keyphrase_count"] = len(filtered_keyphrases)
                    
                    logger.info(f"Cluster {cluster_id}: {len(original_keyphrases)} -> {len(filtered_keyphrases)} keyphrases")
                else:
                    # No documents found for this cluster in this channel
                    cluster["keyphrases"] = []
                    cluster["keyphrase_count"] = 0
                    logger.info(f"Cluster {cluster_id}: No documents found in channel {mapped_channel}")
            
            # Filter subclusters by channel for socialmedia data type
            if subclusters:
                logger.info(f"Filtering subclusters by channel: {mapped_channel}")
                
                # Get all subcluster pairs for efficient querying
                subcluster_pairs = [(sub["kmeans_cluster_id"], sub["subcluster_id"]) for sub in subclusters]
                
                # Get actual subclusters that exist in socialmedia collection for this channel
                subcluster_existence_pipeline = [
                    {"$match": {
                        "domain": domain,
                        "kmeans_cluster_id": {"$in": [pair[0] for pair in subcluster_pairs]},
                        "subcluster_id": {"$in": [pair[1] for pair in subcluster_pairs]},
                        "channel": mapped_channel
                    }},
                    {"$group": {
                        "_id": {
                            "kmeans_cluster_id": "$kmeans_cluster_id",
                            "subcluster_id": "$subcluster_id"
                        },
                        "actual_keyphrases": {"$addToSet": "$dominant_topic"}
                    }}
                ]
                
                subcluster_existence_results = list(document_collection.aggregate(subcluster_existence_pipeline))
                subcluster_existence_dict = {
                    (result["_id"]["kmeans_cluster_id"], result["_id"]["subcluster_id"]): result["actual_keyphrases"]
                    for result in subcluster_existence_results
                }
                
                logger.info(f"Found {len(subcluster_existence_dict)} subclusters that exist in channel {mapped_channel}")
                
                # Filter subclusters to only include those that have documents in this channel
                filtered_subclusters = []
                for subcluster in subclusters:
                    cluster_id = subcluster["kmeans_cluster_id"]
                    subcluster_id = subcluster["subcluster_id"]
                    key = (cluster_id, subcluster_id)
                    
                    if key in subcluster_existence_dict:
                        # Filter original keyphrases to only include those that exist in socialmedia collection
                        original_keyphrases = subcluster.get("keyphrases", [])
                        actual_keyphrases = subcluster_existence_dict[key]
                        
                        # Find intersection of original keyphrases and actual keyphrases
                        filtered_keyphrases = [kp for kp in original_keyphrases if kp in actual_keyphrases]
                        
                        subcluster["keyphrases"] = filtered_keyphrases
                        subcluster["keyphrase_count"] = len(filtered_keyphrases)
                        
                        filtered_subclusters.append(subcluster)
                        logger.info(f"Subcluster {cluster_id}-{subcluster_id}: {len(original_keyphrases)} -> {len(filtered_keyphrases)} keyphrases")
                    else:
                        logger.info(f"Subcluster {cluster_id}-{subcluster_id}: No documents found in channel {mapped_channel}, excluding from results")
                
                # Update subclusters list to only include filtered ones
                subclusters = filtered_subclusters
                logger.info(f"Filtered subclusters from {len(subcluster_pairs)} to {len(subclusters)} for channel {mapped_channel}")

        # Get all cluster IDs for efficient counting
        cluster_ids = [cluster["kmeans_cluster_id"] for cluster in dominant_clusters]
        subcluster_pairs = [(sub["kmeans_cluster_id"], sub["subcluster_id"]) for sub in subclusters]
        
        # Optimized: Single aggregation to get all dominant cluster counts with urgent counts
        logger.info("Starting optimized dominant cluster counting...")
        
        # Create document query that includes channel filter for socialmedia
        document_query = {
            "domain": domain,
            "kmeans_cluster_id": {"$in": cluster_ids}
        }
        
        # Add channel filter for socialmedia data type
        if data_type == "socialmedia" and channel:
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
            document_query["channel"] = mapped_channel
            logger.info(f"Adding channel filter to document counting: {mapped_channel}")
        
        dominant_counts_pipeline = [
            {"$match": document_query},
            {"$project": {
                "_id": 1,
                "kmeans_cluster_id": 1,
                "urgency": {"$ifNull": ["$urgency", False]}
            }},
            {"$group": {
                "_id": "$kmeans_cluster_id",
                "count": {"$sum": 1},
                "urgent_count": {"$sum": {"$cond": ["$urgency", 1, 0]}}
            }}
        ]
        
        dominant_counts_result = list(document_collection.aggregate(dominant_counts_pipeline))
        logger.info(f"Dominant cluster counting result: {dominant_counts_result}")
        
        dominant_counts_dict = {}
        for item in dominant_counts_result:
            cluster_id = item["_id"]
            total_count = item["count"]
            urgent_count = item["urgent_count"]
            urgent_percentage = (urgent_count / total_count * 100) if total_count > 0 else 0
            dominant_counts_dict[cluster_id] = {
                "count": total_count,
                "urgent_count": urgent_count,
                "urgent_percentage": round(urgent_percentage, 2)
            }
        
        logger.info(f"Completed dominant cluster counting for {len(dominant_counts_dict)} clusters")
        logger.info(f"Dominant counts dict: {dominant_counts_dict}")
        
        # Optimized: Single aggregation to get all subcluster counts with urgent counts
        if subclusters:  # Only do subcluster counting if there are subclusters
            logger.info("Starting optimized subcluster counting...")
            
            # Create subcluster document query that includes channel filter for socialmedia
            subcluster_document_query = {
                "domain": domain,
                "kmeans_cluster_id": {"$in": [pair[0] for pair in subcluster_pairs]},
                "subcluster_id": {"$in": [pair[1] for pair in subcluster_pairs]}
            }
            
            # Add channel filter for socialmedia data type
            if data_type == "socialmedia" and channel:
                # Use the same channel mapping as above
                channel_normalized = channel.lower().strip()
                channel_mapping = {
                    "app store": "App Store/Google Play",
                    "google play": "App Store/Google Play",
                    "app store/google play": "App Store/Google Play",
                    "twitter": "Twitter",
                    "reddit": "Reddit",
                    "trustpilot": "Trustpilot"
                }
                mapped_channel = channel_mapping.get(channel_normalized, channel)
                subcluster_document_query["channel"] = mapped_channel
                logger.info(f"Adding channel filter to subcluster counting: {mapped_channel}")
            
            subcluster_counts_pipeline = [
                 {"$match": subcluster_document_query},
                 {"$project": {
                     "_id": 1,
                     "kmeans_cluster_id": 1,
                     "subcluster_id": 1,
                     "urgency": {"$ifNull": ["$urgency", False]}
                 }},
                 {"$group": {
                     "_id": {"cluster_id": "$kmeans_cluster_id", "subcluster_id": "$subcluster_id"},
                     "count": {"$sum": 1},
                     "urgent_count": {"$sum": {"$cond": ["$urgency", 1, 0]}}
                 }}
             ]
            
            subcluster_counts_result = list(document_collection.aggregate(subcluster_counts_pipeline))
            subcluster_counts_dict = {}
            for item in subcluster_counts_result:
                cluster_id = item["_id"]["cluster_id"]
                subcluster_id = item["_id"]["subcluster_id"]
                total_count = item["count"]
                urgent_count = item["urgent_count"]
                urgent_percentage = (urgent_count / total_count * 100) if total_count > 0 else 0
                subcluster_counts_dict[(cluster_id, subcluster_id)] = {
                    "count": total_count,
                    "urgent_count": urgent_count,
                    "urgent_percentage": round(urgent_percentage, 2)
                }
            logger.info(f"Completed subcluster counting for {len(subcluster_counts_dict)} subclusters")
        else:
            subcluster_counts_dict = {}
        
        # Add counts to dominant clusters (optimized logging)
        logger.info("Adding counts to dominant clusters...")
        for cluster in dominant_clusters:
            cluster_id = cluster["kmeans_cluster_id"]
            count_data = dominant_counts_dict.get(cluster_id, {"count": 0, "urgent_count": 0, "urgent_percentage": 0})
            cluster["document_count"] = count_data["count"]
            cluster["urgent_count"] = count_data["urgent_count"]
            cluster["urgent_percentage"] = count_data["urgent_percentage"]
        
        # Add counts to subclusters (optimized logging)
        if subclusters:
            logger.info("Adding counts to subclusters...")
            for subcluster in subclusters:
                cluster_id = subcluster["kmeans_cluster_id"]
                subcluster_id = subcluster["subcluster_id"]
                count_data = subcluster_counts_dict.get((cluster_id, subcluster_id), {"count": 0, "urgent_count": 0, "urgent_percentage": 0})
                subcluster["document_count"] = count_data["count"]
                subcluster["urgent_count"] = count_data["urgent_count"]
                subcluster["urgent_percentage"] = count_data["urgent_percentage"]
        
        logger.info(f"Completed processing: {len(dominant_clusters)} dominant clusters, {len(subclusters)} subclusters")

        # Build response - only include channel for socialmedia
        response_data = {
            "status": "success",
            "data_type": data_type,
            "domain": domain,
            "dominant_clusters": dominant_clusters,
            "subclusters": subclusters
        }
        
        # Only add channel field for socialmedia data type
        if data_type == "socialmedia":
            response_data["channel"] = channel
        
        return ClusterOptionsResponse(**response_data)

    except Exception as e:
        logger.error(f"Error retrieving cluster options for {data_type}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving cluster options: {str(e)}"
        )

@router.get("/topic-analysis/documents")
async def get_topic_analysis_documents(
    data_type: Literal["email", "chat", "ticket", "socialmedia", "voice"] = Query(..., description="Type of data (email, chat, ticket, socialmedia, voice)"),
    domain: Literal["banking"] = Query("banking", description="Domain filter"),
    kmeans_cluster_id: int = Query(..., description="Mandatory dominant cluster ID"),
    subcluster_id: Optional[str] = Query(None, description="Optional subcluster IDs (comma-separated, e.g., 'sub1,sub2,sub3')"),
    page: int = Query(1, ge=1, description="Page number (starts from 1) - used to calculate skip = (page-1) * page_size"),
    page_size: int = Query(10, ge=1, le=100, description="Number of records per page (max 100) - used as MongoDB limit"),
    channel: Optional[str] = Query(None, description="Optional channel filter for socialmedia (twitter, reddit, trustpilot, app store/google play)"),
    db: database.Database = Depends(get_database),
    current_user: dict = Depends(get_current_user)
) -> TopicAnalysisResponse:
    """
    Get filtered documents for topic analysis with pagination using MongoDB skip/limit
    
    This endpoint implements pagination using MongoDB's skip and limit operations:
    - skip = (page - 1) * page_size
    - limit = page_size
    
    For example:
    - Page 1, page_size 10: skip=0, limit=10 (records 1-10)
    - Page 2, page_size 10: skip=10, limit=10 (records 11-20)
    - Page 3, page_size 10: skip=20, limit=10 (records 21-30)
    
    Args:
        data_type: Type of data (email, chat, ticket, socialmedia, voice)
        domain: Domain filter (banking)
        kmeans_cluster_id: Mandatory dominant cluster ID
        subcluster_id: Optional subcluster IDs (comma-separated)
        page: Page number (starts from 1) - internally converted to skip
        page_size: Number of records per page (max 100) - used as MongoDB limit
        channel: Optional channel filter for socialmedia (twitter, reddit, trustpilot, app store/google play)
    
    Returns:
        Paginated documents matching the filter criteria with pagination metadata
    """
    try:
        collection = get_collection(db, data_type)
        base_query = get_base_query(collection, domain)

        # Parse comma-separated subcluster_id values
        subcluster_ids = []
        if subcluster_id and subcluster_id.strip():
            subcluster_ids = [id.strip() for id in subcluster_id.split(',') if id.strip()]

        # Build query based on filters - single kmeans_cluster_id
        doc_query = {**base_query, "kmeans_cluster_id": kmeans_cluster_id}
        
        # Add optional subcluster filter
        if subcluster_ids:
            if len(subcluster_ids) == 1:
                doc_query["subcluster_id"] = subcluster_ids[0]
            else:
                doc_query["subcluster_id"] = {"$in": subcluster_ids}
            
            # Debug: Check subcluster filtering
            subcluster_count = collection.count_documents(doc_query)
            logger.info(f"Documents with subcluster filter: {subcluster_count}")
            logger.info(f"Subcluster filter applied: {doc_query.get('subcluster_id')}")
        
        # Add channel filter for socialmedia data type
        if data_type == "socialmedia" and channel:
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
            doc_query["channel"] = mapped_channel
            logger.info(f"Filtering socialmedia documents by channel: {mapped_channel}")
            
            # Debug: Check channel filtering
            channel_filter_count = collection.count_documents(doc_query)
            logger.info(f"Documents with channel filter: {channel_filter_count}")
            logger.info(f"Channel filter applied: {doc_query.get('channel')}")
        
        logger.info(f"Document query filters: {doc_query}")
        logger.info(f"Collection name: {collection.name}")
        logger.info(f"Base query: {base_query}")
        
        # Debug: Check if any documents exist with just the base query
        base_count = collection.count_documents(base_query)
        logger.info(f"Documents with base query only: {base_count}")
        
        # Debug: Check if any documents exist with kmeans_cluster_id filter
        cluster_count = collection.count_documents({"kmeans_cluster_id": kmeans_cluster_id})
        logger.info(f"Documents with kmeans_cluster_id {kmeans_cluster_id}: {cluster_count}")
        
        # Debug: Check domain filter
        domain_count = collection.count_documents({"domain": domain})
        logger.info(f"Documents with domain '{domain}': {domain_count}")
        
        # Debug: Check total documents in collection
        total_collection_count = collection.count_documents({})
        logger.info(f"Total documents in collection: {total_collection_count}")
        
        # Debug: Check what kmeans_cluster_id values exist
        pipeline = [
            {"$group": {"_id": "$kmeans_cluster_id", "count": {"$sum": 1}}},
            {"$sort": {"_id": 1}}
        ]
        cluster_ids_result = list(collection.aggregate(pipeline))
        logger.info(f"Available kmeans_cluster_id values: {cluster_ids_result}")
        
        # Additional debugging for socialmedia
        if data_type == "socialmedia":
            # Check what channel values exist
            channel_pipeline = [
                {"$group": {"_id": "$channel", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            channel_result = list(collection.aggregate(channel_pipeline))
            logger.info(f"Available channel values: {channel_result}")
            
            # Check what subcluster_id values exist
            subcluster_pipeline = [
                {"$group": {"_id": "$subcluster_id", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}}
            ]
            subcluster_result = list(collection.aggregate(subcluster_pipeline))
            logger.info(f"Available subcluster_id values: {subcluster_result}")
            
            # Check if there are any documents with the specific channel
            if channel:
                channel_count = collection.count_documents({"channel": channel})
                logger.info(f"Documents with channel '{channel}': {channel_count}")
            
            # Check if there are any documents with the specific kmeans_cluster_id and channel
            if channel:
                cluster_channel_count = collection.count_documents({
                    "kmeans_cluster_id": kmeans_cluster_id,
                    "channel": channel
                })
                logger.info(f"Documents with kmeans_cluster_id {kmeans_cluster_id} and channel '{channel}': {cluster_channel_count}")
                
                # Check what subcluster_id values exist for this combination
                subcluster_pipeline = [
                    {"$match": {"kmeans_cluster_id": kmeans_cluster_id, "channel": channel}},
                    {"$group": {"_id": "$subcluster_id", "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                subcluster_result = list(collection.aggregate(subcluster_pipeline))
                logger.info(f"Available subcluster_id values for kmeans_cluster_id {kmeans_cluster_id} and channel '{channel}': {subcluster_result}")
            
            # Sample a few documents to see the structure
            sample_docs = list(collection.find({}).limit(3))
            logger.info(f"Sample socialmedia documents: {sample_docs}")

        # Get total count for pagination
        total_documents = collection.count_documents(doc_query)
        logger.info(f"Total documents matching filters: {total_documents}")
        
        # Additional debugging: Show what documents exist with the final query
        if total_documents == 0 and data_type == "socialmedia":
            logger.info("No documents found with final query. Let's check what exists...")
            
            # Check without subcluster filter
            query_without_subcluster = {k: v for k, v in doc_query.items() if k != 'subcluster_id'}
            count_without_subcluster = collection.count_documents(query_without_subcluster)
            logger.info(f"Documents without subcluster filter: {count_without_subcluster}")
            logger.info(f"Query without subcluster: {query_without_subcluster}")
            
            # Check without channel filter
            query_without_channel = {k: v for k, v in doc_query.items() if k != 'channel'}
            count_without_channel = collection.count_documents(query_without_channel)
            logger.info(f"Documents without channel filter: {count_without_channel}")
            logger.info(f"Query without channel: {query_without_channel}")
            
            # Show sample documents that match kmeans_cluster_id and channel
            if channel:
                sample_docs = list(collection.find({
                    "kmeans_cluster_id": kmeans_cluster_id,
                    "channel": channel
                }).limit(3))
                logger.info(f"Sample documents with kmeans_cluster_id {kmeans_cluster_id} and channel '{channel}': {sample_docs}")
                
                # Show all available combinations for this kmeans_cluster_id and channel
                combo_pipeline = [
                    {"$match": {"kmeans_cluster_id": kmeans_cluster_id, "channel": channel}},
                    {"$group": {"_id": {"subcluster_id": "$subcluster_id"}, "count": {"$sum": 1}}},
                    {"$sort": {"count": -1}}
                ]
                combo_result = list(collection.aggregate(combo_pipeline))
                logger.info(f"Available combinations for kmeans_cluster_id {kmeans_cluster_id} and channel '{channel}': {combo_result}")
        
        if total_documents == 0:
            # Build response - only include channel for socialmedia
            response_data = {
                "status": "success",
                "data_type": data_type,
                "domain": domain,
                "filters": {
                    "kmeans_cluster_id": kmeans_cluster_id,
                    "subcluster_id": subcluster_ids,
                    "domain": domain
                },
                "pagination": {
                    "current_page": page,
                    "page_size": page_size,
                    "total_documents": 0,
                    "total_pages": 0,
                    "filtered_count": 0,
                    "has_next": False,
                    "has_previous": False,
                    "page_document_count": 0
                },
                "documents": []
            }
            
            # Only add channel field for socialmedia data type
            if data_type == "socialmedia":
                response_data["channel"] = channel
            
            return TopicAnalysisResponse(**response_data)

        # Calculate pagination with proper skip logic
        total_pages = (total_documents + page_size - 1) // page_size
        skip = (page - 1) * page_size
        
        # Validate page number
        if page > total_pages and total_pages > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Page {page} exceeds total pages {total_pages}"
            )
        
        # Ensure skip doesn't exceed total documents
        if skip >= total_documents and total_documents > 0:
            raise HTTPException(
                status_code=400,
                detail=f"Skip value {skip} exceeds total documents {total_documents}"
            )

        # Create indexes for better performance (if not exists)
        try:
            collection.create_index([
                ("domain", 1),
                ("kmeans_cluster_id", 1),
                ("subcluster_id", 1)
            ], background=True)
            collection.create_index([("processed_at", -1)], background=True)
        except Exception as index_error:
            logger.warning(f"Index creation warning: {index_error}")

        # Exclude embeddings and any other large fields we don't need
        projection = {
            "embeddings": 0,  # Exclude embeddings
            "text_embeddings": 0,  # Exclude text embeddings if exists
            "vector_embeddings": 0,  # Exclude vector embeddings if exists
            "embedding": 0,  # Exclude embedding if exists
            "embedding_vector": 0,  # Exclude embedding_vector if exists
            "text_embedding": 0,  # Exclude text_embedding if exists
            "vector_embedding": 0,  # Exclude vector_embedding if exists
            "embeddings_array": 0,  # Exclude embeddings_array if exists
            "embedding_array": 0,  # Exclude embedding_array if exists
            "text_embeddings_array": 0,  # Exclude text_embeddings_array if exists
            "vector_embeddings_array": 0  # Exclude vector_embeddings_array if exists
        }
        
        # For chat data, ensure we include chat-specific fields
        if data_type == "chat":
            # Don't exclude any chat-specific fields, let them be included
            logger.info("Including all chat-specific fields in projection")

        # Retrieve documents with pagination, excluding all embedding fields
        logger.info(f"Executing query: {doc_query}")
        logger.info(f"Using projection: {projection}")
        logger.info(f"Pagination: skip={skip}, limit={page_size}, page={page}")
        logger.info(f"MongoDB skip/limit: skip={skip}, limit={page_size}")
        
        # Use skip and limit for proper pagination
        cursor = collection.find(doc_query, projection).sort("processed_at", -1).skip(skip).limit(page_size)
        documents = list(cursor)
        
        # Log pagination details
        logger.info(f"Retrieved {len(documents)} documents for page {page} (skip={skip}, limit={page_size})")
        logger.info(f"Total documents: {total_documents}, Total pages: {total_pages}")
        logger.info(f"Has next: {page < total_pages}, Has previous: {page > 1}")
        logger.info(f"Skip/limit calculation: page={page}, page_size={page_size}, skip=(page-1)*page_size={skip}, limit={page_size}")
        
        logger.info(f"Retrieved {len(documents)} documents for page {page}")
        
        # Log sample document if available
        if documents:
            sample_doc = documents[0]
            logger.info(f"Sample document keys: {list(sample_doc.keys())}")
            logger.info(f"Sample document _id: {sample_doc.get('_id')}")
            logger.info(f"Sample document domain: {sample_doc.get('domain')}")
            logger.info(f"Sample document kmeans_cluster_id: {sample_doc.get('kmeans_cluster_id')}")
            
            # Log chat-specific fields if present
            if data_type == "chat":
                logger.info(f"Chat ID: {sample_doc.get('chat_id')}")
                logger.info(f"Chat members: {sample_doc.get('chat_members')}")
                logger.info(f"Raw segments: {len(sample_doc.get('raw_segments', []))} segments")
                logger.info(f"Cleaned segments: {len(sample_doc.get('cleaned_segments', []))} segments")
                logger.info(f"Total messages: {sample_doc.get('total_messages')}")
                logger.info(f"Created at: {sample_doc.get('created_at')}")
        else:
            logger.warning("No documents retrieved from database")

        # Convert MongoDB documents to Pydantic models based on data type
        formatted_documents = []
        logger.info(f"Retrieved {len(documents)} raw documents, starting formatting...")
        
        # For debugging: return raw documents if formatting fails
        if not documents:
            logger.warning("No documents retrieved from database")
            # Build response - only include channel for socialmedia
            response_data = {
                "status": "success",
                "data_type": data_type,
                "domain": domain,
                "filters": {
                    "kmeans_cluster_id": kmeans_cluster_id,
                    "subcluster_id": subcluster_ids,
                    "domain": domain
                },
                "pagination": {
                    "current_page": page,
                    "page_size": page_size,
                    "total_documents": total_documents,
                    "total_pages": total_pages,
                    "filtered_count": total_documents,
                    "has_next": page < total_pages,
                    "has_previous": page > 1,
                    "page_document_count": len(documents)
                },
                "documents": []
            }
            
            # Only add channel field for socialmedia data type
            if data_type == "socialmedia":
                response_data["channel"] = channel
            
            return TopicAnalysisResponse(**response_data)
        
        # Select the appropriate response model based on data type
        if data_type == "email":
            ResponseModel = EmailDocumentResponse
        elif data_type == "chat":
            ResponseModel = ChatDocumentResponse
            logger.info("Using ChatDocumentResponse model for chat data")
        elif data_type == "ticket":
            ResponseModel = TicketDocumentResponse
        elif data_type == "twitter":
            ResponseModel = TwitterDocumentResponse
            logger.info("Using TwitterDocumentResponse model for twitter data")
        elif data_type == "socialmedia":
            ResponseModel = SocialMediaDocumentResponse
            logger.info("Using SocialMediaDocumentResponse model for socialmedia data")
        elif data_type == "voice":
            ResponseModel = VoiceDocumentResponse
            logger.info("Using VoiceDocumentResponse model for voice data")
        else:
            ResponseModel = BaseDocumentResponse
        
        for i, doc in enumerate(documents):
            try:
                # Convert ObjectId to string
                doc["_id"] = str(doc["_id"])
                
                # Convert datetime objects to ISO string format for created_at and created
                for date_field in ["created_at", "created"]:
                    if doc.get(date_field):
                        if isinstance(doc[date_field], datetime):
                            doc[date_field] = doc[date_field].isoformat()
                        elif isinstance(doc[date_field], str):
                            # If it's already a string, try to parse and format it
                            try:
                                parsed_dt = datetime.fromisoformat(doc[date_field].replace("Z", "+00:00"))
                                doc[date_field] = parsed_dt.isoformat()
                            except ValueError:
                                # If parsing fails, keep as is
                                pass
                
                # For chat data, ensure all chat-specific fields are present
                if data_type == "chat":
                    # Map alternative field names to expected field names
                    if "chatId" in doc and "chat_id" not in doc:
                        doc["chat_id"] = doc["chatId"]
                    if "chatMembers" in doc and "chat_members" not in doc:
                        doc["chat_members"] = doc["chatMembers"]
                    if "rawSegments" in doc and "raw_segments" not in doc:
                        doc["raw_segments"] = doc["rawSegments"]
                    if "cleanedSegments" in doc and "cleaned_segments" not in doc:
                        doc["cleaned_segments"] = doc["cleanedSegments"]
                    if "totalMessages" in doc and "total_messages" not in doc:
                        doc["total_messages"] = doc["totalMessages"]
                    if "createdAt" in doc and "created_at" not in doc:
                        doc["created_at"] = doc["createdAt"]
                    
                    # Ensure chat-specific fields exist with default values if missing
                    if "chat_id" not in doc:
                        doc["chat_id"] = None
                    if "chat_members" not in doc:
                        doc["chat_members"] = []
                    if "raw_segments" not in doc:
                        doc["raw_segments"] = []
                    if "cleaned_segments" not in doc:
                        doc["cleaned_segments"] = []
                    if "total_messages" not in doc:
                        doc["total_messages"] = None
                    if "created_at" not in doc:
                        doc["created_at"] = None
                
                # For email data, ensure all email-specific fields are present
                elif data_type == "email":
                    # Map alternative field names to expected field names
                    if "messageId" in doc and "message_id" not in doc:
                        doc["message_id"] = doc["messageId"]
                    if "conversationId" in doc and "conversation_id" not in doc:
                        doc["conversation_id"] = doc["conversationId"]
                    if "senderId" in doc and "sender_id" not in doc:
                        doc["sender_id"] = doc["senderId"]
                    if "senderName" in doc and "sender_name" not in doc:
                        doc["sender_name"] = doc["senderName"]
                    if "receiverIds" in doc and "receiver_ids" not in doc:
                        doc["receiver_ids"] = doc["receiverIds"]
                    if "receiverNames" in doc and "receiver_names" not in doc:
                        doc["receiver_names"] = doc["receiverNames"]
                    if "messageText" in doc and "message_text" not in doc:
                        doc["message_text"] = doc["messageText"]
                    if "timeTaken" in doc and "time_taken" not in doc:
                        doc["time_taken"] = doc["timeTaken"]
                    
                    # Ensure email-specific fields exist with default values if missing
                    if "message_id" not in doc:
                        doc["message_id"] = None
                    if "conversation_id" not in doc:
                        doc["conversation_id"] = None
                    if "sender_id" not in doc:
                        doc["sender_id"] = None
                    if "sender_name" not in doc:
                        doc["sender_name"] = None
                    if "receiver_ids" not in doc:
                        doc["receiver_ids"] = []
                    if "receiver_names" not in doc:
                        doc["receiver_names"] = []
                    if "timestamp" not in doc:
                        doc["timestamp"] = None
                    if "subject" not in doc:
                        doc["subject"] = None
                    if "message_text" not in doc:
                        doc["message_text"] = None
                    if "time_taken" not in doc:
                        doc["time_taken"] = None
                
                # For ticket data, ensure all ticket-specific fields are present
                elif data_type == "ticket":
                    # Map alternative field names to expected field names
                    if "ticketNumber" in doc and "ticket_number" not in doc:
                        doc["ticket_number"] = doc["ticketNumber"]
                    if "ticketId" in doc and "ticket_id" not in doc:
                        doc["ticket_id"] = doc["ticketId"]
                    if "ticketStatus" in doc and "ticket_status" not in doc:
                        doc["ticket_status"] = doc["ticketStatus"]
                    if "ticketPriority" in doc and "ticket_priority" not in doc:
                        doc["ticket_priority"] = doc["ticketPriority"]
                    if "ticketCategory" in doc and "ticket_category" not in doc:
                        doc["ticket_category"] = doc["ticketCategory"]
                    if "ticketAssignee" in doc and "ticket_assignee" not in doc:
                        doc["ticket_assignee"] = doc["ticketAssignee"]
                    if "ticketCreatedAt" in doc and "ticket_created_at" not in doc:
                        doc["ticket_created_at"] = doc["ticketCreatedAt"]
                    if "ticketUpdatedAt" in doc and "ticket_updated_at" not in doc:
                        doc["ticket_updated_at"] = doc["ticketUpdatedAt"]
                    
                    # Ensure ticket-specific fields exist with default values if missing
                    if "ticket_number" not in doc:
                        doc["ticket_number"] = None
                    if "title" not in doc:
                        doc["title"] = None
                    if "description" not in doc:
                        doc["description"] = None
                    if "priority" not in doc:
                        doc["priority"] = None
                    if "created" not in doc:
                        doc["created"] = None
                    if "ticket_id" not in doc:
                        doc["ticket_id"] = None
                    if "ticket_status" not in doc:
                        doc["ticket_status"] = None
                    if "ticket_priority" not in doc:
                        doc["ticket_priority"] = None
                    if "ticket_category" not in doc:
                        doc["ticket_category"] = None
                    if "ticket_assignee" not in doc:
                        doc["ticket_assignee"] = None
                    if "ticket_created_at" not in doc:
                        doc["ticket_created_at"] = None
                    if "ticket_updated_at" not in doc:
                        doc["ticket_updated_at"] = None
                
                # For twitter data, ensure all twitter-specific fields are present
                elif data_type == "twitter":
                    # Map alternative field names to expected field names
                    if "tweetId" in doc and "tweet_id" not in doc:
                        doc["tweet_id"] = doc["tweetId"]
                    if "userId" in doc and "user_id" not in doc:
                        doc["user_id"] = doc["userId"]
                    if "emailId" in doc and "email_id" not in doc:
                        doc["email_id"] = doc["emailId"]
                    if "tweetText" in doc and "text" not in doc:
                        doc["text"] = doc["tweetText"]
                    if "retweetCount" in doc and "retweet_count" not in doc:
                        doc["retweet_count"] = doc["retweetCount"]
                    if "likeCount" in doc and "like_count" not in doc:
                        doc["like_count"] = doc["likeCount"]
                    if "replyCount" in doc and "reply_count" not in doc:
                        doc["reply_count"] = doc["replyCount"]
                    if "quoteCount" in doc and "quote_count" not in doc:
                        doc["quote_count"] = doc["quoteCount"]
                    if "createdAt" in doc and "created_at" not in doc:
                        doc["created_at"] = doc["createdAt"]
                    
                    # Ensure twitter-specific fields exist with default values if missing
                    twitter_fields = [
                        "tweet_id", "user_id", "username", "email_id", "text",
                        "created_at", "retweet_count", "like_count", "reply_count", "quote_count",
                        "hashtags", "priority", "sentiment"
                    ]
                    for field in twitter_fields:
                        if field not in doc:
                            if field in ["hashtags"]:
                                doc[field] = []
                            elif field in ["retweet_count", "like_count", "reply_count", "quote_count"]:
                                doc[field] = 0
                            else:
                                doc[field] = None
                
                # For socialmedia data, ensure all socialmedia-specific fields are present
                elif data_type == "socialmedia":
                    # Map alternative field names to expected field names
                    if "tweetId" in doc and "tweet_id" not in doc:
                        doc["tweet_id"] = doc["tweetId"]
                    if "userId" in doc and "user_id" not in doc:
                        doc["user_id"] = doc["userId"]
                    if "emailId" in doc and "email_id" not in doc:
                        doc["email_id"] = doc["emailId"]
                    if "tweetText" in doc and "text" not in doc:
                        doc["text"] = doc["tweetText"]
                    if "retweetCount" in doc and "retweet_count" not in doc:
                        doc["retweet_count"] = doc["retweetCount"]
                    if "likeCount" in doc and "like_count" not in doc:
                        doc["like_count"] = doc["likeCount"]
                    if "replyCount" in doc and "reply_count" not in doc:
                        doc["reply_count"] = doc["replyCount"]
                    if "quoteCount" in doc and "quote_count" not in doc:
                        doc["quote_count"] = doc["quoteCount"]
                    if "createdAt" in doc and "created_at" not in doc:
                        doc["created_at"] = doc["createdAt"]
                    
                    # Trustpilot specific field mapping
                    if "Date of experience" in doc and "date_of_experience" not in doc:
                        doc["date_of_experience"] = doc["Date of experience"]
                    if "Title" in doc and "review_title" not in doc:
                        doc["review_title"] = doc["Title"]
                    
                    # Ensure socialmedia-specific fields exist with default values if missing
                    socialmedia_fields = [
                        "channel", "username", "email_id", "user_id", "text", "sentiment", 
                        "priority", "urgency", "created_at", "content_generated_at", 
                        "dominant_topic", "subtopics", "dominant_cluster_label", 
                        "kmeans_cluster_id", "subcluster_id", "subcluster_label", 
                        "kmeans_cluster_keyphrase", "tweet_id", "hashtags", "like_count", 
                        "retweet_count", "reply_count", "quote_count", "post_id", "subreddit", 
                        "comment_count", "share_count", "review_id", "rating", "useful_count", 
                        "date_of_experience", "review_title", "platform", "review_helpful"
                    ]
                    for field in socialmedia_fields:
                        if field not in doc:
                            if field in ["hashtags"]:
                                doc[field] = []
                            elif field in ["like_count", "retweet_count", "reply_count", "quote_count", 
                                         "comment_count", "share_count", "useful_count", "review_helpful", "rating"]:
                                doc[field] = 0
                            else:
                                doc[field] = None
                
                # For voice data, ensure all voice-specific fields are present
                elif data_type == "voice":
                    # Map alternative field names to expected field names
                    if "callId" in doc and "call_id" not in doc:
                        doc["call_id"] = doc["callId"]
                    if "customerName" in doc and "customer_name" not in doc:
                        doc["customer_name"] = doc["customerName"]
                    if "customerId" in doc and "customer_id" not in doc:
                        doc["customer_id"] = doc["customerId"]
                    if "callPurpose" in doc and "call_purpose" not in doc:
                        doc["call_purpose"] = doc["callPurpose"]
                    if "resolutionStatus" in doc and "resolution_status" not in doc:
                        doc["resolution_status"] = doc["resolutionStatus"]
                    
                    # Ensure voice-specific fields exist with default values if missing
                    voice_fields = [
                        "call_id", "timestamp", "customer_name", "customer_id", "email",
                        "call_purpose", "conversation", "priority", "resolution_status", "sentiment"
                    ]
                    for field in voice_fields:
                        if field not in doc:
                            if field == "conversation":
                                doc[field] = []
                            else:
                                doc[field] = None
                
                # Log the first document structure for debugging
                if i == 0:
                    logger.info(f"First document structure: {list(doc.keys())}")
                    logger.info(f"Sample document data: {doc}")
                    if data_type == "chat":
                        logger.info(f"Chat-specific fields in document:")
                        logger.info(f"  chat_id: {doc.get('chat_id')}")
                        logger.info(f"  chat_members: {doc.get('chat_members')}")
                        logger.info(f"  raw_segments: {len(doc.get('raw_segments', []))} items")
                        logger.info(f"  cleaned_segments: {len(doc.get('cleaned_segments', []))} items")
                        logger.info(f"  total_messages: {doc.get('total_messages')}")
                        logger.info(f"  created_at: {doc.get('created_at')}")
                        
                        # Check for alternative field names that might exist in the database
                        logger.info(f"Checking for alternative field names:")
                        logger.info(f"  chatId: {doc.get('chatId')}")
                        logger.info(f"  chatMembers: {doc.get('chatMembers')}")
                        logger.info(f"  rawSegments: {doc.get('rawSegments')}")
                        logger.info(f"  cleanedSegments: {doc.get('cleanedSegments')}")
                        logger.info(f"  totalMessages: {doc.get('totalMessages')}")
                        logger.info(f"  createdAt: {doc.get('createdAt')}")
                    if doc.get("chat_members"):
                        logger.info(f"Sample chat_members structure: {doc['chat_members']}")
                
                # Create appropriate DocumentResponse object based on data type
                formatted_doc = ResponseModel(**doc)
                formatted_documents.append(formatted_doc)
                
            except Exception as format_error:
                logger.error(f"Error formatting document {doc.get('_id')}: {format_error}")
                logger.error(f"Document keys: {list(doc.keys()) if doc else 'None'}")
                logger.error(f"Document data: {doc}")
                
                # For debugging: create a minimal document response based on data type
                try:
                    minimal_doc = {
                        "_id": str(doc.get("_id", "")),
                        "domain": doc.get("domain", ""),
                        "kmeans_cluster_id": doc.get("kmeans_cluster_id", 0),
                        "dominant_topic": doc.get("dominant_topic", ""),
                        "processed_at": doc.get("processed_at", "")
                    }
                    
                    # Add data type specific fields
                    if data_type == "email":
                        minimal_doc.update({
                            "message_id": doc.get("message_id", ""),
                            "conversation_id": doc.get("conversation_id", ""),
                            "sender_id": doc.get("sender_id", ""),
                            "sender_name": doc.get("sender_name", ""),
                            "receiver_ids": doc.get("receiver_ids", []),
                            "receiver_names": doc.get("receiver_names", []),
                            "timestamp": doc.get("timestamp", ""),
                            "subject": doc.get("subject", ""),
                            "message_text": doc.get("message_text", ""),
                            "time_taken": doc.get("time_taken", 0.0)
                        })
                    elif data_type == "chat":
                        minimal_doc.update({
                            "chat_id": doc.get("chat_id", ""),
                            "total_messages": doc.get("total_messages", 0),
                            "raw_segments": doc.get("raw_segments", []),
                            "cleaned_segments": doc.get("cleaned_segments", []),
                            "chat_members": doc.get("chat_members", []),
                            "created_at": doc.get("created_at", "")
                        })
                    elif data_type == "ticket":
                        minimal_doc.update({
                            "ticket_number": doc.get("ticket_number", ""),
                            "title": doc.get("title", ""),
                            "description": doc.get("description", ""),
                            "priority": doc.get("priority", ""),
                            "created": doc.get("created", ""),
                            "ticket_id": doc.get("ticket_id", ""),
                            "ticket_status": doc.get("ticket_status", ""),
                            "ticket_priority": doc.get("ticket_priority", ""),
                            "ticket_category": doc.get("ticket_category", ""),
                            "ticket_assignee": doc.get("ticket_assignee", ""),
                            "ticket_created_at": doc.get("ticket_created_at", ""),
                            "ticket_updated_at": doc.get("ticket_updated_at", "")
                        })
                    elif data_type == "twitter":
                        minimal_doc.update({
                            "tweet_id": doc.get("tweet_id", ""),
                            "user_id": doc.get("user_id", ""),
                            "username": doc.get("username", ""),
                            "email_id": doc.get("email_id", ""),
                            "text": doc.get("text", ""),
                            "created_at": doc.get("created_at", ""),
                            "retweet_count": doc.get("retweet_count", 0),
                            "like_count": doc.get("like_count", 0),
                            "reply_count": doc.get("reply_count", 0),
                            "quote_count": doc.get("quote_count", 0),
                            "hashtags": doc.get("hashtags", []),
                            "priority": doc.get("priority", ""),
                            "sentiment": doc.get("sentiment", "")
                        })
                    elif data_type == "socialmedia":
                        minimal_doc.update({
                            "channel": doc.get("channel", ""),
                            "username": doc.get("username", ""),
                            "email_id": doc.get("email_id", ""),
                            "user_id": doc.get("user_id", ""),
                            "text": doc.get("text", ""),
                            "sentiment": doc.get("sentiment", ""),
                            "priority": doc.get("priority", ""),
                            "urgency": doc.get("urgency", False),
                            "created_at": doc.get("created_at", ""),
                            "content_generated_at": doc.get("content_generated_at", ""),
                            "dominant_topic": doc.get("dominant_topic", ""),
                            "subtopics": doc.get("subtopics", ""),
                            "dominant_cluster_label": doc.get("dominant_cluster_label", ""),
                            "kmeans_cluster_id": doc.get("kmeans_cluster_id", 0),
                            "subcluster_id": doc.get("subcluster_id", ""),
                            "subcluster_label": doc.get("subcluster_label", ""),
                            "kmeans_cluster_keyphrase": doc.get("kmeans_cluster_keyphrase", ""),
                            # Twitter fields
                            "tweet_id": doc.get("tweet_id", ""),
                            "hashtags": doc.get("hashtags", []),
                            "like_count": doc.get("like_count", 0),
                            "retweet_count": doc.get("retweet_count", 0),
                            "reply_count": doc.get("reply_count", 0),
                            "quote_count": doc.get("quote_count", 0),
                            # Reddit fields
                            "post_id": doc.get("post_id", ""),
                            "subreddit": doc.get("subreddit", ""),
                            "comment_count": doc.get("comment_count", 0),
                            "share_count": doc.get("share_count", 0),
                            # Trustpilot fields
                            "review_id": doc.get("review_id", ""),
                            "rating": doc.get("rating", 0),
                            "useful_count": doc.get("useful_count", 0),
                            "date_of_experience": doc.get("date_of_experience", ""),
                            "review_title": doc.get("review_title", ""),
                            # App Store/Google Play fields
                            "platform": doc.get("platform", ""),
                            "review_helpful": doc.get("review_helpful", 0)
                        })
                    elif data_type == "voice":
                        minimal_doc.update({
                            "call_id": doc.get("call_id", ""),
                            "timestamp": doc.get("timestamp", ""),
                            "customer_name": doc.get("customer_name", ""),
                            "customer_id": doc.get("customer_id", ""),
                            "email": doc.get("email", ""),
                            "call_purpose": doc.get("call_purpose", ""),
                            "conversation": doc.get("conversation", []),
                            "priority": doc.get("priority", ""),
                            "resolution_status": doc.get("resolution_status", ""),
                            "sentiment": doc.get("sentiment", "")
                        })
                    
                    formatted_doc = ResponseModel(**minimal_doc)
                    formatted_documents.append(formatted_doc)
                    logger.info(f"Created minimal document for {doc.get('_id')}")
                except Exception as minimal_error:
                    logger.error(f"Even minimal document creation failed: {minimal_error}")
                continue
        
        logger.info(f"Successfully formatted {len(formatted_documents)} documents")

        # Build response - only include channel for socialmedia
        response_data = {
            "status": "success",
            "data_type": data_type,
            "domain": domain,
            "filters": {
                "kmeans_cluster_id": kmeans_cluster_id,
                "subcluster_id": subcluster_ids,
                "domain": domain
            },
            "pagination": {
                "current_page": page,
                "page_size": page_size,
                "total_documents": total_documents,
                "total_pages": total_pages,
                "filtered_count": total_documents,
                "has_next": page < total_pages,
                "has_previous": page > 1,
                "page_document_count": len(formatted_documents),
                "skip_used": skip,  # Show the actual skip value used in MongoDB
                "limit_used": page_size  # Show the actual limit value used in MongoDB
            },
            "documents": formatted_documents
        }
        
        # Only add channel field for socialmedia data type
        if data_type == "socialmedia":
            response_data["channel"] = channel
        
        # Create response with detailed pagination info including skip/limit
        response = TopicAnalysisResponse(**response_data)
        
        logger.info(f"Response pagination with skip/limit: {response.pagination}")
        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving topic analysis documents: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving topic analysis documents: {str(e)}"
        )

@router.get("/topic-analysis/search-by-email")
async def search_documents_by_email(
    sender_id: str = Query(..., description="Email address to search for (e.g., 'jamesdavidson5023@gmail.com')"),
    db: database.Database = Depends(get_database),
    current_user: dict = Depends(get_current_user)
) -> EmailSearchResponse:
    """
    Search for documents by email address across email and chat collections
    Searches for the email address in:
    - emailmessages collection: sender_id field
    - chat-chunks collection: chat_members.email field
    
    Args:
        sender_id: Email address to search for (e.g., 'jamesdavidson5023@gmail.com')
        
    Returns:
        EmailSearchResponse containing matching documents from both collections
    """
    try:
        email_address = sender_id.strip().lower()
        logger.info(f"Searching for documents with email: {email_address}")
        
        # Get collections
        email_collection = get_collection(db, "email")
        chat_collection = get_collection(db, "chat")
        
        # Search in emailmessages collection
        email_query = {"sender_id": {"$regex": email_address, "$options": "i"}}
        logger.info(f"Email collection query: {email_query}")
        
        # Exclude embeddings and large fields from email collection
        email_projection = {
            "embeddings": 0,
            "text_embeddings": 0,
            "vector_embeddings": 0,
            "embedding": 0,
            "embedding_vector": 0,
            "text_embedding": 0,
            "vector_embedding": 0,
            "embeddings_array": 0,
            "embedding_array": 0,
            "text_embeddings_array": 0,
            "vector_embeddings_array": 0
        }
        
        email_documents = list(email_collection.find(email_query, email_projection))
        logger.info(f"Found {len(email_documents)} email documents")
        
        # Search in chat-chunks collection
        # Use $elemMatch to search within chat_members array for matching email
        chat_query = {
            "chat_members": {
                "$elemMatch": {
                    "email": {"$regex": email_address, "$options": "i"}
                }
            }
        }
        logger.info(f"Chat collection query: {chat_query}")
        
        # Exclude embeddings and large fields from chat collection
        chat_projection = {
            "embeddings": 0,
            "text_embeddings": 0,
            "vector_embeddings": 0,
            "embedding": 0,
            "embedding_vector": 0,
            "text_embedding": 0,
            "vector_embedding": 0,
            "embeddings_array": 0,
            "embedding_array": 0,
            "text_embeddings_array": 0,
            "vector_embeddings_array": 0
        }
        
        chat_documents = list(chat_collection.find(chat_query, chat_projection))
        logger.info(f"Found {len(chat_documents)} chat documents")
        
        # Format email documents
        formatted_email_documents = []
        for doc in email_documents:
            try:
                # Convert ObjectId to string
                doc["_id"] = str(doc["_id"])
                
                # Convert datetime objects to ISO string format
                for date_field in ["timestamp", "created_at", "created"]:
                    if doc.get(date_field):
                        if isinstance(doc[date_field], datetime):
                            doc[date_field] = doc[date_field].isoformat()
                        elif isinstance(doc[date_field], str):
                            try:
                                parsed_dt = datetime.fromisoformat(doc[date_field].replace("Z", "+00:00"))
                                doc[date_field] = parsed_dt.isoformat()
                            except ValueError:
                                pass
                
                # Map alternative field names for email data
                if "messageId" in doc and "message_id" not in doc:
                    doc["message_id"] = doc["messageId"]
                if "conversationId" in doc and "conversation_id" not in doc:
                    doc["conversation_id"] = doc["conversationId"]
                if "senderId" in doc and "sender_id" not in doc:
                    doc["sender_id"] = doc["senderId"]
                if "senderName" in doc and "sender_name" not in doc:
                    doc["sender_name"] = doc["senderName"]
                if "receiverIds" in doc and "receiver_ids" not in doc:
                    doc["receiver_ids"] = doc["receiverIds"]
                if "receiverNames" in doc and "receiver_names" not in doc:
                    doc["receiver_names"] = doc["receiverNames"]
                if "messageText" in doc and "message_text" not in doc:
                    doc["message_text"] = doc["messageText"]
                if "timeTaken" in doc and "time_taken" not in doc:
                    doc["time_taken"] = doc["timeTaken"]
                
                # Ensure email-specific fields exist with default values if missing
                email_fields = [
                    "message_id", "conversation_id", "sender_id", "sender_name",
                    "receiver_ids", "receiver_names", "timestamp", "subject",
                    "message_text", "time_taken"
                ]
                for field in email_fields:
                    if field not in doc:
                        if field in ["receiver_ids", "receiver_names"]:
                            doc[field] = []
                        else:
                            doc[field] = None
                
                formatted_doc = EmailDocumentResponse(**doc)
                formatted_email_documents.append(formatted_doc)
                
            except Exception as format_error:
                logger.error(f"Error formatting email document {doc.get('_id')}: {format_error}")
                continue
        
        # Format chat documents
        formatted_chat_documents = []
        for doc in chat_documents:
            try:
                # Convert ObjectId to string
                doc["_id"] = str(doc["_id"])
                
                # Convert datetime objects to ISO string format
                for date_field in ["created_at", "created"]:
                    if doc.get(date_field):
                        if isinstance(doc[date_field], datetime):
                            doc[date_field] = doc[date_field].isoformat()
                        elif isinstance(doc[date_field], str):
                            try:
                                parsed_dt = datetime.fromisoformat(doc[date_field].replace("Z", "+00:00"))
                                doc[date_field] = parsed_dt.isoformat()
                            except ValueError:
                                pass
                
                # Map alternative field names for chat data
                if "chatId" in doc and "chat_id" not in doc:
                    doc["chat_id"] = doc["chatId"]
                if "chatMembers" in doc and "chat_members" not in doc:
                    doc["chat_members"] = doc["chatMembers"]
                if "rawSegments" in doc and "raw_segments" not in doc:
                    doc["raw_segments"] = doc["rawSegments"]
                if "cleanedSegments" in doc and "cleaned_segments" not in doc:
                    doc["cleaned_segments"] = doc["cleanedSegments"]
                if "totalMessages" in doc and "total_messages" not in doc:
                    doc["total_messages"] = doc["totalMessages"]
                if "createdAt" in doc and "created_at" not in doc:
                    doc["created_at"] = doc["createdAt"]
                
                # Ensure chat-specific fields exist with default values if missing
                chat_fields = [
                    "chat_id", "chat_members", "raw_segments", "cleaned_segments",
                    "total_messages", "created_at"
                ]
                for field in chat_fields:
                    if field not in doc:
                        if field in ["chat_members", "raw_segments", "cleaned_segments"]:
                            doc[field] = []
                        else:
                            doc[field] = None
                
                formatted_doc = ChatDocumentResponse(**doc)
                formatted_chat_documents.append(formatted_doc)
                
            except Exception as format_error:
                logger.error(f"Error formatting chat document {doc.get('_id')}: {format_error}")
                continue
        
        total_documents = len(formatted_email_documents) + len(formatted_chat_documents)
        logger.info(f"Total formatted documents: {total_documents} ({len(formatted_email_documents)} email, {len(formatted_chat_documents)} chat)")
        
        return EmailSearchResponse(
            status="success",
            email_address=email_address,
            total_documents=total_documents,
            email_documents=formatted_email_documents,
            chat_documents=formatted_chat_documents
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error searching documents by email: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Error searching documents by email: {str(e)}"
        )