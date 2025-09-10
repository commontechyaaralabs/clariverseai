// API utility functions for fetching data from the backend

// Get API base URL from environment variable or use default
const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL || 'https://clariversev1-153115538723.us-central1.run.app';


export interface Statistics {
  data_type: string;
  domain: string;
  total_no_of_emails: number;
  total_urgent_messages: number;
  urgent_percentage: number;
  total_dominant_clusters: number;
  total_subclusters: number;
  last_run_date: string | null;
}

export interface ApiResponse {
  status: string;
  statistics: Statistics;
}

export interface DominantCluster {
  kmeans_cluster_id: number;
  dominant_cluster_label: string;
  cluster_name?: string;
  keyphrases: string[];
  keyphrase_count: number;
  document_count?: number;
  urgent_count?: number;
  urgent_percentage?: number;
}

export interface Subcluster {
  kmeans_cluster_id: number;
  dominant_cluster_label: string;
  subcluster_id: string;
  subcluster_label: string;
  keyphrases: string[];
  keyphrase_count: number;
  document_count?: number;
  urgent_count?: number;
  urgent_percentage?: number;
}

export interface ClusterOptionsResponse {
  status: string;
  data_type: string;
  domain: string;
  dominant_clusters: DominantCluster[];
  subclusters: Subcluster[];
}

export interface DocumentResponse {
  _id: string;
  domain?: string;
  cleaned_text?: string;
  lemmatized_text?: string;
  preprocessed_text?: string;
  dominant_topic?: string;
  model_used?: string;
  processed_at?: string;
  subtopics?: string;
  urgency?: boolean;
  was_summarized?: boolean;
  clustering_method?: string;
  clustering_updated_at?: number;
  kmeans_cluster_id?: number;
  kmeans_cluster_keyphrase?: string;
  dominant_cluster_label?: string;
  subcluster_label?: string;
  subcluster_id?: string;
  
  // Email specific fields
  message_id?: string;
  conversation_id?: string;
  sender_id?: string;
  sender_name?: string;
  receiver_ids?: string[];
  receiver_names?: string[];
  timestamp?: string;
  subject?: string;
  message_text?: string;
  time_taken?: number;
  
  // Chat specific fields
  chat_id?: string;
  chat_members?: unknown[];
  raw_segments?: unknown[];
  cleaned_segments?: unknown[];
  total_messages?: number;
  created_at?: string;
  
  // Ticket specific fields
  ticket_number?: string;
  title?: string;
  description?: string;
  priority?: string;
  created?: string;
  ticket_id?: string;
  ticket_status?: string;
  ticket_priority?: string;
  ticket_category?: string;
  ticket_assignee?: string;
  ticket_created_at?: string;
  ticket_updated_at?: string;
  
  // Twitter specific fields
  tweet_id?: string;
  user_id?: string;
  username?: string;
  email_id?: string;
  text?: string;
  retweet_count?: number;
  like_count?: number;
  reply_count?: number;
  quote_count?: number;
  hashtags?: string[];
  sentiment?: string;
  channel?: string;
  
  // Trustpilot specific fields
  review_id?: string;
  review_title?: string;
  rating?: number;
  useful_count?: number;
  date_of_experience?: string;
  
  // App Store/Google Play specific fields
  platform?: string;
  review_helpful?: number;
  
  // Reddit specific fields
  post_id?: string;
  subreddit?: string;
  comment_count?: number;
  share_count?: number;
  
  // Voice specific fields
  call_id?: string;
  call_timestamp?: string;
  customer_name?: string;
  customer_id?: string;
  email?: string;
  call_purpose?: string;
  conversation?: Array<{speaker: string; text: string}>;
  call_priority?: string;
  resolution_status?: string;
}

export interface TopicAnalysisResponse {
  status: string;
  data_type: string;
  domain: string;
  filters: {
    kmeans_cluster_id: number;
    subcluster_id?: string;
    domain: string;
  };
  pagination: {
    current_page: number;
    page_size: number;
    total_documents: number;
    total_pages: number;
    filtered_count: number;
    has_next: boolean;
    has_previous: boolean;
    page_document_count: number;
  };
  documents: DocumentResponse[];
}

export async function fetchStatistics(dataType: 'email' | 'chat' | 'ticket' | 'socialmedia' | 'voice', domain: string = 'banking', channel?: string): Promise<ApiResponse> {
  try {
    const queryParams = new URLSearchParams({
      data_type: dataType,
      domain: domain
    });
    
    if (channel) {
      queryParams.append('channel', channel);
    }
    
    const response = await fetch(`/api/home/stats?${queryParams.toString()}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error fetching statistics:', error);
    // Return default values if API fails
    return {
      status: 'error',
      statistics: {
        data_type: dataType,
        domain: domain,
        total_no_of_emails: 0,
        total_urgent_messages: 0,
        urgent_percentage: 0,
        total_dominant_clusters: 0,
        total_subclusters: 0,
        last_run_date: null,
      },
    };
  }
}

export async function fetchClusterOptions(dataType: 'email' | 'chat' | 'ticket' | 'socialmedia' | 'voice', domain: string = 'banking', channel?: string): Promise<ClusterOptionsResponse> {
  try {
    const queryParams = new URLSearchParams({
      data_type: dataType,
      domain: domain
    });
    
    if (channel) {
      queryParams.append('channel', channel);
    }
    
    const response = await fetch(`/api/topic-analysis/clusters?${queryParams.toString()}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error fetching cluster options:', error);
    // Return empty data if API fails
    return {
      status: 'error',
      data_type: dataType,
      domain: domain,
      dominant_clusters: [],
      subclusters: [],
    };
  }
}

export async function fetchTopicAnalysisDocuments(
  dataType: 'email' | 'chat' | 'ticket' | 'socialmedia' | 'voice',
  kmeansClusterId: number,
  subclusterId?: string,
  page: number = 1,
  pageSize: number = 20,
  domain: string = 'banking',
  channel?: string,
  platform?: string
): Promise<TopicAnalysisResponse> {
  try {
    const params = new URLSearchParams({
      data_type: dataType,
      domain: domain,
      kmeans_cluster_id: kmeansClusterId.toString(),
      page: page.toString(),
      page_size: pageSize.toString(),
    });

    if (subclusterId) {
      params.append('subcluster_id', subclusterId);
    }
    
    if (channel) {
      params.append('channel', channel);
    }
    
    if (platform) {
      params.append('platform', platform);
    }

    const response = await fetch(`/api/topic-analysis/documents?${params.toString()}`, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
      },
    });

    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }

    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error fetching topic analysis documents:', error);
    // Return empty data if API fails
    return {
      status: 'error',
      data_type: dataType,
      domain: domain,
      filters: {
        kmeans_cluster_id: kmeansClusterId,
        subcluster_id: subclusterId,
        domain: domain,
      },
      pagination: {
        current_page: page,
        page_size: pageSize,
        total_documents: 0,
        total_pages: 0,
        filtered_count: 0,
        has_next: false,
        has_previous: false,
        page_document_count: 0,
      },
      documents: [],
    };
  }
} 

export interface ClusterData {
  status: string;
  data_type: string;
  domain: string;
  dominant_clusters: Array<{
    kmeans_cluster_id: number;
    dominant_cluster_label: string;
    cluster_name?: string;
    keyphrases: string[];
    keyphrase_count: number;
    document_count: number;
    urgent_count?: number;
    urgent_percentage?: number;
  }>;
  subclusters: Array<{
    kmeans_cluster_id: number;
    dominant_cluster_label: string;
    subcluster_id: string;
    subcluster_label: string;
    keyphrases: string[];
    keyphrase_count: number;
    document_count: number;
    urgent_count?: number;
    urgent_percentage?: number;
  }>;
}

export async function fetchClusterData(dataType: string = 'ticket', domain: string = 'banking', channel?: string): Promise<ClusterData> {
  try {
    const queryParams = new URLSearchParams({
      data_type: dataType,
      domain: domain
    });
    
    if (channel) {
      queryParams.append('channel', channel);
    }
    
    const response = await fetch(`/api/topic-analysis/clusters?${queryParams.toString()}`);
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`);
    }
    const data = await response.json();
    return data;
  } catch (error) {
    console.error('Error fetching cluster data:', error);
    throw error;
  }
} 