import { NextRequest, NextResponse } from 'next/server';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const dataType = searchParams.get('data_type') || 'ticket';
    const domain = searchParams.get('domain') || 'banking';
    const channel = searchParams.get('channel');

    console.log(`Clusters API called with data_type: ${dataType}, domain: ${domain}, channel: ${channel}`);

    // Try different backend URLs
    const possibleBackendUrls = [
      'https://clariversev1-153115538723.us-central1.run.app',
      process.env.BACKEND_URL,
      'http://localhost:8000',
      'http://127.0.0.1:8000',
      'http://localhost:3001',
      'http://127.0.0.1:3001',
    ].filter(Boolean);

    let lastError: Error | null = null;
    const triedUrls: string[] = [];

    for (const backendUrl of possibleBackendUrls) {
      try {
        if (typeof backendUrl !== 'string') {
          continue;
        }
        console.log(`Trying backend URL: ${backendUrl}`);
        triedUrls.push(backendUrl);

        // Build query parameters
        const queryParams = new URLSearchParams({
          data_type: dataType,
          domain: domain
        });
        
        if (channel) {
          queryParams.append('channel', channel);
        }

        // Get authorization header from the incoming request
        const authHeader = request.headers.get('authorization');
        const headers: HeadersInit = {
          'Content-Type': 'application/json',
        };
        
        if (authHeader) {
          headers['Authorization'] = authHeader;
        }

        const response = await fetch(
          `${backendUrl}/api/topic-analysis/clusters?${queryParams.toString()}`,
          {
            method: 'GET',
            headers,
            signal: AbortSignal.timeout(30000), // Increased timeout to 30 seconds for heavy processing
          }
        );

        if (response.ok) {
          const data = await response.json();
          console.log('Successfully fetched cluster data from backend:', data);
          return NextResponse.json(data);
        } else {
          console.error(`Backend API error for ${backendUrl}: ${response.status} ${response.statusText}`);
          lastError = new Error(`Backend API error: ${response.status} ${response.statusText}`);
        }
      } catch (error) {
        console.error(`Error connecting to ${backendUrl}:`, error);
        lastError = error instanceof Error ? error : new Error(String(error));
      }
    }

    // If all backend URLs failed, return mock data for development
    console.log('All backend URLs failed, returning mock cluster data');
    console.log('Tried URLs:', triedUrls);
    console.log('Last error:', lastError);
    
    // Generate mock cluster data
    const mockClusters = {
      ticket: {
        status: 'success',
        data_type: 'ticket',
        domain: 'banking',
        dominant_clusters: [
          {
            kmeans_cluster_id: 0,
            dominant_cluster_label: "ATM Issues and Card Reader Problems",
            cluster_name: "atm card reader",
            keyphrases: ["atm card reader malfunction", "card reader issues"],
            keyphrase_count: 2,
            document_count: 70
          },
          {
            kmeans_cluster_id: 1,
            dominant_cluster_label: "Online Banking Login and Authentication",
            cluster_name: "online banking login",
            keyphrases: ["login problems", "authentication issues", "password reset"],
            keyphrase_count: 3,
            document_count: 120
          },
          {
            kmeans_cluster_id: 2,
            dominant_cluster_label: "Payment Processing and Transaction Failures",
            cluster_name: "payment processing",
            keyphrases: ["transaction failures", "payment declined", "processing delays"],
            keyphrase_count: 3,
            document_count: 85
          },
          {
            kmeans_cluster_id: 3,
            dominant_cluster_label: "Security Concerns and Fraud Alerts",
            cluster_name: "security fraud",
            keyphrases: ["suspicious activity", "fraud alerts", "unauthorized access"],
            keyphrase_count: 3,
            document_count: 45
          },
          {
            kmeans_cluster_id: 4,
            dominant_cluster_label: "Network Connectivity and System Outages",
            cluster_name: "network connectivity",
            keyphrases: ["connection timeouts", "system outages", "network issues"],
            keyphrase_count: 3,
            document_count: 60
          }
        ],
        subclusters: []
      },
      email: {
        status: 'success',
        data_type: 'email',
        domain: 'banking',
        dominant_clusters: [
          {
            kmeans_cluster_id: 0,
            dominant_cluster_label: "Branch Deposit Exception Review Workflow",
            cluster_name: "branch audit notification",
            keyphrases: ["branch audit notification"],
            keyphrase_count: 1,
            document_count: 70
          },
          {
            kmeans_cluster_id: 1,
            dominant_cluster_label: "SentinelOne XDR Threat Hunting Platform",
            cluster_name: "sentinel / system",
            keyphrases: ["sentinel system upgrade", "sentinel system launch", "sentinel system update"],
            keyphrase_count: 3,
            document_count: 33
          },
          {
            kmeans_cluster_id: 2,
            dominant_cluster_label: "Loan Servicing Document Versioning Engine",
            cluster_name: "loan documentation revision",
            keyphrases: ["loan documentation revision"],
            keyphrase_count: 1,
            document_count: 97
          },
          {
            kmeans_cluster_id: 3,
            dominant_cluster_label: "Finacle Core System Availability Monitoring",
            cluster_name: "finacle / system / outage",
            keyphrases: ["finacle system maintenance", "finacle outage impact", "finacle system outage"],
            keyphrase_count: 3,
            document_count: 15
          }
        ],
        subclusters: []
      },
      chat: {
        status: 'success',
        data_type: 'chat',
        domain: 'banking',
        dominant_clusters: [
          {
            kmeans_cluster_id: 0,
            dominant_cluster_label: "Customer Service Inquiries",
            cluster_name: "customer service",
            keyphrases: ["customer support", "service inquiries"],
            keyphrase_count: 2,
            document_count: 50
          },
          {
            kmeans_cluster_id: 1,
            dominant_cluster_label: "Technical Support Issues",
            cluster_name: "technical support",
            keyphrases: ["technical issues", "system problems"],
            keyphrase_count: 2,
            document_count: 35
          }
        ],
        subclusters: []
      }
    };

    const mockData = mockClusters[dataType as keyof typeof mockClusters] || mockClusters.ticket;

    return NextResponse.json({
      ...mockData,
      note: 'Using mock data - backend server not accessible. To use real data, start the backend server.'
    });

  } catch (error) {
    console.error('Error in clusters API route:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
} 