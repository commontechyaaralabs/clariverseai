import { NextRequest, NextResponse } from 'next/server';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const dataType = searchParams.get('data_type');
    const domain = searchParams.get('domain') || 'banking';
    const channel = searchParams.get('channel');

    if (!dataType) {
      return NextResponse.json(
        { error: 'data_type parameter is required' },
        { status: 400 }
      );
    }

    console.log(`Stats API called with data_type: ${dataType}, domain: ${domain}, channel: ${channel}`);

    // Try different backend URLs with correct path
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
        console.log(`Trying backend URL: ${backendUrl}`);
        triedUrls.push(backendUrl!);
        
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

        // Try the correct path based on FastAPI router configuration
        const response = await fetch(
          `${backendUrl}/api/v1/home/stats?${queryParams.toString()}`,
          {
            method: 'GET',
            headers,
            // Add timeout
            signal: AbortSignal.timeout(15000), // Increased timeout to 15 seconds
          }
        );

        if (response.ok) {
          const data = await response.json();
          console.log('Successfully fetched data from backend:', data);
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
    console.log('All backend URLs failed, returning mock data');
    console.log('Tried URLs:', triedUrls);
    console.log('Last error:', lastError);
    
    // Generate mock data based on data type
    const mockData = {
      email: {
        total_no_of_emails: 1250,
        total_urgent_messages: 45,
        urgent_percentage: 3.6,
        total_dominant_clusters: 12,
        total_subclusters: 35,
        last_run_date: '2025-01-17 15:30',
      },
      chat: {
        total_no_of_emails: 890,
        total_urgent_messages: 23,
        urgent_percentage: 2.6,
        total_dominant_clusters: 8,
        total_subclusters: 28,
        last_run_date: '2025-01-17 14:45',
      },
      ticket: {
        total_no_of_emails: 1567,
        total_urgent_messages: 78,
        urgent_percentage: 5.0,
        total_dominant_clusters: 15,
        total_subclusters: 42,
        last_run_date: '2025-01-17 16:15',
      },
      voice: {
        total_no_of_emails: 2040,
        total_urgent_messages: 251,
        urgent_percentage: 12.3,
        total_dominant_clusters: 24,
        total_subclusters: 53,
        last_run_date: '2025-01-17 17:00',
      },
      twitter: {
        total_no_of_emails: 2000,
        total_urgent_messages: 180,
        urgent_percentage: 9.0,
        total_dominant_clusters: 18,
        total_subclusters: 45,
        last_run_date: '2025-01-17 16:45',
      },
    };

    const mockStats = mockData[dataType as keyof typeof mockData] || mockData.email;

    return NextResponse.json({
      status: 'success',
      statistics: {
        data_type: dataType,
        domain: domain,
        ...mockStats,
      },
      note: 'Using mock data - backend server not accessible. To use real data, start the backend server with: cd backend/ranjith/api && python main.py'
    });

  } catch (error) {
    console.error('Error in stats API route:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
} 