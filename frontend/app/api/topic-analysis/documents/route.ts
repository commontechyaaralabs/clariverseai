import { NextRequest, NextResponse } from 'next/server';

export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const dataType = searchParams.get('data_type');
    const domain = searchParams.get('domain') || 'banking';
    const kmeansClusterId = searchParams.get('kmeans_cluster_id');
    const subclusterId = searchParams.get('subcluster_id');
    const page = searchParams.get('page') || '1';
    const pageSize = searchParams.get('page_size') || '10';
    const channel = searchParams.get('channel');
    const platform = searchParams.get('platform');

    if (!dataType || !kmeansClusterId) {
      return NextResponse.json(
        { error: 'data_type and kmeans_cluster_id parameters are required' },
        { status: 400 }
      );
    }

    console.log(`Topic Analysis Documents API called with data_type: ${dataType}, domain: ${domain}, kmeans_cluster_id: ${kmeansClusterId}, subcluster_id: ${subclusterId}, channel: ${channel}`);

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
        if (!backendUrl) continue; // Skip undefined backend URLs
        console.log(`Trying backend URL: ${backendUrl}`);
        triedUrls.push(backendUrl as string);

                 const params = new URLSearchParams({
           data_type: dataType!,
           domain: domain,
           kmeans_cluster_id: kmeansClusterId,
           page: page,
           page_size: pageSize, // This will be 10 by default
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

        const response = await fetch(
          `${backendUrl}/api/topic-analysis/documents?${params.toString()}`,
          {
            method: 'GET',
            headers: {
              'Content-Type': 'application/json',
            },
            signal: AbortSignal.timeout(5000),
          }
        );

        if (response.ok) {
          const data = await response.json();
          console.log('Successfully fetched documents from backend:', data);
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
    console.log('All backend URLs failed, returning mock document data');
    console.log('Tried URLs:', triedUrls);
    console.log('Last error:', lastError);
      
    // Generate mock document data based on data type
    const mockData = {
      email: [
        {
          _id: '1',
          message_id: 'msg_001',
          sender_id: 'user@example.com',
          sender_name: 'John Doe',
          subject: 'Account Access Issue',
          message_text: 'I cannot access my account. Please help.',
          timestamp: '2025-01-17T10:30:00Z',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Account Issues',
          subcluster_id: subclusterId || '1-1',
          subcluster_label: 'Login Problems',
          urgency: true
        },
        {
          _id: '2',
          message_id: 'msg_002',
          sender_id: 'jane@example.com',
          sender_name: 'Jane Smith',
          subject: 'Password Reset Request',
          message_text: 'I need to reset my password.',
          timestamp: '2025-01-17T11:15:00Z',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Account Issues',
          subcluster_id: subclusterId || '1-1',
          subcluster_label: 'Login Problems',
          urgency: false
        }
      ],
      chat: [
        {
          _id: '1',
          chat_id: 'chat_001',
          total_messages: 5,
          created_at: '2025-01-17T10:30:00Z',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Customer Support',
          subcluster_id: subclusterId || '1-1',
          subcluster_label: 'General Questions',
          urgency: true
        },
        {
          _id: '2',
          chat_id: 'chat_002',
          total_messages: 3,
          created_at: '2025-01-17T11:15:00Z',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Customer Support',
          subcluster_id: subclusterId || '1-1',
          subcluster_label: 'General Questions',
          urgency: false
        }
      ],
      socialmedia: [
        {
          _id: '1',
          tweet_id: 'tweet_001',
          username: 'john_doe',
          text: 'Great banking app! Easy to use and secure.',
          created_at: '2025-01-17T10:30:00Z',
          channel: 'Twitter',
          platform: 'Twitter',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'App Experience',
          subcluster_id: subclusterId || '1-1',
          subcluster_label: 'Positive Feedback',
          like_count: 15,
          retweet_count: 3,
          reply_count: 2,
          urgency: false
        },
        {
          _id: '2',
          review_id: 'review_001',
          username: 'jane_smith',
          review_title: 'Excellent mobile banking',
          text: 'The mobile banking app is fantastic. Very user-friendly interface.',
          created_at: '2025-01-17T11:15:00Z',
          channel: 'App Store/Google Play',
          platform: 'App Store',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'App Experience',
          subcluster_id: subclusterId || '1-1',
          subcluster_label: 'Positive Feedback',
          rating: 5,
          review_helpful: 8,
          urgency: false
        },
        {
          _id: '3',
          review_id: 'review_002',
          username: 'mike_wilson',
          review_title: 'Good but needs improvement',
          text: 'The app works well but could use some UI improvements.',
          created_at: '2025-01-17T12:00:00Z',
          channel: 'App Store/Google Play',
          platform: 'Google Play Store',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'App Experience',
          subcluster_id: subclusterId || '1-2',
          subcluster_label: 'UI Issues',
          rating: 4,
          review_helpful: 5,
          urgency: true
        },
        {
          _id: '4',
          tweet_id: 'tweet_002',
          username: 'sarah_jones',
          text: 'Having trouble logging into my account. Anyone else?',
          created_at: '2025-01-17T12:30:00Z',
          channel: 'Twitter',
          platform: 'Twitter',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Account Issues',
          subcluster_id: subclusterId || '2-1',
          subcluster_label: 'Login Problems',
          like_count: 2,
          retweet_count: 1,
          reply_count: 5,
          urgency: true
        },
        {
          _id: '5',
          review_id: 'review_003',
          username: 'alex_brown',
          review_title: 'Login issues',
          text: 'Cannot log into my account. Very frustrating experience.',
          created_at: '2025-01-17T13:00:00Z',
          channel: 'App Store/Google Play',
          platform: 'App Store',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Account Issues',
          subcluster_id: subclusterId || '2-1',
          subcluster_label: 'Login Problems',
          rating: 2,
          review_helpful: 12,
          urgency: true
        },
        {
          _id: '6',
          review_id: 'review_004',
          username: 'lisa_garcia',
          review_title: 'Great customer service',
          text: 'Had an issue but customer service resolved it quickly.',
          created_at: '2025-01-17T13:30:00Z',
          channel: 'App Store/Google Play',
          platform: 'Google Play Store',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Customer Support',
          subcluster_id: subclusterId || '3-1',
          subcluster_label: 'Service Quality',
          rating: 5,
          review_helpful: 7,
          urgency: true
        },
        {
          _id: '7',
          tweet_id: 'tweet_003',
          username: 'david_lee',
          text: 'Transaction failed again. This is getting ridiculous!',
          created_at: '2025-01-17T14:00:00Z',
          channel: 'Twitter',
          platform: 'Twitter',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Payment Processing',
          subcluster_id: subclusterId || '4-1',
          subcluster_label: 'Transaction Errors',
          like_count: 8,
          retweet_count: 2,
          reply_count: 3,
          urgency: true
        },
        {
          _id: '8',
          review_id: 'review_005',
          username: 'emma_taylor',
          review_title: 'Payment processing issues',
          text: 'Payments keep failing. Need to fix this ASAP.',
          created_at: '2025-01-17T14:30:00Z',
          channel: 'App Store/Google Play',
          platform: 'App Store',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Payment Processing',
          subcluster_id: subclusterId || '4-1',
          subcluster_label: 'Transaction Errors',
          rating: 1,
          review_helpful: 15,
          urgency: true
        },
        {
          _id: '9',
          review_id: 'review_006',
          username: 'tom_anderson',
          review_title: 'Smooth payments',
          text: 'All my payments go through without any issues.',
          created_at: '2025-01-17T15:00:00Z',
          channel: 'App Store/Google Play',
          platform: 'Google Play Store',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Payment Processing',
          subcluster_id: subclusterId || '4-2',
          subcluster_label: 'Successful Transactions',
          rating: 5,
          review_helpful: 3,
          urgency: false
        },
        {
          _id: '10',
          tweet_id: 'tweet_004',
          username: 'rachel_martin',
          text: 'Love the new features in the latest update!',
          created_at: '2025-01-17T15:30:00Z',
          channel: 'Twitter',
          platform: 'Twitter',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'App Experience',
          subcluster_id: subclusterId || '1-3',
          subcluster_label: 'Feature Updates',
          like_count: 12,
          retweet_count: 4,
          reply_count: 1,
          urgency: false
        }
      ],
      ticket: [
        {
          _id: '1',
          ticket_number: 'TK-001',
          title: 'ATM Card Reader Not Working',
          description: 'The card reader at ATM location is not accepting my card.',
          priority: 'P2 - High',
          created: '17/01/2025 10:30',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'ATM Issues',
          subcluster_id: subclusterId || '1-1',
          subcluster_label: 'Card Reader Problems',
          urgency: true
        },
        {
          _id: '2',
          ticket_number: 'TK-002',
          title: 'Online Banking Login Issue',
          description: 'Cannot log into online banking portal.',
          priority: 'P3 - Medium',
          created: '17/01/2025 11:15',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Online Banking',
          subcluster_id: subclusterId || '2-1',
          subcluster_label: 'Login Issues',
          urgency: false
        },
        {
          _id: '3',
          ticket_number: 'TK-003',
          title: 'Mobile App Crashes',
          description: 'Mobile banking app keeps crashing on startup.',
          priority: 'P1 - Critical',
          created: '17/01/2025 12:00',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Mobile Banking',
          subcluster_id: subclusterId || '3-1',
          subcluster_label: 'App Crashes',
          urgency: true
        },
        {
          _id: '4',
          ticket_number: 'TK-004',
          title: 'Transaction Failed',
          description: 'Payment transaction failed with error code 500.',
          priority: 'P2 - High',
          created: '17/01/2025 12:30',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Payment Processing',
          subcluster_id: subclusterId || '4-1',
          subcluster_label: 'Transaction Errors',
          urgency: true
        },
        {
          _id: '5',
          ticket_number: 'TK-005',
          title: 'Password Reset Email',
          description: 'Did not receive password reset email.',
          priority: 'P3 - Medium',
          created: '17/01/2025 13:00',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Account Security',
          subcluster_id: subclusterId || '5-1',
          subcluster_label: 'Password Issues',
          urgency: false
        },
        {
          _id: '6',
          ticket_number: 'TK-006',
          title: 'ATM Out of Cash',
          description: 'ATM at downtown location is out of cash.',
          priority: 'P2 - High',
          created: '17/01/2025 13:30',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'ATM Issues',
          subcluster_id: subclusterId || '6-1',
          subcluster_label: 'Cash Dispensing',
          urgency: true
        },
        {
          _id: '7',
          ticket_number: 'TK-007',
          title: 'Account Locked',
          description: 'My account has been locked due to suspicious activity.',
          priority: 'P1 - Critical',
          created: '17/01/2025 14:00',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Account Security',
          subcluster_id: subclusterId || '7-1',
          subcluster_label: 'Account Lockouts',
          urgency: true
        },
        {
          _id: '8',
          ticket_number: 'TK-008',
          title: 'Statement Download Failed',
          description: 'Cannot download monthly statement PDF.',
          priority: 'P3 - Medium',
          created: '17/01/2025 14:30',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Document Access',
          subcluster_id: subclusterId || '8-1',
          subcluster_label: 'Statement Issues',
          urgency: false
        },
        {
          _id: '9',
          ticket_number: 'TK-009',
          title: 'Wire Transfer Delayed',
          description: 'International wire transfer is taking longer than expected.',
          priority: 'P2 - High',
          created: '17/01/2025 15:00',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Payment Processing',
          subcluster_id: subclusterId || '9-1',
          subcluster_label: 'Transfer Delays',
          urgency: true
        },
        {
          _id: '10',
          ticket_number: 'TK-010',
          title: 'Card Declined',
          description: 'My debit card was declined at multiple locations.',
          priority: 'P1 - Critical',
          created: '17/01/2025 15:30',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Card Services',
          subcluster_id: subclusterId || '10-1',
          subcluster_label: 'Card Declines',
          urgency: true
        },
        {
          _id: '11',
          ticket_number: 'TK-011',
          title: 'Online Chat Not Working',
          description: 'Live chat feature is not loading on the website.',
          priority: 'P3 - Medium',
          created: '17/01/2025 16:00',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Customer Support',
          subcluster_id: subclusterId || '11-1',
          subcluster_label: 'Chat Issues',
          urgency: false
        },
        {
          _id: '12',
          ticket_number: 'TK-012',
          title: 'Account Balance Incorrect',
          description: 'My account balance shows incorrect amount.',
          priority: 'P2 - High',
          created: '17/01/2025 16:30',
          kmeans_cluster_id: parseInt(kmeansClusterId),
          dominant_cluster_label: 'Account Management',
          subcluster_id: subclusterId || '12-1',
          subcluster_label: 'Balance Issues',
          urgency: true
        }
      ]
    };

         const mockDocuments = mockData[dataType as keyof typeof mockData] || mockData.socialmedia;
     
     // Calculate pagination for mock data
     const currentPage = parseInt(page);
     const pageSizeInt = parseInt(pageSize);
     const totalDocuments = mockDocuments.length;
     const totalPages = Math.ceil(totalDocuments / pageSizeInt);
     const skip = (currentPage - 1) * pageSizeInt;
     const endIndex = skip + pageSizeInt;
     
     // Get the correct slice of documents for the current page
     const paginatedDocuments = mockDocuments.slice(skip, endIndex);
     const pageDocumentCount = paginatedDocuments.length;

     return NextResponse.json({
       status: 'success',
       data_type: dataType,
       domain: domain,
       filters: {
         kmeans_cluster_id: parseInt(kmeansClusterId),
         subcluster_id: subclusterId,
         domain: domain,
       },
       pagination: {
         current_page: currentPage,
         page_size: pageSizeInt,
         total_documents: totalDocuments,
         total_pages: totalPages,
         filtered_count: totalDocuments,
         has_next: currentPage < totalPages,
         has_previous: currentPage > 1,
         page_document_count: pageDocumentCount,
       },
       documents: paginatedDocuments,
       note: 'Using mock data - backend server not accessible. To use real data, start the backend server.'
     });

  } catch (error) {
    console.error('Error in topic analysis documents API route:', error);
    return NextResponse.json(
      { error: 'Internal server error' },
      { status: 500 }
    );
  }
} 