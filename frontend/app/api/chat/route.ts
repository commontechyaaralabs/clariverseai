import { NextResponse } from 'next/server';

// Mock data for chat messages
const mockChatMessages = [
  {
    id: "CHT-001",
    sender: "John Smith",
    message: "Has anyone else noticed the slow response times on the customer portal today?",
    timestamp: "17/01/2025 22:24",
    channel: "General Support",
    sentiment: "Neutral",
    topic: "Performance Issues"
  },
  {
    id: "CHT-002",
    sender: "Sarah Johnson", 
    message: "Yes, I've been getting complaints from customers about the login page taking too long to load.",
    timestamp: "17/01/2025 22:25",
    channel: "General Support",
    sentiment: "Negative",
    topic: "Performance Issues"
  },
  {
    id: "CHT-003",
    sender: "Mike Chen",
    message: "I think it might be related to the database migration we did last night. Let me check the logs.",
    timestamp: "17/01/2025 22:26",
    channel: "Technical",
    sentiment: "Neutral",
    topic: "Database Issues"
  },
  {
    id: "CHT-004",
    sender: "Lisa Rodriguez",
    message: "Great catch! I'll escalate this to the infrastructure team right away.",
    timestamp: "17/01/2025 22:27",
    channel: "Management",
    sentiment: "Positive",
    topic: "Escalation"
  },
  {
    id: "CHT-005",
    sender: "David Wilson",
    message: "The mobile app is working fine though, so it seems to be web-specific.",
    timestamp: "17/01/2025 22:28",
    channel: "General Support",
    sentiment: "Neutral",
    topic: "Mobile vs Web"
  }
];

export async function GET() {
  try {
    // Simulate API delay
    await new Promise(resolve => setTimeout(resolve, 500));
    
    return NextResponse.json({
      success: true,
      data: mockChatMessages,
      total: mockChatMessages.length
    });
  } catch (error) {
    console.error('Error fetching chat messages:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch chat messages' },
      { status: 500 }
    );
  }
} 