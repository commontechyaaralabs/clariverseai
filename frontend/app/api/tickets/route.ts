import { NextResponse } from 'next/server';

// Mock data for tickets
const mockTickets = [
  {
    ticket_number: "TKT-001",
    title: "ATM Card Reader Malfunction",
    description: "Multiple reports of card reader failures at downtown branch ATM location DT-001. Customers unable to complete transactions.",
    priority: "P2 - High",
    created: "17/01/2025 22:24"
  },
  {
    ticket_number: "TKT-002", 
    title: "Online Banking Login Issues",
    description: "Users experiencing timeout errors when attempting to log into online banking portal. Affecting approximately 15% of users.",
    priority: "P1 - Critical",
    created: "17/01/2025 22:25"
  },
  {
    ticket_number: "TKT-003",
    title: "Mobile App Push Notifications",
    description: "Push notifications not being delivered to mobile app users. Investigation shows server configuration issue.",
    priority: "P3 - Medium", 
    created: "17/01/2025 22:26"
  },
  {
    ticket_number: "TKT-004",
    title: "Database Connection Timeout",
    description: "Periodic database connection timeouts affecting transaction processing. Monitoring shows increased latency.",
    priority: "P2 - High",
    created: "17/01/2025 22:27"
  },
  {
    ticket_number: "TKT-005",
    title: "Payment Gateway Integration Error",
    description: "Third-party payment gateway returning 500 errors for 3% of transactions. Vendor contacted for resolution.",
    priority: "P1 - Critical",
    created: "17/01/2025 22:28"
  }
];

export async function GET() {
  try {
    // Simulate API delay
    await new Promise(resolve => setTimeout(resolve, 500));
    
    return NextResponse.json({
      success: true,
      data: mockTickets,
      total: mockTickets.length
    });
  } catch (error) {
    console.error('Error fetching tickets:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch tickets' },
      { status: 500 }
    );
  }
} 