import { NextResponse } from 'next/server';

// Mock data for email messages
const mockEmails = [
  {
    id: "EML-001",
    subject: "System Maintenance Notification",
    sender: "IT Support <support@bank.com>",
    recipient: "all-staff@bank.com",
    content: "Scheduled maintenance will occur tonight from 2-4 AM. Some services may be temporarily unavailable.",
    timestamp: "17/01/2025 22:24",
    category: "System Notification",
    priority: "Medium",
    status: "Sent"
  },
  {
    id: "EML-002",
    subject: "Security Alert - Suspicious Login Attempts",
    sender: "Security Team <security@bank.com>",
    recipient: "admin@bank.com",
    content: "Multiple failed login attempts detected from unusual IP addresses. Please review security logs immediately.",
    timestamp: "17/01/2025 22:25",
    category: "Security Alert",
    priority: "High",
    status: "Sent"
  },
  {
    id: "EML-003",
    subject: "Customer Complaint - Mobile App Issues",
    sender: "Customer Service <customerservice@bank.com>",
    recipient: "tech-support@bank.com",
    content: "Customer reports that mobile app crashes when accessing account details. Issue needs immediate attention.",
    timestamp: "17/01/2025 22:26",
    category: "Customer Issue",
    priority: "High",
    status: "In Progress"
  },
  {
    id: "EML-004",
    subject: "Weekly Performance Report",
    sender: "Analytics Team <analytics@bank.com>",
    recipient: "management@bank.com",
    content: "System performance metrics for the past week. All systems operating within normal parameters.",
    timestamp: "17/01/2025 22:27",
    category: "Report",
    priority: "Low",
    status: "Sent"
  },
  {
    id: "EML-005",
    subject: "Database Backup Failure",
    sender: "System Admin <admin@bank.com>",
    recipient: "dba-team@bank.com",
    content: "Automated backup process failed last night. Manual intervention required to resolve backup issues.",
    timestamp: "17/01/2025 22:28",
    category: "System Issue",
    priority: "Critical",
    status: "Urgent"
  }
];

export async function GET() {
  try {
    // Simulate API delay
    await new Promise(resolve => setTimeout(resolve, 500));
    
    return NextResponse.json({
      success: true,
      data: mockEmails,
      total: mockEmails.length
    });
  } catch (error) {
    console.error('Error fetching emails:', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch emails' },
      { status: 500 }
    );
  }
} 