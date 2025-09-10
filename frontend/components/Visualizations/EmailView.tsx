"use client"

import React, { useState } from 'react';
import { ArrowLeft, ArrowRight, X } from 'lucide-react';

interface DocumentResponse {
  _id: string;
  sender_name?: string;
  subject?: string;
  message_text?: string;
  receiver_names?: string[];
  urgency?: boolean;
  dominant_topic?: string;
  subtopics?: string;
  dominant_cluster_label?: string;
  subcluster_label?: string;
  ticket_number?: string;
  title?: string;
  description?: string;
  ticket_priority?: string;
  priority?: string;
  ticket_created_at?: string;
  timestamp?: string;
}

interface EmailViewProps {
  emailViewData: {
    cluster_id: number;
    subcluster_label: string;
    emails: DocumentResponse[];
    total: number;
    has_more: boolean;
    pagination: unknown;
  } | null;
  loadingEmails: boolean;
  emailPagination: any;
  onBackToAnalytics: () => void;
  onEmailPageChange: (page: number) => void;
  dataType?: 'ticket' | 'email' | 'chat' | 'voice' | 'socialmedia';
}

export const EmailView: React.FC<EmailViewProps> = ({
  emailViewData,
  loadingEmails,
  emailPagination,
  onBackToAnalytics,
  onEmailPageChange,
  dataType = 'email'
}) => {
  const [selectedEmail, setSelectedEmail] = useState<DocumentResponse | null>(null);

  // Helper functions to get appropriate labels based on dataType
  const getDocumentLabel = () => {
    switch (dataType) {
      case 'email': return 'Emails';
      case 'chat': return 'Chats';
      case 'socialmedia': return 'Social Media Posts';
      case 'voice': return 'Voice Messages';
      default: return 'Tickets';
    }
  };

  const getDocumentLabelSingular = () => {
    switch (dataType) {
      case 'email': return 'Email';
      case 'chat': return 'Chat';
      case 'socialmedia': return 'Social Media Post';
      case 'voice': return 'Voice Message';
      default: return 'Ticket';
    }
  };

  if (!emailViewData) return null;

  const { emails: emailList } = emailViewData;

  return (
    <div className="max-w-7xl mx-auto">
      {/* Header with back button */}
      <div className="flex items-center justify-between mb-6">
        <button
          onClick={onBackToAnalytics}
          className="flex items-center gap-2 text-pink-400 hover:text-pink-300 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Back to Analytics
        </button>
        <h1 className="text-2xl font-bold text-white">
          {getDocumentLabel()}
        </h1>
      </div>

      {/* Loading state */}
      {loadingEmails && (
        <div className="bg-gray-800 rounded-lg p-6 text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-400 mx-auto mb-4"></div>
          <p className="text-gray-300">Loading {getDocumentLabelSingular().toLowerCase()}...</p>
        </div>
      )}

      {/* Main Content */}
      {!loadingEmails && (
        <div className="flex-1 flex flex-col">
          {/* Toolbar */}
          <div className="bg-gray-800 bg-opacity-80 border-b border-gray-700 p-4 flex items-center justify-between mb-4">
            <div className="text-sm text-gray-300">
              Showing {emailPagination?.page_document_count || 0} of {emailPagination?.total_documents || 0} {getDocumentLabel().toLowerCase()}
            </div>
          </div>

          {/* Content Area */}
          <div className="flex-1 flex overflow-hidden">
            {/* Email List */}
            <div className={`${selectedEmail ? 'w-1/3' : 'w-full'} border-r border-gray-700 bg-gray-800 bg-opacity-50 overflow-y-auto`}>
              {emailList.length === 0 ? (
                <div className="p-4 text-center text-gray-300">
                  No {getDocumentLabelSingular().toLowerCase()} found for this selection.
                </div>
              ) : (
                <>
                  {emailList.map((email: DocumentResponse) => (
                    <div
                      key={email._id}
                      onClick={() => setSelectedEmail(email)}
                      className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer ${
                        selectedEmail?._id === email._id ? 'bg-pink-600 bg-opacity-20' : ''
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex-1">
                          <div className="font-medium text-white">{email.ticket_number || email.sender_name || 'No Ticket Number'}</div>
                          <div className="text-sm text-gray-400">{email.title || email.subject || 'No Title'}</div>
                        </div>
                        <div className="flex items-center gap-2">
                          {email.urgency && (
                            <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                              Urgent
                            </span>
                          )}
                          {email.ticket_priority && (
                            <span className="px-2 py-1 bg-yellow-600 text-white text-xs rounded-full">
                              {email.ticket_priority}
                            </span>
                          )}
                          <div className="text-xs text-gray-300">
                            {email.dominant_cluster_label || 'No Topic'}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                  {emailPagination && emailPagination.total_pages > 1 && (
                    <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => onEmailPageChange(emailPagination.current_page - 1)}
                          disabled={!emailPagination.has_previous}
                          className="flex items-center gap-2 px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                          <ArrowLeft className="w-4 h-4" />
                          Previous
                        </button>
                        <span className="text-sm text-gray-300 px-3">
                          Page {emailPagination.current_page} of {emailPagination.total_pages}
                        </span>
                        <button
                          onClick={() => onEmailPageChange(emailPagination.current_page + 1)}
                          disabled={!emailPagination.has_next}
                          className="flex items-center gap-2 px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                          Next
                          <ArrowRight className="w-4 h-4" />
                        </button>
                      </div>
                      <div className="text-sm text-gray-400">
                        Page {emailPagination.current_page} of {emailPagination.total_pages} • Showing {emailPagination.page_document_count} of {emailPagination.total_documents} {getDocumentLabel().toLowerCase()}
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>

            {/* Email Details */}
            {selectedEmail && (
              <div className="w-2/3 bg-gray-800 bg-opacity-80 overflow-y-auto p-6">
                <div className="flex justify-between items-center mb-4">
                  <h2 className="text-xl font-semibold text-white">{selectedEmail.title || selectedEmail.subject || 'No Title'}</h2>
                  <button
                    onClick={() => setSelectedEmail(null)}
                    className="p-1 hover:bg-gray-700 rounded-full"
                  >
                    <X className="w-5 h-5 text-gray-300" />
                  </button>
                </div>
                <div className="border-b border-gray-600 pb-4 mb-4">
                  <div className="text-sm text-gray-300">From: {selectedEmail.sender_name || 'N/A'}</div>
                  <div className="text-sm text-gray-300">To: {selectedEmail.receiver_names?.join(', ') || 'N/A'}</div>
                  <div className="text-sm text-gray-300">Timestamp: {selectedEmail.timestamp || selectedEmail.ticket_created_at || 'N/A'}</div>
                </div>
                <div className="space-y-6">
                  <div>
                    <label className="text-sm font-medium text-gray-300">Description</label>
                    <div className="mt-1 p-4 bg-gray-900 rounded-lg text-gray-200">
                      {selectedEmail.description || selectedEmail.message_text || 'No description available'}
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="text-sm font-medium text-gray-300">Dominant Topic</label>
                      <div className="mt-1 text-pink-400 font-medium">{selectedEmail.dominant_topic || selectedEmail.dominant_cluster_label || 'N/A'}</div>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-300">Subtopics</label>
                      <div className="mt-1 text-purple-400 font-medium">{selectedEmail.subtopics || 'N/A'}</div>
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="text-sm font-medium text-gray-300">Dominant Cluster Label</label>
                      <div className="mt-1 text-gray-200">{selectedEmail.dominant_cluster_label || 'N/A'}</div>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-300">Subcluster Label</label>
                      <div className="mt-1 text-gray-200">{selectedEmail.subcluster_label || 'N/A'}</div>
                    </div>
                  </div>
                </div>
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
};
