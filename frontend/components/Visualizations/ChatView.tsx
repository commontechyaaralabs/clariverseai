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
  // Chat specific fields
  chat_id?: string;
  chat_members?: unknown[];
  raw_segments?: unknown[];
  cleaned_segments?: unknown[];
  total_messages?: number;
  created_at?: string;
  channel_name?: string;
  user_id?: string;
}

interface ChatViewProps {
  chatViewData: {
    cluster_id: number;
    subcluster_label: string;
    chats: DocumentResponse[];
    total: number;
    has_more: boolean;
    pagination: unknown;
  } | null;
  loadingChats: boolean;
  chatPagination: any;
  onBackToAnalytics: () => void;
  onChatPageChange: (page: number) => void;
}

export const ChatView: React.FC<ChatViewProps> = ({
  chatViewData,
  loadingChats,
  chatPagination,
  onBackToAnalytics,
  onChatPageChange
}) => {
  const [selectedChat, setSelectedChat] = useState<DocumentResponse | null>(null);

  if (!chatViewData) return null;

  const { chats: chatList } = chatViewData;

  // Helper function to format chat members
  const formatChatMembers = (chat: DocumentResponse) => {
    if (chat.chat_members && chat.chat_members.length > 0) {
      return chat.chat_members.map((member: unknown) => (member as { display_name?: string; name?: string; id?: string })?.display_name || (member as { display_name?: string; name?: string; id?: string })?.name || (member as { display_name?: string; name?: string; id?: string })?.id || 'Unknown').join(', ');
    }
    return 'No members';
  };

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
          Chats
        </h1>
      </div>

      {/* Loading state */}
      {loadingChats && (
        <div className="bg-gray-800 rounded-lg p-6 text-center">
          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-400 mx-auto mb-4"></div>
          <p className="text-gray-300">Loading chats...</p>
        </div>
      )}

      {/* Main Content */}
      {!loadingChats && (
        <div className="flex-1 flex flex-col">
          {/* Toolbar */}
          <div className="bg-gray-800 bg-opacity-80 border-b border-gray-700 p-4 flex items-center justify-between mb-4">
            <div className="text-sm text-gray-300">
              Showing {chatPagination?.page_document_count || 0} of {chatPagination?.total_documents || 0} chats
            </div>
          </div>

          {/* Content Area */}
          <div className="flex-1 flex overflow-hidden">
            {/* Chat List */}
            <div className={`${selectedChat ? 'w-1/3' : 'w-full'} border-r border-gray-700 bg-gray-800 bg-opacity-50 overflow-y-auto`}>
              {chatList.length === 0 ? (
                <div className="p-4 text-center text-gray-300">
                  No chats found for this selection.
                </div>
              ) : (
                <>
                  {chatList.map((chat: DocumentResponse) => (
                    <div
                      key={chat._id}
                      onClick={() => setSelectedChat(chat)}
                      className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer ${
                        selectedChat?._id === chat._id ? 'bg-pink-600 bg-opacity-20' : ''
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div className="flex-1">
                          <div className="font-medium text-white">{formatChatMembers(chat)}</div>
                        </div>
                        <div className="flex items-center gap-2">
                          {chat.urgency && (
                            <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                              Urgent
                            </span>
                          )}
                          <div className="text-xs text-gray-300">
                            {chat.dominant_cluster_label || 'No Topic'}
                          </div>
                        </div>
                      </div>
                    </div>
                  ))}
                  {chatPagination && chatPagination.total_pages > 1 && (
                    <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                      <div className="flex items-center gap-2">
                        <button
                          onClick={() => onChatPageChange(chatPagination.current_page - 1)}
                          disabled={!chatPagination.has_previous}
                          className="flex items-center gap-2 px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                          <ArrowLeft className="w-4 h-4" />
                          Previous
                        </button>
                        <span className="text-sm text-gray-300 px-3">
                          Page {chatPagination.current_page} of {chatPagination.total_pages}
                        </span>
                        <button
                          onClick={() => onChatPageChange(chatPagination.current_page + 1)}
                          disabled={!chatPagination.has_next}
                          className="flex items-center gap-2 px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                        >
                          Next
                          <ArrowRight className="w-4 h-4" />
                        </button>
                      </div>
                      <div className="text-sm text-gray-400">
                        Page {chatPagination.current_page} of {chatPagination.total_pages} • Showing {chatPagination.page_document_count} of {chatPagination.total_documents} chats
                      </div>
                    </div>
                  )}
                </>
              )}
            </div>

            {/* Chat Details */}
            {selectedChat && (
              <div className="w-2/3 bg-gray-800 bg-opacity-80 overflow-y-auto p-6">
                <div className="flex justify-between items-center mb-4">
                  <h2 className="text-xl font-semibold text-white">Chat ID: {selectedChat.chat_id || 'No Chat ID'}</h2>
                  <button
                    onClick={() => setSelectedChat(null)}
                    className="p-1 hover:bg-gray-700 rounded-full"
                  >
                    <X className="w-5 h-5 text-gray-300" />
                  </button>
                </div>
                <div className="border-b border-gray-600 pb-4 mb-4">
                  <div className="text-sm text-gray-300">Created: {selectedChat.created_at || selectedChat.timestamp || 'N/A'}</div>
                  <div className="text-sm text-gray-300">Total Messages: {selectedChat.total_messages || 'N/A'}</div>
                  <div className="text-sm text-gray-300">Members: {formatChatMembers(selectedChat)}</div>
                </div>
                <div className="space-y-6">
                  <div>
                    <label className="text-sm font-medium text-gray-300">Messages</label>
                    <div className="mt-1 p-4 bg-gray-900 rounded-lg text-gray-200 max-h-60 overflow-y-auto">
                      {selectedChat.raw_segments && selectedChat.raw_segments.length > 0 ? (
                        <div className="space-y-3">
                          {selectedChat.raw_segments.map((segment: any, index: number) => (
                            <div key={index} className="border-b border-gray-700 pb-2 last:border-b-0">
                              <div className="flex justify-between items-start mb-1">
                                <span className="text-blue-400 text-sm font-medium">
                                  {segment.sender_name || segment.sender || segment.speaker || 'Unknown'}
                                </span>
                                <span className="text-gray-500 text-xs">
                                  {segment.timestamp || segment.time || 'No timestamp'}
                                </span>
                              </div>
                              <div className="text-gray-200 text-sm">
                                {segment.text || segment.message || segment.content || 'No message content'}
                              </div>
                            </div>
                          ))}
                        </div>
                      ) : (
                        <div className="text-gray-400">No raw segments available</div>
                      )}
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="text-sm font-medium text-gray-300">Dominant Topic</label>
                      <div className="mt-1 text-pink-400 font-medium">{selectedChat.dominant_topic || selectedChat.dominant_cluster_label || 'N/A'}</div>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-300">Subtopics</label>
                      <div className="mt-1 text-purple-400 font-medium">{selectedChat.subtopics || 'N/A'}</div>
                    </div>
                  </div>
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <label className="text-sm font-medium text-gray-300">Dominant Cluster Label</label>
                      <div className="mt-1 text-gray-200">{selectedChat.dominant_cluster_label || 'N/A'}</div>
                    </div>
                    <div>
                      <label className="text-sm font-medium text-gray-300">Subcluster Label</label>
                      <div className="mt-1 text-gray-200">{selectedChat.subcluster_label || 'N/A'}</div>
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
