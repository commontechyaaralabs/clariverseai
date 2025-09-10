"use client"

import React, { useState, useEffect, useRef } from 'react';
import { BarChart3, Layers } from 'lucide-react';
import DataLayout from '../../layout';
import { fetchStatistics, Statistics, fetchClusterData, ClusterData, fetchTopicAnalysisDocuments } from '@/lib/apiClient';
import { 
  StatisticsCards, 
  VisualizationSelector, 
  ChartComponents, 
  DataTable, 
  ChatView 
} from '@/components/Visualizations';
import { AuthGuard } from "@/components/AuthGuard";

interface Subtopic {
  name: string;
  frequency: number;
  urgent_count?: number;
  urgent_percentage?: number;
  subcluster_id?: string;
}

interface Topic {
  name: string;
  frequency: number;
  id: string;
  subtopics: Subtopic[];
  urgent_count?: number;
  urgent_percentage?: number;
}

interface SubclusterData {
  subcluster_label: string;
  chat_count: number;
  urgent_count: number;
  urgent_percentage: number;
}

interface TooltipData {
  x: number;
  y: number;
  data: {
    name: string;
    documents: number;
    urgent: number;
    urgentPercentage: number;
  } | null;
}

interface ChatViewData {
  cluster_id: number;
  subcluster_label: string;
  chats: any[];
  total: number;
  has_more: boolean;
  pagination: PaginationData;
}

interface PaginationData {
  current_page: number;
  total_pages: number;
  page_document_count: number;
  total_documents: number;
  has_previous: boolean;
  has_next: boolean;
}

const ChatHomePage = () => {
  const [selectedViz, setSelectedViz] = useState('WordCloud');
  const [expandedStats, setExpandedStats] = useState(true);
  const [sortOrder, setSortOrder] = useState<'desc' | 'asc'>('desc');
  const [dataSource, setDataSource] = useState<Topic[]>([]);
  const [expandedRows, setExpandedRows] = useState<{[key: string]: boolean}>({});
  const [searchTerm, setSearchTerm] = useState('');
  const [isSearchFocused, setIsSearchFocused] = useState(false);
  const [statistics, setStatistics] = useState<Statistics | null>(null);
  const [loadingStats, setLoadingStats] = useState(true);
  const [clusterData, setClusterData] = useState<ClusterData | null>(null);
  const [loadingClusters, setLoadingClusters] = useState(true);
  const [selectedCluster, setSelectedCluster] = useState<string | null>(null);
  const [selectedDominantCluster, setSelectedDominantCluster] = useState<number | null>(null);
  const [topicToggles, setTopicToggles] = useState<{[key: string]: boolean}>({});
  const [sortColumn, setSortColumn] = useState<string>('No. of Chats');
  const [sortAscending, setSortAscending] = useState<boolean>(false);
  const [currentPage, setCurrentPage] = useState<number>(0);
  const [selectedTopic, setSelectedTopic] = useState<string>('Show all');
  const [subclusterData, setSubclusterData] = useState<{[key: string]: SubclusterData[]}>({});
  const [chatViewData, setChatViewData] = useState<ChatViewData | null>(null);
  const [currentChatPage, setCurrentChatPage] = useState<'home' | 'chat_view'>('home');
  const [chatPageNumber, setChatPageNumber] = useState<number>(0);
  const [chatPagination, setChatPagination] = useState<PaginationData | null>(null);
  const [loadingChats, setLoadingChats] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);
  const [tooltip, setTooltip] = useState<TooltipData>({ x: 0, y: 0, data: null });
  const [selectedTopicForSubtopicViz, setSelectedTopicForSubtopicViz] = useState<string | null>(null);
  const [selectedSubtopicViz, setSelectedSubtopicViz] = useState('WordCloud');
  const [selectedChat, setSelectedChat] = useState<any>(null);

  const recordsPerPage = 10;

  // Scroll to top only when switching to chat view from home page
  useEffect(() => {
    if (currentChatPage === 'chat_view') {
      // Small delay to ensure the view has rendered
      setTimeout(() => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }, 100);
    }
  }, [currentChatPage]);

  // Handle wordcloud click
  const handleWordClick = (word: string, value: number) => {
    setSelectedCluster(word);
    console.log(`Clicked on cluster: ${word} with ${value} documents`);
  };

  // Fetch statistics from API
  useEffect(() => {
    const loadStatistics = async () => {
      try {
        setLoadingStats(true);
        const response = await fetchStatistics('chat', 'banking');
        if (response.status === 'success') {
          setStatistics(response.statistics);
      } else {
          console.error('Failed to fetch statistics:', response);
      }
    } catch (error) {
        console.error('Error loading statistics:', error);
    } finally {
        setLoadingStats(false);
      }
    };

    loadStatistics();
  }, []);

  // Fetch cluster data from API
  useEffect(() => {
    const loadClusterData = async () => {
      try {
        setLoadingClusters(true);
        const response = await fetchClusterData('chat', 'banking');
        if (response.status === 'success') {
          setClusterData(response);
          
          // Transform cluster data to match the Topic interface
                      const transformedData: Topic[] = response.dominant_clusters.map((cluster) => {
            // Get subclusters for this dominant cluster
            const clusterSubclusters = response.subclusters.filter(
              sub => sub.kmeans_cluster_id === cluster.kmeans_cluster_id
            );
            
            const subtopics: Subtopic[] = clusterSubclusters.map(sub => ({
              name: sub.subcluster_label,
              frequency: sub.document_count || 0,
              urgent_count: sub.urgent_count,
              urgent_percentage: sub.urgent_percentage,
              subcluster_id: sub.subcluster_id
            }));
            
            return {
              name: cluster.dominant_cluster_label,
              frequency: cluster.document_count || 0,
              id: cluster.kmeans_cluster_id.toString(),
              subtopics,
              urgent_count: cluster.urgent_count,
              urgent_percentage: cluster.urgent_percentage,
              keyphrases: cluster.keyphrases || [] // Include keyphrases from dominant cluster
            };
          });
          
          setDataSource(transformedData);
        } else {
          console.error('Failed to fetch cluster data:', response);
        }
      } catch (error) {
        console.error('Error loading cluster data:', error);
      } finally {
        setLoadingClusters(false);
      }
    };

    loadClusterData();
  }, []);

  // Click outside handler
  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (searchRef.current && !searchRef.current.contains(event.target as Node)) {
        setIsSearchFocused(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const getAllSearchItems = () => {
    const items: string[] = [];
    dataSource.forEach(topic => {
      items.push(topic.name);
      topic.subtopics.forEach(subtopic => {
        items.push(subtopic.name);
      });
    });
    return items;
  };

  const getFilteredSearchItems = () => {
    if (!searchTerm.trim()) return [];
    const allItems = getAllSearchItems();
    return allItems.filter(item =>
      item.toLowerCase().includes(searchTerm.toLowerCase())
    );
  };

  const _handleSearchItemSelect = (topicName: string) => {
    setSearchTerm(topicName);
    setIsSearchFocused(false);
  };

  const _handleVizChange = (viz: string) => setSelectedViz(viz);
  const _toggleSortOrder = () => setSortOrder(prev => prev === 'desc' ? 'asc' : 'desc');
  const _toggleRowExpansion = (id: string) => {
    setExpandedRows(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const _handleDominantClusterSelect = (clusterId: number) => {
    setSelectedDominantCluster(selectedDominantCluster === clusterId ? null : clusterId);
  };

  const toggleTopicKeyphrases = (topicLabel: string) => {
    setTopicToggles(prev => ({
      ...prev,
      [topicLabel]: !prev[topicLabel]
    }));
  };

  const _handleShowSubtopicVisualization = (topicName: string) => {
    setSelectedTopicForSubtopicViz(topicName);
  };

  const _handleSubtopicVizChange = (viz: string) => setSelectedSubtopicViz(viz);

  const handleSortColumn = (column: string) => {
    if (sortColumn === column) {
      setSortAscending(!sortAscending);
    } else {
      setSortColumn(column);
      setSortAscending(true);
    }
    setCurrentPage(0);
  };

  const handleShowChats = async (clusterId: number, subclusterId: string, page: number = 1) => {
    try {
      setLoadingChats(true);
      
      console.log('handleShowChats called with:', { clusterId, subclusterId, page });
      
      const response = await fetchTopicAnalysisDocuments(
        'chat',
        clusterId,
        subclusterId,
        page,
        10,
        'banking'
      );
      
      console.log('API Response:', response);

      if (response.status === 'success') {
        setChatViewData({
          cluster_id: clusterId,
          subcluster_label: subclusterId,
          chats: response.documents,
          total: response.pagination.total_documents,
          has_more: response.pagination.has_next,
          pagination: response.pagination
        });
        setChatPagination(response.pagination);
        setCurrentChatPage('chat_view');
        setChatPageNumber(page - 1);
      } else {
        console.error('Failed to fetch chats:', response);
        // Fallback to mock data if API fails
        const mockChats = [
          {
            _id: '1',
            sender_name: 'John Doe',
            channel_name: 'Banking Support',
            message_text: 'Hi, I am having issues with my account balance not updating after a recent deposit.',
            receiver_names: ['support@bank.com'],
            urgency: true,
            dominant_topic: 'Account Issues',
            subtopics: ['Account Balance', 'Deposit Issues'],
            dominant_cluster_label: 'Account Issues',
            subcluster_label: subclusterId,
            timestamp: '2024-01-15T10:30:00Z',
            user_id: 'user123'
          },
          {
            _id: '2',
            sender_name: 'Jane Smith',
            channel_name: 'Payment Support',
            message_text: 'My payment failed and I need help resolving this issue.',
            receiver_names: ['payments@bank.com'],
            urgency: false,
            dominant_topic: 'Payment Problems',
            subtopics: ['Failed Transactions'],
            dominant_cluster_label: 'Payment Problems',
            subcluster_label: subclusterId,
            timestamp: '2024-01-15T11:15:00Z',
            user_id: 'user456'
          }
        ];

        const mockPagination = {
          current_page: page,
          page_size: 10,
          total_documents: mockChats.length,
          total_pages: 1,
          filtered_count: mockChats.length,
          has_next: false,
          has_previous: false,
          page_document_count: mockChats.length,
        };

        setChatViewData({
          cluster_id: clusterId,
          subcluster_label: subclusterId,
          chats: mockChats,
          total: mockChats.length,
          has_more: false,
          pagination: mockPagination
        });
        setChatPagination(mockPagination);
        setCurrentChatPage('chat_view');
        setChatPageNumber(page - 1);
      }
    } catch (error) {
      console.error('Error fetching chats:', error);
      // Fallback to mock data
      const mockChats = [
        {
          _id: '1',
          sender_name: 'John Doe',
          channel_name: 'Banking Support',
          message_text: 'Hi, I am having issues with my account balance not updating after a recent deposit.',
          receiver_names: ['support@bank.com'],
          urgency: true,
          dominant_topic: 'Account Issues',
          subtopics: ['Account Balance', 'Deposit Issues'],
          dominant_cluster_label: 'Account Issues',
          subcluster_label: subclusterId,
          timestamp: '2024-01-15T10:30:00Z',
          user_id: 'user123'
        },
        {
          _id: '2',
          sender_name: 'Jane Smith',
          channel_name: 'Payment Support',
          message_text: 'My payment failed and I need help resolving this issue.',
          receiver_names: ['payments@bank.com'],
          urgency: false,
          dominant_topic: 'Payment Problems',
          subtopics: ['Failed Transactions'],
          dominant_cluster_label: 'Payment Problems',
          subcluster_label: subclusterId,
          timestamp: '2024-01-15T11:15:00Z',
          user_id: 'user456'
        }
      ];

      const mockPagination = {
        current_page: page,
        page_size: 10,
        total_documents: mockChats.length,
        total_pages: 1,
        filtered_count: mockChats.length,
        has_next: false,
        has_previous: false,
        page_document_count: mockChats.length,
      };

      setChatViewData({
        cluster_id: clusterId,
        subcluster_label: subclusterId,
        chats: mockChats,
        total: mockChats.length,
        has_more: false,
        pagination: mockPagination
      });
      setChatPagination(mockPagination);
      setCurrentChatPage('chat_view');
      setChatPageNumber(page - 1);
    } finally {
      setLoadingChats(false);
    }
  };

  const handleChatPageChange = async (newPage: number) => {
    if (!chatViewData) return;
    
    const { cluster_id, subcluster_label } = chatViewData;
    await handleShowChats(cluster_id, subcluster_label, newPage);
  };

  const handleChatClick = (chat: any) => {
    // If clicking the same chat that's already selected, close the details
    if (selectedChat?._id === chat._id) {
      setSelectedChat(null);
    } else {
      // Otherwise, select the new chat
      setSelectedChat(chat);
      
      // Auto-scroll to make the details panel visible
      setTimeout(() => {
        const detailsPanel = document.querySelector('[data-chat-details]');
        if (detailsPanel) {
          // Get the header height to offset the scroll
          const header = document.querySelector('.sticky.top-0');
          const headerHeight = header ? header.getBoundingClientRect().height : 80;
          
          // Scroll to the details panel with proper offset
          const elementRect = detailsPanel.getBoundingClientRect();
          const absoluteElementTop = elementRect.top + window.pageYOffset;
          const offset = absoluteElementTop - headerHeight - 20; // 20px extra padding
          
          window.scrollTo({
            top: offset,
            behavior: 'smooth'
          });
        }
      }, 100);
    }
  };

  const getFilteredChats = () => {
    if (!chatViewData?.chats) return [];
    if (!searchTerm.trim()) return chatViewData.chats;
    
    return chatViewData.chats.filter(chat => {
      const searchLower = searchTerm.toLowerCase();
      
      // Search in member names
      const memberNames = chat.chat_members?.map((member: any) => 
        member?.display_name || member?.id || 'Unknown'
      ).join(' ') || '';
      
      // Search in other fields
      return (
        memberNames.toLowerCase().includes(searchLower) ||
        chat.dominant_cluster_label?.toLowerCase().includes(searchLower) ||
        chat.subcluster_label?.toLowerCase().includes(searchLower) ||
        chat.chat_id?.toLowerCase().includes(searchLower) ||
        chat.sender_name?.toLowerCase().includes(searchLower)
      );
    });
  };

  const _SubtopicPill = ({ subtopic, onClick }: { subtopic: Subtopic; onClick: () => void }) => (
    <span
      onClick={onClick}
      className="inline-block bg-purple-600 text-white px-2 py-1 rounded-full text-xs cursor-pointer hover:bg-purple-700 transition-colors"
    >
      {subtopic.name} ({subtopic.frequency})
    </span>
  );

  // Prepare chart data for visualizations
  const getChartData = () => {
    if (!clusterData || !clusterData.dominant_clusters) return [];
    
    return clusterData.dominant_clusters
      .sort((a, b) => (b.document_count || 0) - (a.document_count || 0))
      .map(cluster => ({
        name: cluster.dominant_cluster_label,
        documents: cluster.document_count || 0,
        urgent: cluster.urgent_count || 0,
        urgentPercentage: cluster.urgent_percentage || 0
      }));
  };



  const _sortedData = [...dataSource].sort((a, b) => {
    return sortOrder === 'desc' ? b.frequency - a.frequency : a.frequency - b.frequency;
  });

  const _filteredSearchItems = getFilteredSearchItems();

  // Filter data based on selected topic
  const filteredData = selectedTopic !== 'Show all' 
    ? dataSource.filter(topic => topic.name === selectedTopic)
    : dataSource;

  // Sort filtered data
  const sortedFilteredData = [...filteredData].sort((a, b) => {
    let aValue, bValue;
    
    switch (sortColumn) {
      case 'No. of Chats':
        aValue = a.frequency;
        bValue = b.frequency;
        break;
      case 'No. of Urgent':
        aValue = a.urgent_count || 0;
        bValue = b.urgent_count || 0;
        break;
      case 'Urgent %':
        aValue = a.urgent_percentage || 0;
        bValue = b.urgent_percentage || 0;
        break;
      case 'No. of subclusters':
        aValue = a.subtopics.length;
        bValue = b.subtopics.length;
        break;
      default:
        aValue = a.frequency;
        bValue = b.frequency;
    }
    
    return sortAscending ? aValue - bValue : bValue - aValue;
  });

  // Pagination
  const totalPages = Math.ceil(sortedFilteredData.length / recordsPerPage);
  const startIndex = currentPage * recordsPerPage;
  const endIndex = startIndex + recordsPerPage;
  const _currentPageData = sortedFilteredData.slice(startIndex, endIndex);

  // Main content
  if (currentChatPage === 'chat_view') {
      return (
      <AuthGuard requireAuth={true}>
        <DataLayout>
          <section className="px-6">
            <div className="max-w-[95vw] mx-auto">
              {/* Header Section */}
              <div className="flex items-center justify-between">
                <button
                  onClick={() => setCurrentChatPage('home')}
                  className="flex items-center gap-2 text-pink-400 hover:text-pink-300 transition-colors"
                >
                  <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                  </svg>
                  Back to Analytics
                </button>
              </div>

                             <div className="flex flex-col w-full">
                 {/* Main Content */}
                 <div className="flex-1 flex flex-col">
                   {/* Toolbar */}
                   <div className="bg-gray-800 bg-opacity-80 border border-gray-700 rounded-lg shadow-lg p-6 mb-6 flex items-center justify-between">
                     <div className="flex items-center gap-6">
                       <div className="relative min-w-[300px]">
                         <input
                           type="text"
                           placeholder="Search chats..."
                           value={searchTerm}
                           onChange={(e) => setSearchTerm(e.target.value)}
                           className="w-full pl-4 pr-4 py-3 bg-gray-700 text-gray-200 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-pink-500 text-sm"
                         />
                       </div>
                     </div>
                     <div className="text-sm text-gray-300">
                       Showing {chatPagination?.page_document_count || 0} of {chatPagination?.total_documents || 0} chats
                     </div>
                   </div>

                  {/* Content Area */}
                  <div className="flex-1 flex overflow-hidden min-h-[600px]">
                    {/* Chat List */}
                    <div className="w-full bg-gray-800 bg-opacity-50 overflow-y-auto">
                      {loadingChats ? (
                        <div className="p-4 text-center text-gray-300">
                          <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-500 mx-auto mb-2"></div>
                          Loading chats...
                        </div>
                      ) : getFilteredChats().length === 0 ? (
                        <div className="p-4 text-center text-gray-300">
                          {searchTerm ? 'No chats match your search criteria.' : 'No chats found for this selection.'}
                        </div>
                      ) : (
                        <>
                          {/* Column Header */}
                          <div className="sticky top-0 bg-gray-900 bg-opacity-95 border-b border-gray-700 p-4 z-10">
                            <div className="flex items-center justify-between">
                              <div className="flex-1">
                                <h3 className="text-sm font-semibold text-gray-200">Members</h3>
                              </div>
                            </div>
                          </div>
                          {getFilteredChats().map((chat: any) => (
                            <div key={chat._id} className="flex">
                              {/* Chat List Item */}
                              <div
                                onClick={() => handleChatClick(chat)}
                                className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer flex-1 ${
                                  selectedChat?._id === chat._id ? 'bg-gray-700' : ''
                                }`}
                              >
                                <div className="flex items-center justify-between">
                                  <div className="flex-1">
                                    <div className="font-medium text-white text-base">
                                      {chat.chat_members?.slice(0, 2).map((member: any) => member?.display_name || member?.id || 'Unknown').join(', ') || 
                                       chat.sender_name || 'No Members'}
                                    </div>
                                  </div>
                                  <div className="flex items-center gap-2">
                                    {chat.urgency && (
                                      <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                        Urgent
                                      </span>
                                    )}
                                  </div>
                                </div>
                              </div>
                              
                              {/* Chat Details - appears right next to selected item */}
                              {selectedChat?._id === chat._id && (
                                <div data-chat-details className="w-2/3 bg-gray-800 bg-opacity-80 border-l border-gray-700 p-6">
                                  <div className="flex justify-between items-center mb-4">
                                    <h2 className="text-xl font-semibold text-white">Chat ID: {selectedChat.chat_id || selectedChat._id}</h2>
                                    <button
                                      onClick={() => setSelectedChat(null)}
                                      className="p-1 hover:bg-gray-700 rounded-full"
                                    >
                                      <svg className="w-5 h-5 text-gray-300" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M6 18L18 6M6 6l12 12" />
                                      </svg>
                                    </button>
                                  </div>
                                  <div className="border-b border-gray-600 pb-6 mb-6">
                                    <div className="text-base text-gray-300 mb-2">Total Messages: {selectedChat.total_messages || 'N/A'}</div>
                                    <div className="text-base text-gray-300">Members: {selectedChat.chat_members?.map((member: any) => member?.display_name || member?.id || 'Unknown').join(', ') || 'N/A'}</div>
                                  </div>
                                  <div className="space-y-6">
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Messages</label>
                                      <div className="mt-1 p-6 bg-gray-900 rounded-lg text-gray-200 max-h-[400px] overflow-y-auto">
                                        {selectedChat.raw_segments && selectedChat.raw_segments.length > 0 ? (
                                          <div className="space-y-2">
                                            {selectedChat.raw_segments.map((segment: any, index: number) => {
                                              // Get sender name and clean it
                                              const rawSenderName = segment.sender_name || segment.sender || segment.speaker || 'Unknown';
                                              const senderName = rawSenderName.trim();
                                              
                                              // Create a more reliable way to determine current user
                                              const firstSender = selectedChat.raw_segments[0]?.sender_name || selectedChat.raw_segments[0]?.sender || selectedChat.raw_segments[0]?.speaker || 'Unknown';
                                              const isCurrentUser = senderName !== firstSender;
                                              
                                              let messageText = segment.text || segment.message || segment.content || 'No message content';
                                              const timestamp = segment.timestamp || segment.time || 'No timestamp';
                                              
                                              // Clean up message text - remove sender name if it's embedded in the message
                                              if (messageText.includes(senderName)) {
                                                messageText = messageText.replace(new RegExp(`^${senderName}\\s*\\n?`, 'i'), '').trim();
                                              }
                                              
                                              // Also clean up any remaining newlines and extra spaces
                                              messageText = messageText.replace(/\n+/g, ' ').trim();
                                              
                                              return (
                                                <div key={index} className={`flex ${isCurrentUser ? 'justify-end' : 'justify-start'} mb-2`}>
                                                  <div className={`max-w-[70%] px-4 py-3 rounded-2xl ${
                                                    isCurrentUser 
                                                      ? 'bg-green-500 text-white rounded-br-sm' 
                                                      : 'bg-gray-600 text-gray-200 rounded-bl-sm'
                                                  }`}>
                                                    <div className={`text-xs font-medium mb-1 ${
                                                      isCurrentUser ? 'text-green-100' : 'text-green-400'
                                                    }`}>
                                                      {senderName}
                                                    </div>
                                                    <div className={`text-sm leading-relaxed ${
                                                      isCurrentUser ? 'text-white' : 'text-gray-200'
                                                    }`}>
                                                      {messageText}
                                                    </div>
                                                    <div className={`text-xs mt-1 ${
                                                      isCurrentUser ? 'text-green-100' : 'text-gray-400'
                                                    }`}>
                                                      {new Date(timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })}
                                                    </div>
                                                  </div>
                                                </div>
                                              );
                                            })}
                                          </div>
                                        ) : (
                                          <div className="text-gray-400 text-center py-8">No raw segments available</div>
                                        )}
                                      </div>
                                    </div>
                                    <div className="grid grid-cols-2 gap-6">
                                      <div>
                                        <label className="text-base font-medium text-gray-300 mb-2">Dominant Cluster Label</label>
                                        <div className="mt-1 text-pink-400 font-medium text-lg">{selectedChat.dominant_cluster_label || 'N/A'}</div>
                                      </div>
                                      <div>
                                        <label className="text-base font-medium text-gray-300 mb-2">Subcluster Label</label>
                                        <div className="mt-1 text-purple-400 font-medium text-lg">{selectedChat.subcluster_label || 'N/A'}</div>
                                      </div>
                                    </div>
                                    <div className="grid grid-cols-2 gap-6">
                                      <div>
                                        <label className="text-base font-medium text-gray-300 mb-2">Dominant Topic</label>
                                        <div className="mt-1 text-gray-200 text-lg">{selectedChat.dominant_topic || 'N/A'}</div>
                                      </div>
                                      <div>
                                        <label className="text-base font-medium text-gray-300 mb-2">Subtopics</label>
                                        <div className="mt-1 text-gray-200 text-lg">{selectedChat.subtopics || 'N/A'}</div>
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              )}
                            </div>
                          ))}
                          {chatPagination && chatPagination.total_pages > 1 && (
                            <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                              <button
                                onClick={() => handleChatPageChange(chatPagination.current_page - 1)}
                                disabled={!chatPagination.has_previous}
                                className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                              >
                                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15 19l-7-7 7-7" />
                                </svg>
                                Previous
                              </button>
                              <div className="text-sm text-gray-300">
                                Page {chatPagination.current_page} of {chatPagination.total_pages} • Showing {chatPagination.page_document_count} of {chatPagination.total_documents} chats
                              </div>
                              <button
                                onClick={() => handleChatPageChange(chatPagination.current_page + 1)}
                                disabled={!chatPagination.has_next}
                                className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                              >
                                Next
                                <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                                </svg>
                              </button>
                            </div>
                          )}
                        </>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </section>
        </DataLayout>
      </AuthGuard>
    );
  }

  return (
    <AuthGuard requireAuth={true}>
      <DataLayout>
        <section className="py-4 px-4">
          <div className="max-w-7xl mx-auto">
            {/* Header Section */}
            <div className="text-center mb-6">
              <h1 className="text-4xl md:text-6xl font-bold mb-4 bg-gradient-to-r from-pink-400 to-purple-400 bg-clip-text text-transparent">
                Chat Dashboard
              </h1>
              <p className="text-xl text-gray-300 max-w-4xl mx-auto mb-8">
                Interactively analyze banking chat data with advanced visualizations and topic modeling insights.
              </p>
            </div>

            {/* Basic Statistics Section */}
            <StatisticsCards 
              statistics={statistics}
              loadingStats={loadingStats}
              expandedStats={expandedStats}
              onToggleExpanded={() => setExpandedStats(!expandedStats)}
              dataType="chat"
            />

            {/* Dominant Topics Visualization */}
            <div className="mb-8">
              <div className="flex items-center justify-between mb-6">
                <h2 className="text-2xl font-bold text-white flex items-center gap-2">
                  <BarChart3 className="w-6 h-6" />
                  Dominant Topics Visualization
                </h2>
                
                <VisualizationSelector 
                  selectedViz={selectedViz}
                  onVizChange={setSelectedViz}
                />
                  </div>

              <ChartComponents 
                type={selectedViz}
                data={getChartData()}
                loading={loadingClusters}
                title="Dominant Clusters Visualization"
                description="Document count by dominant clusters. Hover over elements for detailed metadata."
                onWordClick={handleWordClick}
              />
        </div>

            {/* Dominant Cluster Topics Analysis */}
            <div className="mb-8">
              <h2 className="text-2xl font-bold text-white flex items-center gap-2 mb-6">
                <Layers className="w-6 h-6" />
                Dominant Cluster Topics Analysis
              </h2>
              
              <DataTable 
                data={sortedFilteredData}
                loading={loadingClusters}
                selectedTopic={selectedTopic}
                onTopicChange={setSelectedTopic}
                sortColumn={sortColumn}
                sortAscending={sortAscending}
                onSortColumn={handleSortColumn}
                currentPage={currentPage}
                totalPages={totalPages}
                onPageChange={setCurrentPage}
                topicToggles={topicToggles}
                onToggleTopic={toggleTopicKeyphrases}
                onShowSubtopicViz={setSelectedTopicForSubtopicViz}
                onShowEmails={handleShowChats}
                selectedTopicForSubtopicViz={selectedTopicForSubtopicViz}
                selectedSubtopicViz={selectedSubtopicViz}
                onSubtopicVizChange={setSelectedSubtopicViz}
                dataType="chat"
              />


            </div>
          </div>
        </section>
      </DataLayout>
    </AuthGuard>
  );
};

export default ChatHomePage; 