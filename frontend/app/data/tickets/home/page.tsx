"use client"

import React, { useState, useEffect, useRef } from 'react';
import { BarChart3, Layers, ArrowLeft, ArrowRight, X } from 'lucide-react';
import DataLayout from '../../layout';
import { fetchStatistics, Statistics, fetchClusterData, ClusterData, fetchTopicAnalysisDocuments, DocumentResponse } from '@/lib/api';
import { 
  StatisticsCards, 
  VisualizationSelector, 
  ChartComponents, 
  DataTable, 
  EmailView 
} from '@/components/Visualizations';



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
  keyphrases?: string[]; // Add keyphrases field
}

interface SubclusterData {
  subcluster_label: string;
  email_count: number;
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

const TicketsHomePage = () => {
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
  const [sortColumn, setSortColumn] = useState<string>('No. of Emails');
  const [sortAscending, setSortAscending] = useState<boolean>(false);
  const [currentPage, setCurrentPage] = useState<number>(0);
  const [selectedTopic, setSelectedTopic] = useState<string>('Show all');
  const [subclusterData, setSubclusterData] = useState<{[key: string]: SubclusterData[]}>({});
  const [emailViewData, setEmailViewData] = useState<any>(null);
  const [currentEmailPage, setCurrentEmailPage] = useState<'home' | 'email_view'>('home');
  const [emailPageNumber, setEmailPageNumber] = useState<number>(0);
  const [emailPagination, setEmailPagination] = useState<any>(null);
  const [loadingEmails, setLoadingEmails] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);
  const [tooltip, setTooltip] = useState<TooltipData>({ x: 0, y: 0, data: null });
  const [selectedTopicForSubtopicViz, setSelectedTopicForSubtopicViz] = useState<string | null>(null);
  const [selectedSubtopicViz, setSelectedSubtopicViz] = useState('WordCloud');
  const [selectedEmail, setSelectedEmail] = useState<DocumentResponse | null>(null);

  const recordsPerPage = 10;

  // Handle ticket selection with toggle functionality
  const handleTicketSelect = (ticket: any) => {
    // If clicking the same ticket that's already selected, close the details
    if (selectedEmail?._id === ticket._id) {
      setSelectedEmail(null);
    } else {
      // Otherwise, select the new ticket
      setSelectedEmail(ticket);
      
      // Auto-scroll to make the details panel visible
      setTimeout(() => {
        const detailsPanel = document.querySelector('[data-ticket-details]');
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

  // Scroll to top only when switching to email view from home page
  useEffect(() => {
    if (currentEmailPage === 'email_view') {
      // Small delay to ensure the view has rendered
      setTimeout(() => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }, 100);
    }
  }, [currentEmailPage]);

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
        const response = await fetchStatistics('ticket', 'banking');
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
        const response = await fetchClusterData('ticket', 'banking');
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
          
          console.log('Transformed data:', transformedData);
          console.log('First topic keyphrases:', transformedData[0]?.keyphrases);
          
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

  const handleSearchItemSelect = (topicName: string) => {
    setSearchTerm(topicName);
    setIsSearchFocused(false);
  };

  const handleVizChange = (viz: string) => setSelectedViz(viz);
  const toggleSortOrder = () => setSortOrder(prev => prev === 'desc' ? 'asc' : 'desc');
  const toggleRowExpansion = (id: string) => {
    setExpandedRows(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const handleDominantClusterSelect = (clusterId: number) => {
    setSelectedDominantCluster(selectedDominantCluster === clusterId ? null : clusterId);
  };

  const toggleTopicKeyphrases = (topicLabel: string) => {
    setTopicToggles(prev => ({
      ...prev,
      [topicLabel]: !prev[topicLabel]
    }));
  };

  const handleShowSubtopicVisualization = (topicName: string) => {
    setSelectedTopicForSubtopicViz(topicName);
  };

  const handleSubtopicVizChange = (viz: string) => setSelectedSubtopicViz(viz);

  const handleSortColumn = (column: string) => {
    if (sortColumn === column) {
      setSortAscending(!sortAscending);
    } else {
      setSortColumn(column);
      setSortAscending(true);
    }
    setCurrentPage(0);
  };

  const handleShowEmails = async (clusterId: number, subclusterId: string, page: number = 1) => {
    try {
      setLoadingEmails(true);
      
      console.log('handleShowEmails called with:', { clusterId, subclusterId, page });
      
      const response = await fetchTopicAnalysisDocuments(
        'ticket',
        clusterId,
        subclusterId,
        page, // Use the page parameter
        30, // pageSize
        'banking'
      );
      
      console.log('API Response:', response);

      if (response.status === 'success') {
        setEmailViewData({
          cluster_id: clusterId,
          subcluster_label: subclusterId,
          emails: response.documents,
          total: response.pagination.total_documents,
          has_more: response.pagination.has_next,
          pagination: response.pagination
        });
        setEmailPagination(response.pagination);
        setCurrentEmailPage('email_view');
        setEmailPageNumber(page - 1); // Convert to 0-based index
      } else {
        console.error('Failed to fetch emails:', response);
        // Fallback to mock data if API fails
    const mockEmails = [
      {
        _id: '1',
        sender_name: 'John Doe',
        subject: 'Account Issue Resolution',
        message_text: 'This is a sample email content for demonstration purposes.',
        receiver_names: ['support@bank.com'],
            urgency: true,
        dominant_topic: 'Account Issues',
        subtopics: ['Login Problems', 'Password Reset'],
            dominant_cluster_label: 'Account Issues',
            subcluster_label: subclusterId
      },
      {
        _id: '2',
        sender_name: 'Jane Smith',
        subject: 'Payment Processing Query',
        message_text: 'Another sample email content for demonstration.',
        receiver_names: ['payments@bank.com'],
            urgency: false,
        dominant_topic: 'Payment Problems',
        subtopics: ['Failed Transactions'],
            dominant_cluster_label: 'Payment Problems',
            subcluster_label: subclusterId
          }
        ];

        const mockPagination = {
          current_page: page,
          page_size: 10,
          total_documents: mockEmails.length,
          total_pages: 1,
          filtered_count: mockEmails.length,
          has_next: false,
          has_previous: false,
          page_document_count: mockEmails.length,
        };

    setEmailViewData({
      cluster_id: clusterId,
          subcluster_label: subclusterId,
      emails: mockEmails,
      total: mockEmails.length,
          has_more: false,
          pagination: mockPagination
    });
        setEmailPagination(mockPagination);
    setCurrentEmailPage('email_view');
        setEmailPageNumber(page - 1);
      }
    } catch (error) {
      console.error('Error fetching emails:', error);
      // Fallback to mock data
      const mockEmails = [
        {
          _id: '1',
          sender_name: 'John Doe',
          subject: 'Account Issue Resolution',
          message_text: 'This is a sample email content for demonstration purposes.',
          receiver_names: ['support@bank.com'],
          urgency: true,
          dominant_topic: 'Account Issues',
          subtopics: ['Login Problems', 'Password Reset'],
          dominant_cluster_label: 'Account Issues',
          subcluster_label: subclusterId
        },
        {
          _id: '2',
          sender_name: 'Jane Smith',
          subject: 'Payment Processing Query',
          message_text: 'Another sample email content for demonstration.',
          receiver_names: ['payments@bank.com'],
          urgency: false,
          dominant_topic: 'Payment Problems',
          subtopics: ['Failed Transactions'],
          dominant_cluster_label: 'Payment Problems',
          subcluster_label: subclusterId
        }
      ];

      const mockPagination = {
        current_page: page,
        page_size: 10,
        total_documents: mockEmails.length,
        total_pages: 1,
        filtered_count: mockEmails.length,
        has_next: false,
        has_previous: false,
        page_document_count: mockEmails.length,
      };

      setEmailViewData({
        cluster_id: clusterId,
        subcluster_label: subclusterId,
        emails: mockEmails,
        total: mockEmails.length,
        has_more: false,
        pagination: mockPagination
      });
      setEmailPagination(mockPagination);
      setCurrentEmailPage('email_view');
      setEmailPageNumber(page - 1);
    } finally {
      setLoadingEmails(false);
    }
  };

  const handleEmailPageChange = async (newPage: number) => {
    if (!emailViewData) return;
    
    const { cluster_id, subcluster_label } = emailViewData;
    await handleShowEmails(cluster_id, subcluster_label, newPage);
  };

  const SubtopicPill = ({ subtopic, onClick }: { subtopic: Subtopic; onClick: () => void }) => (
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





  const sortedData = [...dataSource].sort((a, b) => {
    return sortOrder === 'desc' ? b.frequency - a.frequency : a.frequency - b.frequency;
  });

  const filteredSearchItems = getFilteredSearchItems();

  // Filter data based on selected topic
  const filteredData = selectedTopic !== 'Show all' 
    ? dataSource.filter(topic => topic.name === selectedTopic)
    : dataSource;

  // Sort filtered data
  const sortedFilteredData = [...filteredData].sort((a, b) => {
    let aValue, bValue;
    
    switch (sortColumn) {
      case 'No. of Emails':
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
  const currentPageData = sortedFilteredData.slice(startIndex, endIndex);

  // Email view component
  const EmailViewPage = () => {
    if (!emailViewData) return null;

    const { emails: emailList } = emailViewData;

    return (
      <div className="max-w-7xl mx-auto">
        {/* Header with back button */}
        <div className="flex items-center justify-between mb-6">
          <button
            onClick={() => setCurrentEmailPage('home')}
            className="flex items-center gap-2 text-pink-400 hover:text-pink-300 transition-colors"
          >
            <ArrowLeft className="w-4 h-4" />
            Back to Analytics
          </button>
          <h1 className="text-2xl font-bold text-white">
            Tickets
          </h1>
        </div>

        {/* Loading state */}
        {loadingEmails && (
          <div className="bg-gray-800 rounded-lg p-6 text-center">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-400 mx-auto mb-4"></div>
            <p className="text-gray-300">Loading tickets...</p>
        </div>
        )}

        {/* Main Content */}
        {!loadingEmails && (
          <div className="flex-1 flex flex-col">
            {/* Toolbar */}
            <div className="bg-gray-800 bg-opacity-80 border-b border-gray-700 p-4 flex items-center justify-between mb-4">
              <div className="text-sm text-gray-300">
                Showing {emailPagination?.page_document_count || 0} of {emailPagination?.total_documents || 0} tickets
                </div>
                </div>

            {/* Content Area */}
            <div className="flex-1 flex overflow-hidden">
              {/* Email List */}
              <div className={`${selectedEmail ? 'w-1/3' : 'w-full'} border-r border-gray-700 bg-gray-800 bg-opacity-50 overflow-y-auto`}>
                {emailList.length === 0 ? (
                  <div className="p-4 text-center text-gray-300">
                    No tickets found for this selection.
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
                            <div className="font-medium text-white text-lg mb-1">{email.title || email.subject || 'No Title'}</div>
                            <div className="text-xs text-gray-300">{email.ticket_number || email.sender_name || 'No Ticket Number'}</div>
                          </div>
                          <div className="flex items-center gap-2">
                            {email.urgency && (
                              <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                Urgent
                              </span>
                            )}
                          </div>
                        </div>
                      </div>
                    ))}
                    {emailPagination && emailPagination.total_pages > 1 && (
                      <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                        <div className="flex items-center gap-2">
          <button
                            onClick={() => handleEmailPageChange(emailPagination.current_page - 1)}
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
                            onClick={() => handleEmailPageChange(emailPagination.current_page + 1)}
                            disabled={!emailPagination.has_next}
                            className="flex items-center gap-2 px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                          >
                            Next
            <ArrowRight className="w-4 h-4" />
          </button>
        </div>
                        <div className="text-sm text-gray-400">
                          Page {emailPagination.current_page} of {emailPagination.total_pages} • Showing {emailPagination.page_document_count} of {emailPagination.total_documents} tickets
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
                    <div className="text-sm text-gray-300">Ticket Number: {selectedEmail.ticket_number || 'N/A'}</div>
                    <div className="text-sm text-gray-300">Priority: {selectedEmail.ticket_priority || selectedEmail.priority || 'N/A'}</div>
                    <div className="text-sm text-gray-300">Created: {selectedEmail.ticket_created_at || selectedEmail.timestamp || 'N/A'}</div>
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

  // Main content
  if (currentEmailPage === 'email_view') {
    return (
      <DataLayout>
        <section className="px-6">
          <div className="max-w-[95vw] mx-auto">
            {/* Header Section */}
            <div className="flex items-center justify-between">
              <button
                onClick={() => setCurrentEmailPage('home')}
                className="flex items-center gap-2 text-pink-400 hover:text-pink-300 transition-colors"
              >
                <ArrowLeft className="w-4 h-4" />
                Back to Analytics
              </button>
              <div></div>
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
                        placeholder="Search tickets..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="w-full pl-4 pr-4 py-3 bg-gray-700 text-gray-200 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-pink-500 text-sm"
                      />
                    </div>
                  </div>
                  <div className="text-sm text-gray-300">
                    Showing {emailPagination?.page_document_count || 0} of {emailPagination?.total_documents || 0} tickets
                  </div>
                </div>

                {/* Content Area */}
                <div className="flex-1 flex overflow-hidden min-h-[600px]">
                  {/* Ticket List */}
                  <div className="w-full bg-gray-800 bg-opacity-50 overflow-y-auto">
                    {loadingEmails ? (
                      <div className="p-4 text-center text-gray-300">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-500 mx-auto mb-2"></div>
                        Loading tickets...
                      </div>
                    ) : emailViewData?.emails?.length === 0 ? (
                      <div className="p-4 text-center text-gray-300">
                        No tickets found for this selection.
                      </div>
                    ) : (
                      <>
                        {/* Column Header */}
                        <div className="sticky top-0 bg-gray-900 bg-opacity-95 border-b border-gray-700 p-4 z-10">
                          <div className="flex items-center justify-between">
                            <div className="flex-1">
                              <h3 className="text-sm font-semibold text-gray-200">Tickets</h3>
                            </div>
                          </div>
                        </div>
                        {emailViewData?.emails?.map((ticket: DocumentResponse) => (
                          <div key={ticket._id} className="flex">
                            {/* Ticket List Item */}
                            <div
                              onClick={() => handleTicketSelect(ticket)}
                              className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer flex-1 ${
                                selectedEmail?._id === ticket._id ? 'bg-gray-700' : ''
                              }`}
                            >
                              <div className="flex items-center justify-between">
                                <div className="flex-1">
                                  <div className="font-medium text-white text-lg mb-1">
                                    {ticket.title || 'No Title'}
                                  </div>
                                  <div className="text-xs text-gray-300">
                                    {ticket.ticket_number || 'No Ticket Number'}
                                  </div>
                                </div>
                                <div className="flex items-center gap-2">
                                  {ticket.urgency && (
                                    <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                      Urgent
                                    </span>
                                  )}
                                </div>
                              </div>
                            </div>
                            
                            {/* Ticket Details - appears right next to selected item */}
                            {selectedEmail?._id === ticket._id && (
                              <div data-ticket-details className="w-2/3 bg-gray-800 bg-opacity-80 border-l border-gray-700 p-6">
                                <div className="flex justify-between items-center mb-4">
                                  <h2 className="text-xl font-semibold text-white">{selectedEmail.title || selectedEmail.subject || 'No Title'}</h2>
                                  <button
                                    onClick={() => setSelectedEmail(null)}
                                    className="p-1 hover:bg-gray-700 rounded-full"
                                  >
                                    <X className="w-5 h-5 text-gray-300" />
                                  </button>
                                </div>
                                <div className="border-b border-gray-600 pb-6 mb-6">
                                  <div className="text-base text-gray-300 mb-2">Ticket Number: {selectedEmail.ticket_number || 'N/A'}</div>
                                  <div className="text-base text-gray-300">Priority: {selectedEmail.ticket_priority || selectedEmail.priority || 'N/A'}</div>
                                </div>
                                <div className="space-y-6">
                                  <div>
                                    <label className="text-sm font-medium text-gray-300">Description</label>
                                    <div className="mt-1 p-6 bg-gray-900 rounded-lg text-gray-200 max-h-[400px] overflow-y-auto">
                                      {selectedEmail.description || selectedEmail.message_text || 'No description available'}
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-2 gap-6">
                                    <div>
                                      <label className="text-base font-medium text-gray-300 mb-2">Dominant Cluster Label</label>
                                      <div className="mt-1 text-pink-400 font-medium text-lg">{selectedEmail.dominant_cluster_label || 'N/A'}</div>
                                    </div>
                                    <div>
                                      <label className="text-base font-medium text-gray-300 mb-2">Subcluster Label</label>
                                      <div className="mt-1 text-purple-400 font-medium text-lg">{selectedEmail.subcluster_label || 'N/A'}</div>
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-2 gap-6">
                                    <div>
                                      <label className="text-base font-medium text-gray-300 mb-2">Dominant Topic</label>
                                      <div className="mt-1 text-gray-200 text-lg">{selectedEmail.dominant_topic || 'N/A'}</div>
                                    </div>
                                    <div>
                                      <label className="text-base font-medium text-gray-300 mb-2">Subtopics</label>
                                      <div className="mt-1 text-gray-200 text-lg">{selectedEmail.subtopics || 'N/A'}</div>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        ))}
                        {emailPagination && emailPagination.total_pages > 1 && (
                          <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                            <button
                              onClick={() => handleEmailPageChange(emailPagination.current_page - 1)}
                              disabled={!emailPagination.has_previous}
                              className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              <ArrowLeft className="w-4 h-4" />
                              Previous
                            </button>
                            <div className="text-sm text-gray-300">
                              Page {emailPagination.current_page} of {emailPagination.total_pages} • Showing {emailPagination.page_document_count} of {emailPagination.total_documents} tickets
                            </div>
                            <button
                              onClick={() => handleEmailPageChange(emailPagination.current_page + 1)}
                              disabled={!emailPagination.has_next}
                              className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              Next
                              <ArrowRight className="w-4 h-4" />
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
    );
  }

  return (
    <DataLayout>
      <section className="py-12 px-4">
        <div className="max-w-7xl mx-auto">
          {/* Header Section */}
          <div className="text-center mb-12">
            <h1 className="text-4xl md:text-6xl font-bold mb-4 bg-gradient-to-r from-pink-400 to-purple-400 bg-clip-text text-transparent">
              Tickets Dashboard
            </h1>
            <p className="text-xl text-gray-300 max-w-4xl mx-auto mb-8">
              Interactively analyze banking trouble ticket data with advanced visualizations and topic modeling insights.
            </p>
          </div>

          {/* Basic Statistics Section */}
          <StatisticsCards 
            statistics={statistics}
            loadingStats={loadingStats}
            expandedStats={expandedStats}
            onToggleExpanded={() => setExpandedStats(!expandedStats)}
            dataType="ticket"
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
              onShowEmails={handleShowEmails}
              selectedTopicForSubtopicViz={selectedTopicForSubtopicViz}
              selectedSubtopicViz={selectedSubtopicViz}
              onSubtopicVizChange={setSelectedSubtopicViz}
              dataType="ticket"
            />


          </div>
        </div>
      </section>
    </DataLayout>
  );
};

export default TicketsHomePage; 