"use client";

import React, { useState, useRef, useEffect } from 'react';
import { ChevronDown, ChevronLeft, ChevronRight, X, Search } from 'lucide-react';
import { Header } from '@/components/Header/Header';
import Sidebar from '@/components/Sidebar/Sidebar';
import { fetchClusterOptions, fetchTopicAnalysisDocuments, DominantCluster, Subcluster, DocumentResponse } from '@/lib/api';

type Ticket = DocumentResponse;

const TicketsTopicAnalysis = () => {
  const [selectedDominantTopics, setSelectedDominantTopics] = useState<number[]>([]);
  const [selectedSubtopics, setSelectedSubtopics] = useState<string[]>([]);
  const [dominantDropdownOpen, setDominantDropdownOpen] = useState(false);
  const [subtopicDropdownOpen, setSubtopicDropdownOpen] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [selectedTicket, setSelectedTicket] = useState<Ticket | null>(null);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadingDocuments, setLoadingDocuments] = useState(false);
  const [dominantClusters, setDominantClusters] = useState<DominantCluster[]>([]);
  const [subclusters, setSubclusters] = useState<Subcluster[]>([]);
  const [tickets, setTickets] = useState<Ticket[]>([]);
  const [pagination, setPagination] = useState({
    current_page: 1,
    page_size: 30,
    total_documents: 0,
    total_pages: 0,
    filtered_count: 0,
    has_next: false,
    has_previous: false,
    page_document_count: 0,
  });
  const [searchTerm, setSearchTerm] = useState('');
  
  // Cache for storing page data and pagination
  const [pageCache, setPageCache] = useState<Map<number, { documents: Ticket[], pagination: any }>>(new Map());
  const [cacheLoading, setCacheLoading] = useState<Set<number>>(new Set());

  const dominantDropdownRef = useRef<HTMLDivElement>(null);
  const subtopicDropdownRef = useRef<HTMLDivElement>(null);

  // Load cluster options on component mount
  useEffect(() => {
    const loadClusterOptions = async () => {
      try {
        setLoading(true);
        const response = await fetchClusterOptions('ticket', 'banking');
        if (response.status === 'success') {
          setDominantClusters(response.dominant_clusters);
          setSubclusters(response.subclusters);
      } else {
          console.error('Failed to fetch cluster options:', response);
      }
    } catch (error) {
        console.error('Error loading cluster options:', error);
    } finally {
      setLoading(false);
    }
  };

    loadClusterOptions();
  }, []);

    // Load documents when filters change
  useEffect(() => {
    console.log(`useEffect triggered - selectedDominantTopics:`, selectedDominantTopics, `selectedSubtopics:`, selectedSubtopics, `currentPage:`, currentPage);
    const loadDocuments = async () => {
      if (selectedDominantTopics.length === 0) {
        setTickets([]);
        setPagination({
          current_page: 1,
          page_size: 30,
          total_documents: 0,
          total_pages: 0,
          filtered_count: 0,
          has_next: false,
          has_previous: false,
          page_document_count: 0,
        });
        // Clear cache when no filters selected
        console.log(`Clearing cache - no filters selected`);
        setPageCache(new Map());
        setCacheLoading(new Set());
        return;
      }

      // Check if data is already cached for this page
      console.log(`Checking cache for page ${currentPage}, cache size: ${pageCache.size}`);
      console.log(`Cache keys:`, Array.from(pageCache.keys()));
      if (pageCache.has(currentPage)) {
        console.log(`Loading page ${currentPage} from cache`);
        const cachedData = pageCache.get(currentPage)!;
        console.log(`Cached data for page ${currentPage}:`, cachedData.documents.length, 'tickets');
        setTickets(cachedData.documents);
        setPagination(cachedData.pagination);
        console.log(`Updated pagination from cache:`, cachedData.pagination);
        setLoadingDocuments(false);
        return;
      }

      // Check if we're already loading this page
      if (cacheLoading.has(currentPage)) {
        console.log(`Page ${currentPage} is already being loaded`);
        return;
      }

      try {
        setLoadingDocuments(true);
        setCacheLoading(prev => new Set(prev).add(currentPage));
        
        const kmeansClusterId = selectedDominantTopics[0]; // Use first selected cluster
        const subclusterId = selectedSubtopics.length > 0 ? selectedSubtopics.join(',') : undefined;
        
        const response = await fetchTopicAnalysisDocuments(
          'ticket',
          kmeansClusterId,
          subclusterId,
          currentPage,
          30,
          'banking'
        );

        if (response.status === 'success') {
          console.log('API Response:', response);
          console.log('Documents received:', response.documents.length);
          console.log('Pagination info:', response.pagination);
          
          // Cache the data for this page
          setPageCache(prev => {
            const newCache = new Map(prev);
            newCache.set(currentPage, { documents: response.documents, pagination: response.pagination });
            console.log(`Cached page ${currentPage} with ${response.documents.length} tickets`);
            console.log(`Cache size after caching: ${newCache.size}`);
            console.log(`Cache keys after caching:`, Array.from(newCache.keys()));
            return newCache;
          });
          
          setTickets(response.documents);
          console.log(`Setting pagination:`, response.pagination);
          setPagination(response.pagination);
        } else {
          console.error('Failed to fetch documents:', response);
        }
      } catch (error) {
        console.error('Error loading documents:', error);
      } finally {
        setLoadingDocuments(false);
        setCacheLoading(prev => {
          const newSet = new Set(prev);
          newSet.delete(currentPage);
          return newSet;
        });
      }
    };

    loadDocuments();
  }, [selectedDominantTopics, selectedSubtopics, currentPage]);

  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (
        dominantDropdownOpen &&
        dominantDropdownRef.current &&
        !dominantDropdownRef.current.contains(event.target as Node)
      ) {
        setDominantDropdownOpen(false);
      }
      if (
        subtopicDropdownOpen &&
        subtopicDropdownRef.current &&
        !subtopicDropdownRef.current.contains(event.target as Node)
      ) {
        setSubtopicDropdownOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, [dominantDropdownOpen, subtopicDropdownOpen]);

  const getFilteredTickets = () => {
    if (!searchTerm) return tickets;
    
    return tickets.filter(ticket => {
      const searchLower = searchTerm.toLowerCase();
    return (
        ticket.ticket_number?.toLowerCase().includes(searchLower) ||
        ticket.title?.toLowerCase().includes(searchLower) ||
        ticket.description?.toLowerCase().includes(searchLower) ||
        ticket.dominant_cluster_label?.toLowerCase().includes(searchLower) ||
        ticket.subcluster_label?.toLowerCase().includes(searchLower)
      );
    });
  };

  const handleDominantTopicToggle = (clusterId: number) => {
    setSelectedDominantTopics(prev => 
      prev.includes(clusterId) 
        ? prev.filter(id => id !== clusterId)
        : [clusterId] // Only allow one selection at a time
    );
    setSelectedSubtopics([]); // Clear subtopics when dominant topic changes
    setCurrentPage(1); // Reset to first page
    // Clear cache when filters change
    console.log(`Clearing cache due to dominant topic toggle`);
    setPageCache(new Map());
    setCacheLoading(new Set());
  };

  const handleSubtopicToggle = (subtopicId: string) => {
    setSelectedSubtopics(prev => 
      prev.includes(subtopicId) 
        ? prev.filter(id => id !== subtopicId)
        : [...prev, subtopicId] // Allow multiple selections
    );
    setCurrentPage(1); // Reset to first page
    // Clear cache when filters change
    console.log(`Clearing cache due to subtopic toggle`);
    setPageCache(new Map());
    setCacheLoading(new Set());
  };

  const handleTicketClick = (ticket: Ticket) => {
    // If clicking the same ticket that's already selected, close the details
    if (selectedTicket?._id === ticket._id) {
      setSelectedTicket(null);
    } else {
      // Otherwise, select the new ticket
      setSelectedTicket(ticket);
      
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

  const clearAllFilters = () => {
    setSelectedDominantTopics([]);
    setSelectedSubtopics([]);
    setSearchTerm('');
    setCurrentPage(1);
    // Clear cache when filters are cleared
    console.log(`Clearing cache due to clear all filters`);
    setPageCache(new Map());
    setCacheLoading(new Set());
  };

  const handlePreviousPage = () => {
    console.log(`Previous button clicked. Current page: ${pagination.current_page}, has_previous: ${pagination.has_previous}`);
    if (pagination.has_previous) {
      const prevPage = pagination.current_page - 1;
      console.log(`Navigating to previous page: ${prevPage}`);
      setCurrentPage(prevPage);
    } else {
      console.log(`Cannot go to previous page - has_previous is false`);
    }
  };

  const handleNextPage = () => {
    console.log(`Next button clicked. Current page: ${pagination.current_page}, has_next: ${pagination.has_next}`);
    if (pagination.has_next) {
      const nextPage = pagination.current_page + 1;
      console.log(`Navigating to next page: ${nextPage}`);
      setCurrentPage(nextPage);
    } else {
      console.log(`Cannot go to next page - has_next is false`);
    }
  };

  const filteredTickets = getFilteredTickets();
  // Use the tickets directly from the API response since they're already paginated
  const paginatedTickets = filteredTickets;

  // Sidebar toggle logic
  const toggleSidebar = () => setIsSidebarOpen((prev) => !prev);
  const closeSidebar = () => setIsSidebarOpen(false);

  return (
    <div className="min-h-screen relative overflow-hidden">
      {/* Header */}
      <Header
        transparent={true}
        isLoggedIn={true}
        isSidebarOpen={isSidebarOpen}
        onToggleSidebar={toggleSidebar}
      />
      {/* Sidebar Drawer - z-40, below header */}
      <div className={`fixed inset-y-0 left-0 z-40 transform transition-transform duration-300 ease-in-out ${
        isSidebarOpen ? 'translate-x-0' : '-translate-x-full'
      }`}>
        <Sidebar onClose={closeSidebar} />
      </div>
      {/* Sidebar Overlay (z-30, below header) */}
      {isSidebarOpen && (
        <div className="fixed inset-0 z-30" onClick={closeSidebar} />
      )}
      {/* Background (z-0) */}
      <div
        className="fixed inset-0 z-0"
        style={{ background: 'linear-gradient(135deg, #0a0a0a 0%, #1a0a1a 50%, #0a0a1a 100%)' }}
      />
      <div
        className="fixed inset-0 z-10 pointer-events-none"
        style={{
          background: 'linear-gradient(135deg, rgba(185, 10, 189, 0.3) 0%, rgba(83, 50, 255, 0.3) 100%)',
          mixBlendMode: 'multiply',
        }}
      />
      {/* Main Content */}
      <div className={`relative z-20 transition-all duration-300 ${isSidebarOpen ? 'filter blur-sm' : ''} pt-[72px]`}>
        {/* Header Section */}
        <section className="pb-4 px-6">
          <div className="max-w-[95vw] mx-auto">
            <div className="text-center mb-4">
              <h2 className="text-4xl md:text-6xl font-bold mb-4 bg-gradient-to-r from-pink-400 to-purple-400 bg-clip-text text-transparent">
                Topic Analysis Dashboard
              </h2>
              <p className="text-xl text-gray-300 max-w-4xl mx-auto mb-8">
                Explore and filter tickets by topics and subtopics to gain actionable insights
              </p>
            </div>

            <div className="flex flex-col w-full">
              {/* Filter Bar */}
              <div className="sticky top-0 z-30 bg-gray-900 bg-opacity-95 border border-gray-700 rounded-lg shadow-lg p-4 mb-4 flex flex-wrap gap-4 items-center justify-between transition-all duration-300">
                {/* Dominant Topics Filter */}
                <div className="relative min-w-[220px]" ref={dominantDropdownRef}>
                  <label className="text-sm font-semibold text-gray-200 mb-1 block">Dominant Topics</label>
                  <button
                    aria-label="Select dominant topics"
                    onClick={() => setDominantDropdownOpen(!dominantDropdownOpen)}
                    className="w-full bg-gray-800 text-white rounded-lg p-3 border border-gray-600 flex items-center justify-between hover:bg-gray-700 focus:ring-2 focus:ring-pink-500 focus:outline-none transition-all"
                  >
                    <span className="text-sm">
                      {selectedDominantTopics.length > 0 
                        ? `${selectedDominantTopics.length} topic${selectedDominantTopics.length > 1 ? 's' : ''} selected`
                        : 'Select topics...'}
                    </span>
                    <ChevronDown className={`w-4 h-4 transition-transform ${dominantDropdownOpen ? 'rotate-180' : ''}`} />
                  </button>
                  {dominantDropdownOpen && (
                    <div className="absolute z-50 w-64 mt-1 bg-gray-800 border border-gray-600 rounded-lg shadow-xl max-h-60 overflow-y-auto animate-fade-in">
                      {loading ? (
                        <div className="p-4 text-gray-300">Loading clusters...</div>
                      ) : (
                        dominantClusters.map((cluster) => (
                          <div
                            key={cluster.kmeans_cluster_id}
                            onClick={() => handleDominantTopicToggle(cluster.kmeans_cluster_id)}
                            className={`p-3 cursor-pointer hover:bg-pink-600/20 flex items-center justify-between transition-all ${
                              selectedDominantTopics.includes(cluster.kmeans_cluster_id) ? 'bg-pink-600/30' : ''
                            }`}
                            tabIndex={0}
                            role="option"
                            aria-selected={selectedDominantTopics.includes(cluster.kmeans_cluster_id)}
                          >
                            <span className="text-gray-200 text-sm">{cluster.dominant_cluster_label}</span>
                            {selectedDominantTopics.includes(cluster.kmeans_cluster_id) && (
                              <div className="w-4 h-4 rounded-full bg-pink-500 flex items-center justify-center">
                                <span className="text-white text-xs">✓</span>
                              </div>
                            )}
                          </div>
                        ))
                      )}
                    </div>
                  )}
                </div>
                {/* Subtopics Filter */}
                <div className="relative min-w-[220px]" ref={subtopicDropdownRef}>
                  <label className="text-sm font-semibold text-gray-200 mb-1 block">Subtopics</label>
                  <button
                    aria-label="Select subtopics"
                    onClick={() => setSubtopicDropdownOpen(!subtopicDropdownOpen)}
                    disabled={selectedDominantTopics.length === 0}
                    className="w-full bg-gray-800 text-white rounded-lg p-3 border border-gray-600 flex items-center justify-between hover:bg-gray-700 focus:ring-2 focus:ring-purple-500 focus:outline-none transition-all disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    <span className="text-sm">
                      {selectedSubtopics.length > 0 
                        ? `${selectedSubtopics.length} subtopic${selectedSubtopics.length > 1 ? 's' : ''} selected`
                        : selectedDominantTopics.length === 0 
                          ? 'Select dominant topics first...'
                          : 'Select multiple subtopics...'}
                    </span>
                    <ChevronDown className={`w-4 h-4 transition-transform ${subtopicDropdownOpen ? 'rotate-180' : ''}`} />
                  </button>
                  {subtopicDropdownOpen && selectedDominantTopics.length > 0 && (
                    <div className="absolute z-50 w-64 mt-1 bg-gray-800 border border-gray-600 rounded-lg shadow-xl max-h-60 overflow-y-auto animate-fade-in">
                      {subclusters
                        .filter(sub => selectedDominantTopics.includes(sub.kmeans_cluster_id))
                        .map((subcluster) => (
                          <div
                            key={subcluster.subcluster_id}
                            onClick={() => handleSubtopicToggle(subcluster.subcluster_id)}
                            className={`p-3 cursor-pointer hover:bg-purple-600/20 flex items-center justify-between transition-all ${
                              selectedSubtopics.includes(subcluster.subcluster_id) ? 'bg-purple-600/30' : ''
                            }`}
                            tabIndex={0}
                            role="option"
                            aria-selected={selectedSubtopics.includes(subcluster.subcluster_id)}
                          >
                            <span className="text-gray-200 text-sm">{subcluster.subcluster_label}</span>
                            {selectedSubtopics.includes(subcluster.subcluster_id) && (
                              <div className="w-4 h-4 rounded-full bg-purple-500 flex items-center justify-center">
                                <span className="text-white text-xs">✓</span>
                              </div>
                            )}
                          </div>
          ))}
        </div>
                  )}
                </div>
                {/* Clear Filters Button */}
                <div className="flex-1 flex items-end justify-end">
                  <button
                    onClick={clearAllFilters}
                    className="bg-red-600 text-white rounded-lg p-3 hover:bg-red-700 text-sm focus:ring-2 focus:ring-red-400 focus:outline-none transition-all"
                  >
                    Clear Filters
                  </button>
      </div>
              </div>
              {/* Active Filters Summary Chips */}
              {(selectedDominantTopics.length > 0 || selectedSubtopics.length > 0) && (
                <div className="flex flex-wrap gap-2 mb-4">
                  {selectedDominantTopics.map((clusterId) => {
                    const cluster = dominantClusters.find(c => c.kmeans_cluster_id === clusterId);
                    return (
                      <span
                        key={clusterId}
                        className="bg-pink-600 text-white px-3 py-1 rounded-full text-xs flex items-center gap-1 shadow-sm animate-fade-in"
                      >
                        {cluster?.dominant_cluster_label || `Cluster ${clusterId}`}
                        <X 
                          className="w-3 h-3 cursor-pointer" 
                          onClick={() => handleDominantTopicToggle(clusterId)}
                          aria-label={`Remove filter ${cluster?.dominant_cluster_label}`}
                        />
                      </span>
                    );
                  })}
                  {selectedSubtopics.map((subtopicId) => {
                    const subtopic = subclusters.find(s => s.subcluster_id === subtopicId);
                    return (
                      <span
                        key={subtopicId}
                        className="bg-purple-600 text-white px-3 py-1 rounded-full text-xs flex items-center gap-1 shadow-sm animate-fade-in"
                      >
                        {subtopic?.subcluster_label || `Subtopic ${subtopicId}`}
                        <X 
                          className="w-3 h-3 cursor-pointer" 
                          onClick={() => handleSubtopicToggle(subtopicId)}
                          aria-label={`Remove filter ${subtopic?.subcluster_label}`}
                        />
                      </span>
                    );
                  })}
                </div>
              )}
              <hr className="border-gray-700 mb-4" />

              {/* Main Content */}
              <div className="flex-1 flex flex-col">
                {/* Toolbar */}
                <div className="bg-gray-800 bg-opacity-80 border-b border-gray-700 p-4 flex items-center justify-between">
                  <div className="flex items-center gap-4">
                    <div className="relative">
                      <Search className="w-5 h-5 text-gray-400 absolute left-3 top-1/2 transform -translate-y-1/2" />
                      <input
                        type="text"
                        placeholder="Search tickets..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="pl-10 pr-4 py-2 bg-gray-700 text-gray-200 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-pink-500 text-sm"
                      />
                    </div>
                  </div>
                                     <div className="text-sm text-gray-300">
                     Showing {pagination.page_document_count} of {pagination.total_documents} tickets
                     {pageCache.has(pagination.current_page)}
                   </div>
                </div>

                {/* Content Area */}
                <div className="flex-1 flex overflow-hidden">
                  {/* Ticket List */}
                  <div className="w-full bg-gray-800 bg-opacity-50 overflow-y-auto">
                    {loadingDocuments ? (
                      <div className="p-4 text-center text-gray-300">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-500 mx-auto mb-2"></div>
                        Loading tickets...
                      </div>
                                         ) : filteredTickets.length === 0 && !loadingDocuments ? (
                       <div className="p-4 text-center text-gray-300">
                         No tickets match the selected criteria. Try adjusting your filters.
                       </div>
                     ) : (
                      <>
                        {paginatedTickets.map((ticket) => (
                          <div key={ticket._id} className="flex">
                            {/* Ticket List Item */}
                            <div
                              onClick={() => handleTicketClick(ticket)}
                              className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer flex-1 ${
                                selectedTicket?._id === ticket._id ? 'bg-gray-700' : ''
                              }`}
                            >
                              <div className="flex items-center justify-between">
                                <div className="flex-1">
                                  <div className="font-medium text-white text-lg mb-1">{ticket.title || 'No Title'}</div>
                                  <div className="text-xs text-gray-300">{ticket.ticket_number || 'No Ticket Number'}</div>
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
                            {selectedTicket?._id === ticket._id && (
                              <div data-ticket-details className="w-2/3 bg-gray-800 bg-opacity-80 border-l border-gray-700 p-6">
                                <div className="flex justify-between items-center mb-4">
                                  <h2 className="text-xl font-semibold text-white">{selectedTicket.title || 'No Title'}</h2>
                                  <button
                                    onClick={() => setSelectedTicket(null)}
                                    className="p-1 hover:bg-gray-700 rounded-full"
                                  >
                                    <X className="w-5 h-5 text-gray-300" />
                                  </button>
                                </div>
                                <div className="border-b border-gray-600 pb-4 mb-4">
                                  <div className="text-sm text-gray-300">Ticket Number: {selectedTicket.ticket_number || 'N/A'}</div>
                                  <div className="text-sm text-gray-300">Priority: {selectedTicket.priority || 'N/A'}</div>
                                </div>
                                <div className="space-y-6">
                                  <div>
                                    <label className="text-sm font-medium text-gray-300">Description</label>
                                    <div className="mt-1 p-4 bg-gray-900 rounded-lg text-gray-200">
                                      {selectedTicket.description || 'No description available'}
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-2 gap-4">
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Dominant Cluster Label</label>
                                      <div className="mt-1 text-pink-400 font-medium">{selectedTicket.dominant_cluster_label || 'N/A'}</div>
                                    </div>
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Subcluster Label</label>
                                      <div className="mt-1 text-purple-400 font-medium">{selectedTicket.subcluster_label || 'N/A'}</div>
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-2 gap-4">
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Dominant Topic</label>
                                      <div className="mt-1 text-gray-200">{selectedTicket.dominant_topic || 'N/A'}</div>
                                    </div>
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Subtopics</label>
                                      <div className="mt-1 text-gray-200">{selectedTicket.subtopics || 'N/A'}</div>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        ))}
                        {pagination.total_pages > 1 && (
                          <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                            <div className="flex items-center gap-2">
                              <button
                                onClick={handlePreviousPage}
                                disabled={!pagination.has_previous}
                                className="flex items-center gap-2 px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                              >
                                <ChevronLeft className="w-4 h-4" />
                                Previous
                              </button>
                              <span className="text-sm text-gray-300 px-3">
                                Page {pagination.current_page} of {pagination.total_pages}
                              </span>
                              <button
                                onClick={handleNextPage}
                                disabled={!pagination.has_next}
                                className="flex items-center gap-2 px-3 py-2 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                              >
                                Next
                                <ChevronRight className="w-4 h-4" />
                              </button>
                            </div>
                                                         <div className="text-sm text-gray-400">
                               Page {pagination.current_page} of {pagination.total_pages} • Showing {pagination.page_document_count} of {pagination.total_documents} tickets
                             </div>
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
      </div>
    </div>
  );
};

export default TicketsTopicAnalysis; 