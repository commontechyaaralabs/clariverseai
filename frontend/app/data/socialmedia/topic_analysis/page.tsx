"use client";

import React, { useState, useRef, useEffect, useCallback } from 'react';
import { ChevronDown, ChevronLeft, ChevronRight, X, Search } from 'lucide-react';
import { Header } from '@/components/Header/Header';
import Sidebar from '@/components/Sidebar/Sidebar';
import { fetchClusterOptions, fetchTopicAnalysisDocuments, DominantCluster, Subcluster, DocumentResponse } from '@/lib/api';

type Tweet = DocumentResponse;

const TwitterTopicAnalysis = () => {
  const [selectedDominantTopics, setSelectedDominantTopics] = useState<number[]>([]);
  const [selectedSubtopics, setSelectedSubtopics] = useState<string[]>([]);
  const [dominantDropdownOpen, setDominantDropdownOpen] = useState(false);
  const [subtopicDropdownOpen, setSubtopicDropdownOpen] = useState(false);
  const [currentPage, setCurrentPage] = useState(1);
  const [selectedTweet, setSelectedTweet] = useState<Tweet | null>(null);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [loading, setLoading] = useState(true);
  const [loadingDocuments, setLoadingDocuments] = useState(false);
  const [dominantClusters, setDominantClusters] = useState<DominantCluster[]>([]);
  const [subclusters, setSubclusters] = useState<Subcluster[]>([]);
  const [tweets, setTweets] = useState<Tweet[]>([]);
  const [pagination, setPagination] = useState({
    current_page: 1,
    page_size: 10,
    total_documents: 0,
    total_pages: 0,
    filtered_count: 0,
    has_next: false,
    has_previous: false,
    page_document_count: 0,
  });
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedChannel, setSelectedChannel] = useState<string>('All');
  const [selectedPlatform, setSelectedPlatform] = useState<string>('All');
  
  // Cache for storing page data and pagination
  const [pageCache, setPageCache] = useState<Map<number, { documents: Tweet[], pagination: any }>>(new Map());
  const [cacheLoading, setCacheLoading] = useState<Set<number>>(new Set());
  // Store all unique documents for proper pagination
  const [allUniqueDocuments, setAllUniqueDocuments] = useState<Tweet[]>([]);
  const [isLoadingAllData, setIsLoadingAllData] = useState(false);

  const channelOptions = [
    { value: 'All', label: 'All' },
    { value: 'Trustpilot', label: 'Trustpilot' },
    { value: 'Twitter', label: 'Twitter' },
    { value: 'App Store/Google Play', label: 'App Store/Google Play' },
    { value: 'Reddit', label: 'Reddit' }
  ];

  const platformOptions = [
    { value: 'All', label: 'All Platforms' },
    { value: 'App Store', label: 'App Store' },
    { value: 'Google Play Store', label: 'Google Play Store' }
  ];

  const dominantDropdownRef = useRef<HTMLDivElement>(null);
  const subtopicDropdownRef = useRef<HTMLDivElement>(null);

  // Load cluster options on component mount and when channel changes
  useEffect(() => {
    const loadClusterOptions = async () => {
      try {
        setLoading(true);
        console.log(`Loading cluster options for channel: ${selectedChannel}`);
        
        // Clear existing selections when channel changes
        setSelectedDominantTopics([]);
        setSelectedSubtopics([]);
        setCurrentPage(1);
        setTweets([]);
        setPageCache(new Map());
        setCacheLoading(new Set());
        setSelectedPlatform('All');
        setAllUniqueDocuments([]);
        setIsLoadingAllData(false);
        
        const response = await fetchClusterOptions('socialmedia', 'banking', selectedChannel === 'All' ? undefined : selectedChannel);
        console.log(`Cluster options response for ${selectedChannel}:`, response);
        
        if (response.status === 'success') {
          setDominantClusters(response.dominant_clusters);
          setSubclusters(response.subclusters);
          console.log(`Loaded ${response.dominant_clusters.length} dominant clusters and ${response.subclusters.length} subclusters for channel: ${selectedChannel}`);
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
  }, [selectedChannel]);

    // Load documents when filters change
  const loadDocuments = useCallback(async () => {
    // Check if we need to fetch all data for platform filtering
    const needsPlatformFiltering = selectedChannel === 'App Store/Google Play' && selectedPlatform !== 'All';
    
    console.log(`Data loading check - selectedChannel: ${selectedChannel}, selectedPlatform: ${selectedPlatform}, needsPlatformFiltering: ${needsPlatformFiltering}`);
    
    // If no dominant topics selected and not doing platform filtering, clear data
    if (selectedDominantTopics.length === 0 && !needsPlatformFiltering) {
        setTweets([]);
      setAllUniqueDocuments([]);
        setPagination({
          current_page: 1,
          page_size: 10,
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

    // If platform filtering is needed but no dominant topics selected, show message
    if (needsPlatformFiltering && selectedDominantTopics.length === 0) {
      console.log(`Platform filtering requested but no dominant topics selected - cannot proceed`);
      setTweets([]);
      setAllUniqueDocuments([]);
      setPagination({
        current_page: 1,
        page_size: 10,
        total_documents: 0,
        total_pages: 0,
        filtered_count: 0,
        has_next: false,
        has_previous: false,
        page_document_count: 0,
      });
        setLoadingDocuments(false);
        return;
      }

    if (needsPlatformFiltering) {
      // For platform filtering, fetch all data and paginate client-side
      console.log(`Platform filtering detected, fetching all data for client-side filtering`);
      
      try {
        setIsLoadingAllData(true);
        setLoadingDocuments(true);
        
        // Use single dominant cluster
        const kmeansClusterId = selectedDominantTopics[0];
        const subclusterId = selectedSubtopics.length > 0 ? selectedSubtopics.join(',') : undefined;
        const channelFilter = selectedChannel;
        
        // Fetch all data - backend returns all documents in one response
        console.log(`Fetching all documents for platform filtering`);
        
        const response = await fetchTopicAnalysisDocuments(
          'socialmedia',
          kmeansClusterId,
          subclusterId,
          1, // Always fetch page 1
          100, // Use larger page size to get all documents
          'banking',
          channelFilter,
          selectedPlatform
        );

        let allDocuments: Tweet[] = [];
        if (response.status === 'success' && response.documents.length > 0) {
          console.log(`Received ${response.documents.length} documents in single response`);
          console.log(`Sample document platforms:`, response.documents.slice(0, 3).map(doc => ({ id: doc._id, platform: doc.platform })));
          allDocuments = response.documents;
        } else {
          console.log(`No documents or error - status: ${response.status}, documents: ${response.documents?.length || 0}`);
        }
        
        // Filter by platform
        console.log(`Filtering ${allDocuments.length} documents by platform: ${selectedPlatform}`);
        const platformFilteredDocuments = allDocuments.filter(tweet => {
          const tweetPlatform = tweet.platform?.trim();
          const expectedPlatform = selectedPlatform?.trim();
          const matches = tweetPlatform === expectedPlatform;
          if (tweetPlatform && expectedPlatform) {
            console.log(`Tweet platform: "${tweetPlatform}" vs Expected: "${expectedPlatform}" - Match: ${matches}`);
          }
          return matches;
        });
        
        // No need for additional filtering since we only have one dominant cluster selected
        
        console.log(`Platform filtering results: ${platformFilteredDocuments.length} total documents`);
        console.log(`Expected total documents for cluster ${kmeansClusterId} in channel ${channelFilter}: should be 46`);
        
        // Store all unique documents for pagination
        setAllUniqueDocuments(platformFilteredDocuments);
        
        // Paginate client-side
        const pageSize = 10;
        const totalPages = Math.ceil(platformFilteredDocuments.length / pageSize);
        const startIndex = (currentPage - 1) * pageSize;
        const endIndex = startIndex + pageSize;
        const paginatedDocuments = platformFilteredDocuments.slice(startIndex, endIndex);
        
        setTweets(paginatedDocuments);
        setPagination({
          current_page: currentPage,
          page_size: pageSize,
          total_documents: platformFilteredDocuments.length,
          total_pages: totalPages,
          filtered_count: platformFilteredDocuments.length,
          has_next: currentPage < totalPages,
          has_previous: currentPage > 1,
          page_document_count: paginatedDocuments.length,
        });
        
      } catch (error) {
        console.error('Error loading documents for platform filtering:', error);
      } finally {
        setLoadingDocuments(false);
        setIsLoadingAllData(false);
      }
      return;
    }

    // Regular pagination for non-platform filtering
    // Always load fresh data for non-platform filtering
    console.log(`Loading data for regular pagination`);
    try {
      setIsLoadingAllData(true);
      setLoadingDocuments(true);
      
      // Use single dominant cluster
      const kmeansClusterId = selectedDominantTopics[0];
      const subclusterId = selectedSubtopics.length > 0 ? selectedSubtopics.join(',') : undefined;
      const channelFilter = selectedChannel === 'All' ? undefined : selectedChannel;
      
      // Fetch all data - backend returns all documents in one response
      console.log(`Fetching all documents for regular pagination`);
      
      const response = await fetchTopicAnalysisDocuments(
        'socialmedia',
        kmeansClusterId,
        subclusterId,
        1, // Always fetch page 1
        100, // Use larger page size to get all documents
        'banking',
        channelFilter
      );

      let allDocuments: Tweet[] = [];
      if (response.status === 'success' && response.documents.length > 0) {
        console.log(`Received ${response.documents.length} documents in single response`);
        allDocuments = response.documents;
      } else {
        console.log(`No documents or error - status: ${response.status}, documents: ${response.documents?.length || 0}`);
      }
      
        // No need for additional filtering since we only have one dominant cluster selected
        console.log(`Loaded ${allDocuments.length} unique documents total`);
        console.log(`Expected total documents for cluster ${kmeansClusterId} in channel ${channelFilter || 'All'}: should be 46`);
      setAllUniqueDocuments(allDocuments);
      
      // Paginate the documents
      const pageSize = 10;
      const totalPages = Math.ceil(allDocuments.length / pageSize);
      const startIndex = (currentPage - 1) * pageSize;
      const endIndex = startIndex + pageSize;
      const paginatedDocuments = allDocuments.slice(startIndex, endIndex);
      
      console.log(`Paginating: showing ${paginatedDocuments.length} documents on page ${currentPage} of ${totalPages}`);
      
      setTweets(paginatedDocuments);
      setPagination({
        current_page: currentPage,
        page_size: pageSize,
        total_documents: allDocuments.length,
        total_pages: totalPages,
        filtered_count: allDocuments.length,
        has_next: currentPage < totalPages,
        has_previous: currentPage > 1,
        page_document_count: paginatedDocuments.length,
      });
      
      } catch (error) {
        console.error('Error loading documents:', error);
      } finally {
      setIsLoadingAllData(false);
        setLoadingDocuments(false);
      }
  }, [selectedDominantTopics, selectedSubtopics, selectedChannel, selectedPlatform, currentPage]);

  useEffect(() => {
    console.log(`useEffect triggered - selectedDominantTopics:`, selectedDominantTopics, `selectedSubtopics:`, selectedSubtopics, `currentPage:`, currentPage);
    
    // Debounce the API call to prevent rapid successive calls
    const timeoutId = setTimeout(() => {
    loadDocuments();
    }, 300); // 300ms debounce

    // Cleanup timeout on unmount or dependency change
    return () => clearTimeout(timeoutId);
  }, [loadDocuments]);

  // Separate useEffect for pagination only (when currentPage changes but filters don't)
  useEffect(() => {
    if (allUniqueDocuments.length > 0) {
      const pageSize = 10;
      const totalPages = Math.ceil(allUniqueDocuments.length / pageSize);
      const startIndex = (currentPage - 1) * pageSize;
      const endIndex = startIndex + pageSize;
      const paginatedDocuments = allUniqueDocuments.slice(startIndex, endIndex);
      
      console.log(`Pagination only: showing ${paginatedDocuments.length} documents on page ${currentPage} of ${totalPages}`);
      
      setTweets(paginatedDocuments);
      setPagination({
        current_page: currentPage,
        page_size: pageSize,
        total_documents: allUniqueDocuments.length,
        total_pages: totalPages,
        filtered_count: allUniqueDocuments.length,
        has_next: currentPage < totalPages,
        has_previous: currentPage > 1,
        page_document_count: paginatedDocuments.length,
      });
    }
  }, [currentPage, allUniqueDocuments]);

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

  const getFilteredTweets = () => {
    const filteredTweets = tweets;
    
    // Platform filtering is now handled in the data fetching logic
    // Only apply search filtering here
    if (!searchTerm) return filteredTweets;
    
    return filteredTweets.filter(tweet => {
      const searchLower = searchTerm.toLowerCase();
      
      // If a specific channel is selected, only search within that channel
      if (selectedChannel !== 'All' && tweet.channel !== selectedChannel) {
        return false;
      }
      
      return (
        tweet.tweet_id?.toLowerCase().includes(searchLower) ||
        tweet.username?.toLowerCase().includes(searchLower) ||
        tweet.text?.toLowerCase().includes(searchLower) ||
        tweet.dominant_cluster_label?.toLowerCase().includes(searchLower) ||
        tweet.subcluster_label?.toLowerCase().includes(searchLower) ||
        tweet.review_title?.toLowerCase().includes(searchLower)
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
    setAllUniqueDocuments([]);
    setIsLoadingAllData(false);
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

  const handleTweetClick = (tweet: Tweet) => {
    // If clicking the same tweet that's already selected, close the details
    if (selectedTweet?._id === tweet._id) {
      setSelectedTweet(null);
    } else {
      // Otherwise, select the new tweet
      setSelectedTweet(tweet);
      
      // Auto-scroll to make the details panel visible
      setTimeout(() => {
        const detailsPanel = document.querySelector('[data-tweet-details]');
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
    setSelectedChannel('All');
    setSelectedPlatform('All');
    // Clear cache when filters are cleared
    console.log(`Clearing cache due to clear all filters`);
    setPageCache(new Map());
    setCacheLoading(new Set());
    setAllUniqueDocuments([]);
    setIsLoadingAllData(false);
  };

  const handlePlatformChange = (platform: string) => {
    setSelectedPlatform(platform);
    setCurrentPage(1); // Reset to first page
    // Clear cache when platform changes
    console.log(`Clearing cache due to platform change to ${platform}`);
    setPageCache(new Map());
    setCacheLoading(new Set());
    setAllUniqueDocuments([]);
    setIsLoadingAllData(false);
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

  const filteredTweets = getFilteredTweets();
  // Use the tweets directly from the API response since they're already paginated
  const paginatedTweets = filteredTweets;

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
                Social Media Topic Analysis Dashboard
              </h2>
              <p className="text-xl text-gray-300 max-w-4xl mx-auto mb-8">
                Explore and filter social media posts by topics and subtopics to gain actionable insights
              </p>
            </div>

            {/* Channel Tags */}
            <div className="mb-8">
              <div className="flex flex-wrap justify-center gap-3">
                {channelOptions.map((channel) => {
                  const getChannelIcon = (channelValue: string) => {
                    switch (channelValue) {
                      case 'Trustpilot':
                        return (
                          <svg className="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z"/>
                          </svg>
                        );
                      case 'Twitter':
                        return (
                          <svg className="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M23.953 4.57a10 10 0 01-2.825.775 4.958 4.958 0 002.163-2.723c-.951.555-2.005.959-3.127 1.184a4.92 4.92 0 00-8.384 4.482C7.69 8.095 4.067 6.13 1.64 3.162a4.822 4.822 0 00-.666 2.475c0 1.71.87 3.213 2.188 4.096a4.904 4.904 0 01-2.228-.616v.06a4.923 4.923 0 003.946 4.827 4.996 4.996 0 01-2.212.085 4.936 4.936 0 004.604 3.417 9.867 9.867 0 01-6.102 2.105c-.39 0-.779-.023-1.17-.067a13.995 13.995 0 007.557 2.209c9.053 0 13.998-7.496 13.998-13.985 0-.21 0-.42-.015-.63A9.935 9.935 0 0024 4.59z"/>
                          </svg>
                        );
                      case 'App Store/Google Play':
                        return (
                          <div className="flex items-center gap-1">
                            {/* App Store Logo */}
                            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
                              <path d="M18.71 19.5c-.83 1.24-1.71 2.45-3.05 2.47-1.34.03-1.77-.79-3.29-.79-1.53 0-2 .77-3.27.82-1.31.05-2.3-1.32-3.14-2.53C4.25 17 2.94 12.45 4.7 9.39c.87-1.52 2.43-2.48 4.11-2.51 1.28-.02 2.5.87 3.29.87.79 0 2.26-1.07 3.81-.91.65.03 2.47.26 3.64 1.98-.09.06-2.17 1.28-2.15 3.81.03 3.02 2.65 4.03 2.68 4.04-.03.07-.42 1.44-1.38 2.83M13 3.5c.73-.83 1.94-1.46 2.94-1.5.13 1.17-.34 2.35-1.04 3.19-.69.85-1.83 1.51-2.95 1.42-.15-1.15.41-2.35 1.05-3.11z"/>
                            </svg>
                            {/* Google Play Logo */}
                            <svg className="w-4 h-4" viewBox="0 0 24 24" fill="currentColor">
                              <path d="M3.609 1.814L13.792 12L3.609 22.186a.996.996 0 01-.61-.92V2.734a1 1 0 01.61-.92zm10.89 10.893l2.302 2.302-10.937 6.333 8.635-8.635zm3.199-3.198l2.807 1.626a1 1 0 010 1.73l-2.808 1.626L13.5 12l4.198-2.491zM5.864 2.658L16.802 8.99l-2.302 2.302-8.636-8.634z"/>
                            </svg>
                          </div>
                        );
                      case 'Reddit':
                        return (
                          <svg className="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                            <path d="M12 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0zm5.01 4.744c.688 0 1.25.561 1.25 1.249a1.25 1.25 0 0 1-2.498.056l-2.597-.547-.8 3.747c1.824.07 3.48.632 4.674 1.488.308-.309.73-.491 1.207-.491.968 0 1.754.786 1.754 1.754 0 .716-.435 1.333-1.01 1.614a3.111 3.111 0 0 1 .042.52c0 2.694-3.13 4.87-7.004 4.87-3.874 0-7.004-2.176-7.004-4.87 0-.183.015-.366.043-.534A1.748 1.748 0 0 1 4.028 12c0-.968.786-1.754 1.754-1.754.463 0 .898.196 1.207.49 1.207-.883 2.878-1.43 4.744-1.487l.885-4.182a.342.342 0 0 1 .14-.197.35.35 0 0 1 .238-.042l2.906.617a1.214 1.214 0 0 1 1.108-.701zM9.25 12C8.561 12 8 12.562 8 13.25c0 .687.561 1.248 1.25 1.248.687 0 1.248-.561 1.248-1.249 0-.688-.561-1.249-1.249-1.249zm5.5 0c-.687 0-1.248.561-1.248 1.25c0 .687.561 1.248 1.249 1.248.688 0 1.249-.561 1.249-1.249 0-.687-.562-1.249-1.25-1.249zm-5.466 3.99a.327.327 0 0 0-.231.094.33.33 0 0 0 0 .463c.842.842 2.484.913 2.961.913.477 0 2.105-.056 2.961-.913a.361.361 0 0 0 .029-.463.33.33 0 0 0-.464 0c-.547.533-1.684.73-2.512.73-.828 0-1.979-.196-2.512-.73a.326.326 0 0 0-.234-.095z"/>
                          </svg>
                        );
                      default:
                        return null;
                    }
                  };

                  return (
                    <button
                      key={channel.value}
                      onClick={() => setSelectedChannel(channel.value)}
                      className={`px-6 py-3 rounded-full text-sm font-medium transition-all duration-200 flex items-center gap-2 ${
                        selectedChannel === channel.value
                          ? 'bg-pink-600 text-white shadow-lg transform scale-105'
                          : 'bg-gray-800 text-gray-300 hover:bg-gray-700 hover:text-white'
                      }`}
                    >
                      {getChannelIcon(channel.value)}
                      {channel.label}
                    </button>
                  );
                })}
              </div>
            </div>

            {/* Platform Filter - Only show for App Store/Google Play */}
            {selectedChannel === 'App Store/Google Play' && (
              <div className="mb-6">
                <div className="text-center mb-4">
                  <h3 className="text-lg font-semibold text-gray-200 mb-2">Select Platform</h3>
                </div>
                <div className="flex flex-wrap justify-center gap-3">
                  {platformOptions.map((platform) => (
                    <button
                      key={platform.value}
                      onClick={() => handlePlatformChange(platform.value)}
                      className={`px-6 py-3 rounded-full text-sm font-medium transition-all duration-200 ${
                        selectedPlatform === platform.value
                          ? 'bg-purple-600 text-white shadow-lg transform scale-105'
                          : 'bg-gray-800 text-gray-300 hover:bg-gray-700 hover:text-white'
                      }`}
                    >
                      {platform.label}
                    </button>
                  ))}
                </div>
              </div>
            )}

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
                        ? '1 topic selected'
                        : 'Select topic...'}
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
                        placeholder="Search social media posts..."
                        value={searchTerm}
                        onChange={(e) => setSearchTerm(e.target.value)}
                        className="pl-10 pr-4 py-2 bg-gray-700 text-gray-200 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-pink-500 text-sm"
                      />
                    </div>
                  </div>
                  <div className="text-sm text-gray-300">
                    Showing {pagination.page_document_count} of {pagination.total_documents} social media posts
                    {selectedChannel === 'App Store/Google Play' && selectedPlatform !== 'All' && (
                      <span className="text-purple-400 ml-1">({selectedPlatform} only)</span>
                    )}
                  </div>
                </div>

                {/* Content Area */}
                <div className="flex-1 flex overflow-hidden">
                  {/* Tweet List */}
                  <div className="w-full bg-gray-800 bg-opacity-50 overflow-y-auto">
                    {loadingDocuments ? (
                      <div className="p-4 text-center text-gray-300">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-500 mx-auto mb-2"></div>
                        Loading social media posts...
                      </div>
                                         ) : filteredTweets.length === 0 && !loadingDocuments ? (
                       <div className="p-4 text-center text-gray-300">
                         {selectedChannel === 'App Store/Google Play' && selectedPlatform !== 'All' && selectedDominantTopics.length === 0 ? (
                           <div>
                             <p className="text-lg mb-2">Please select a dominant topic first</p>
                             <p className="text-sm text-gray-400">Choose a dominant topic to filter {selectedPlatform} posts</p>
                           </div>
                         ) : (
                           'No social media posts match the selected criteria. Try adjusting your filters.'
                         )}
                       </div>
                     ) : (
                      <>
                        {paginatedTweets.map((tweet, index) => (
                          <div key={`${tweet._id}-${index}-${tweet.platform || 'unknown'}`} className="flex">
                            {/* Tweet List Item */}
                            <div
                              onClick={() => handleTweetClick(tweet)}
                              className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer flex-1 ${
                                selectedTweet?._id === tweet._id ? 'bg-gray-700' : ''
                              }`}
                            >
                              <div className="flex items-center justify-between">
                                <div className="flex-1">
                                  <div className="font-medium text-white text-base mb-1">
                                    {tweet.channel === 'Trustpilot' && tweet.review_title 
                                      ? tweet.review_title 
                                      : tweet.username || 'No Username'
                                    }
                                  </div>
                              <div className="text-sm text-gray-300">
                                {tweet.channel === 'Trustpilot' 
                                  ? (tweet.review_id || 'No Review ID')
                                  : tweet.channel === 'App Store/Google Play'
                                  ? (tweet.review_id || 'No Review ID')
                                  : tweet.channel === 'Reddit'
                                  ? (tweet.post_id || 'No Post ID')
                                  : (tweet.tweet_id || 'No Tweet ID')
                                }
                              </div>
                                  {/* Show App Store/Google Play preview text */}
                                  {tweet.channel === 'App Store/Google Play' && tweet.text && (
                                    <div className="text-xs text-gray-400 mt-1 line-clamp-2">
                                      {tweet.text.length > 100 ? `${tweet.text.substring(0, 100)}...` : tweet.text}
                                    </div>
                                  )}
                                  {/* Show platform info for App Store/Google Play */}
                                  {tweet.channel === 'App Store/Google Play' && tweet.platform && (
                                    <div className="text-xs text-gray-400 mt-1">Platform: {tweet.platform}</div>
                                  )}
                                </div>
                                <div className="flex items-center gap-2">
                                  {tweet.channel === 'Trustpilot' && tweet.urgency && (
                                    <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                      Urgent
                                    </span>
                                  )}
                                  {selectedChannel === 'All' && tweet.channel && (
                                    <span className="px-2 py-1 bg-blue-600 text-white text-xs rounded-full">
                                      {tweet.channel}
                                    </span>
                                  )}
                                  {tweet.channel === 'Trustpilot' && tweet.rating && tweet.rating > 0 && (
                                    <span className="px-2 py-1 bg-yellow-600 text-white text-xs rounded-full">
                                      {tweet.rating}★
                                    </span>
                                  )}
                                  {/* App Store/Google Play specific badges */}
                                  {tweet.channel === 'App Store/Google Play' && (
                                    <>
                                      {tweet.urgency && (
                                        <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                          Urgent
                                        </span>
                                      )}
                                      {tweet.rating && tweet.rating > 0 && (
                                        <span className="px-2 py-1 bg-yellow-600 text-white text-xs rounded-full">
                                          {tweet.rating}★
                                        </span>
                                      )}
                                      {tweet.platform && (
                                        <span className="px-2 py-1 bg-blue-600 text-white text-xs rounded-full">
                                          {tweet.platform}
                                        </span>
                                      )}
                                    </>
                                  )}
                                  {/* Twitter urgency only */}
                                  {tweet.channel === 'Twitter' && tweet.urgency && (
                                    <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                      Urgent
                                    </span>
                                  )}
                                  {/* Reddit urgency only */}
                                  {tweet.channel === 'Reddit' && tweet.urgency && (
                                    <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                      Urgent
                                    </span>
                                  )}
                                </div>
                              </div>
                            </div>
                            
                            {/* Tweet Details - appears right next to selected item */}
                            {selectedTweet?._id === tweet._id && (
                              <div data-tweet-details className="w-2/3 bg-gray-800 bg-opacity-80 border-l border-gray-700 p-6">
                                <div className="flex justify-between items-center mb-4">
                                  <h2 className="text-xl font-semibold text-white">
                                    {selectedTweet.channel === 'Trustpilot' && selectedTweet.review_title 
                                      ? selectedTweet.review_title 
                                      : selectedTweet.username || selectedTweet.tweet_id || 'No Username'
                                    }
                                  </h2>
                                  <button
                                    onClick={() => setSelectedTweet(null)}
                                    className="p-1 hover:bg-gray-700 rounded-full"
                                  >
                                    <X className="w-5 h-5 text-gray-300" />
                                  </button>
                                </div>
                                <div className="border-b border-gray-600 pb-4 mb-4">
                                  {selectedChannel === 'Trustpilot' && selectedTweet.review_title && (
                                    <div className="mb-4">
                                      <div className="text-lg font-semibold text-white mb-2">Review Title</div>
                                      <div className="text-gray-200 bg-gray-900 p-3 rounded-lg">{selectedTweet.review_title}</div>
                                    </div>
                                  )}
                                  <div className="grid grid-cols-2 gap-6">
                                    <div className="space-y-2">
                                  <div className="text-sm text-gray-300">
                                    {selectedTweet.channel === 'Twitter' ? 'Tweet ID' : 
                                     selectedTweet.channel === 'App Store/Google Play' ? 'Review ID' : 
                                     selectedTweet.channel === 'Reddit' ? 'Post ID' : 'Review ID'}: {selectedTweet.review_id || selectedTweet.tweet_id || selectedTweet.post_id || 'N/A'}
                                  </div>
                                      <div className="text-sm text-gray-300">Posted at: {selectedTweet.created_at || 'N/A'}</div>
                                      <div className="text-sm text-gray-300">Priority: {selectedTweet.priority || 'N/A'}</div>
                                      {selectedTweet.rating && selectedTweet.rating > 0 && (
                                        <div className="text-sm text-gray-300">Rating: <span className="text-yellow-400 font-medium">{selectedTweet.rating}★</span></div>
                                      )}
                                      {selectedChannel === 'Trustpilot' && selectedTweet.useful_count !== undefined && (
                                        <div className="text-sm text-gray-300">Useful Count: <span className="text-green-400 font-medium">{selectedTweet.useful_count}</span></div>
                                      )}
                                      {/* Twitter engagement metrics */}
                                      {selectedTweet.channel === 'Twitter' && (
                                        <div className="space-y-1">
                                          {(selectedTweet.like_count ?? 0) > 0 && (
                                            <div className="text-sm text-gray-300">Likes: <span className="text-red-400 font-medium">{selectedTweet.like_count}</span></div>
                                          )}
                                          {(selectedTweet.retweet_count ?? 0) > 0 && (
                                            <div className="text-sm text-gray-300">Retweets: <span className="text-green-400 font-medium">{selectedTweet.retweet_count}</span></div>
                                          )}
                                          {(selectedTweet.reply_count ?? 0) > 0 && (
                                            <div className="text-sm text-gray-300">Replies: <span className="text-blue-400 font-medium">{selectedTweet.reply_count}</span></div>
                                          )}
                                          {(selectedTweet.quote_count ?? 0) > 0 && (
                                            <div className="text-sm text-gray-300">Quotes: <span className="text-purple-400 font-medium">{selectedTweet.quote_count}</span></div>
                                          )}
                                        </div>
                                      )}
                                      {/* Reddit engagement metrics */}
                                      {selectedTweet.channel === 'Reddit' && (
                                        <div className="space-y-1">
                                          {(selectedTweet.like_count ?? 0) > 0 && (
                                            <div className="text-sm text-gray-300">Upvotes: <span className="text-red-400 font-medium">{selectedTweet.like_count}</span></div>
                                          )}
                                          {(selectedTweet.comment_count ?? 0) > 0 && (
                                            <div className="text-sm text-gray-300">Comments: <span className="text-blue-400 font-medium">{selectedTweet.comment_count}</span></div>
                                          )}
                                          {(selectedTweet.share_count ?? 0) > 0 && (
                                            <div className="text-sm text-gray-300">Shares: <span className="text-green-400 font-medium">{selectedTweet.share_count}</span></div>
                                          )}
                                        </div>
                                      )}
                                    </div>
                                    <div className="space-y-2">
                                      <div className="text-sm text-gray-300">User ID: {selectedTweet.user_id || 'N/A'}</div>
                                      <div className="text-sm text-gray-300">Email: {selectedTweet.email_id || 'N/A'}</div>
                                      {selectedChannel === 'All' && selectedTweet.channel && (
                                        <div className="text-sm text-gray-300">Channel: <span className="text-blue-400 font-medium">{selectedTweet.channel}</span>
                                        </div>
                                      )}
                                      {selectedTweet.channel === 'App Store/Google Play' && selectedTweet.platform && (
                                        <div className="text-sm text-gray-300">Platform: <span className="text-purple-400 font-medium">{selectedTweet.platform}</span></div>
                                      )}
                                      {selectedTweet.channel === 'Reddit' && selectedTweet.subreddit && (
                                        <div className="text-sm text-gray-300">Subreddit: <span className="text-orange-400 font-medium">{selectedTweet.subreddit}</span></div>
                                      )}
                                      {selectedTweet.date_of_experience && (
                                        <div className="text-sm text-gray-300">Experience Date: {selectedTweet.date_of_experience}</div>
                                      )}
                                      {selectedTweet.channel === 'App Store/Google Play' && selectedTweet.review_helpful !== undefined && (
                                        <div className="text-sm text-gray-300">Review Helpful: <span className="text-green-400 font-medium">{selectedTweet.review_helpful}</span></div>
                                      )}
                                      {/* Twitter hashtags */}
                                      {selectedTweet.channel === 'Twitter' && selectedTweet.hashtags && selectedTweet.hashtags.length > 0 && (
                                        <div className="text-sm text-gray-300">
                                          <div className="mb-1">Hashtags:</div>
                                          <div className="flex flex-wrap gap-1">
                                            {selectedTweet.hashtags.map((hashtag, index) => (
                                              <span key={index} className="px-2 py-1 bg-blue-600 text-white text-xs rounded-full">
                                                {hashtag}
                                              </span>
                                            ))}
                                          </div>
                                        </div>
                                      )}
                                    </div>
                                  </div>
                                </div>
                                <div className="space-y-6">
                                  <div>
                                <label className="text-sm font-medium text-gray-300">
                                  {selectedTweet.channel === 'Twitter' ? 'Tweet Content' : 
                                   selectedTweet.channel === 'App Store/Google Play' ? 'Review Content' : 
                                   selectedTweet.channel === 'Reddit' ? 'Post Content' : 'Review Content'}
                                </label>
                                    <div className="mt-1 p-4 bg-gray-900 rounded-lg text-gray-200">
                                      {selectedTweet.text || 'No content available'}
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-2 gap-4">
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Dominant Cluster Label</label>
                                      <div className="mt-1 text-pink-400 font-medium">{selectedTweet.dominant_cluster_label || 'N/A'}</div>
                                    </div>
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Subcluster Label</label>
                                      <div className="mt-1 text-purple-400 font-medium">{selectedTweet.subcluster_label || 'N/A'}</div>
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-2 gap-4">
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Dominant Topic</label>
                                      <div className="mt-1 text-gray-200">{selectedTweet.dominant_topic || 'N/A'}</div>
                                    </div>
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Subtopics</label>
                                      <div className="mt-1 text-gray-200">{selectedTweet.subtopics || 'N/A'}</div>
                                    </div>
                                  </div>
                                  <div className="grid grid-cols-2 gap-4">
                                    {selectedTweet.channel === 'Trustpilot' && (
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Useful Count</label>
                                      <div className="mt-1 text-gray-200">{selectedTweet.useful_count || 0}</div>
                                    </div>
                                    )}
                                    {selectedTweet.channel === 'App Store/Google Play' && selectedTweet.review_helpful !== undefined && (
                                      <div>
                                        <label className="text-sm font-medium text-gray-300">Review Helpful</label>
                                        <div className="mt-1 text-gray-200">{selectedTweet.review_helpful}</div>
                                      </div>
                                    )}
                                    <div>
                                      <label className="text-sm font-medium text-gray-300">Sentiment</label>
                                      <div className="mt-1">
                                        <span className={`px-3 py-1 rounded-full text-sm font-medium ${
                                          selectedTweet.sentiment === 'Positive' ? 'bg-green-600 text-white' :
                                          selectedTweet.sentiment === 'Negative' ? 'bg-red-600 text-white' :
                                          selectedTweet.sentiment === 'Neutral' ? 'bg-gray-600 text-white' :
                                          'bg-gray-500 text-white'
                                        }`}>
                                          {selectedTweet.sentiment || 'N/A'}
                                        </span>
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              </div>
                            )}
                          </div>
                        ))}
                        {pagination.total_pages > 1 && (
                          <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                            <button
                              onClick={handlePreviousPage}
                              disabled={!pagination.has_previous}
                              className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              <ChevronLeft className="w-4 h-4" />
                              Previous
                            </button>
                            <div className="text-sm text-gray-300">
                              Page {pagination.current_page} of {pagination.total_pages} • Showing {pagination.page_document_count} of {pagination.total_documents} social media posts
                              {selectedChannel === 'App Store/Google Play' && selectedPlatform !== 'All' && (
                                <span className="text-purple-400 ml-1">({selectedPlatform} only)</span>
                              )}
                            </div>
                            <button
                              onClick={handleNextPage}
                              disabled={!pagination.has_next}
                              className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                            >
                              Next
                              <ChevronRight className="w-4 h-4" />
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
      </div>
    </div>
  );
};

export default TwitterTopicAnalysis;
