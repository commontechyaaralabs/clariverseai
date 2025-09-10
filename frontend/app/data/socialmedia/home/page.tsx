"use client"

import React, { useState, useEffect, useRef } from 'react';
import { BarChart3, Layers, ArrowLeft, ArrowRight, X } from 'lucide-react';
import DataLayout from '../../layout';
import { fetchStatistics, Statistics, fetchClusterData, ClusterData, fetchTopicAnalysisDocuments, DocumentResponse } from '@/lib/api';
import { 
  StatisticsCards, 
  VisualizationSelector, 
  ChartComponents, 
  DataTable
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

const TwitterHomePage = () => {
  const [selectedViz, setSelectedViz] = useState('WordCloud');
  const [expandedStats, setExpandedStats] = useState(true);
  const [sortOrder, setSortOrder] = useState<'desc' | 'asc'>('desc');
  const [dataSource, setDataSource] = useState<Topic[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [statistics, setStatistics] = useState<Statistics | null>(null);
  const [loadingStats, setLoadingStats] = useState(true);
  const [clusterData, setClusterData] = useState<ClusterData | null>(null);
  const [loadingClusters, setLoadingClusters] = useState(true);
  const [selectedDominantCluster, setSelectedDominantCluster] = useState<number | null>(null);
  const [topicToggles, setTopicToggles] = useState<{[key: string]: boolean}>({});
  const [sortColumn, setSortColumn] = useState<string>('No. of Tweets');
  const [sortAscending, setSortAscending] = useState<boolean>(false);
  const [currentPage, setCurrentPage] = useState<number>(0);
  const [selectedTopic, setSelectedTopic] = useState<string>('Show all');
  const [tweetViewData, setTweetViewData] = useState<any>(null);
  const [currentTweetPage, setCurrentTweetPage] = useState<'home' | 'tweet_view'>('home');
  const [tweetPagination, setTweetPagination] = useState<any>(null);
  const [loadingTweets, setLoadingTweets] = useState(false);
  const searchRef = useRef<HTMLDivElement>(null);
  const [tooltip, setTooltip] = useState<TooltipData>({ x: 0, y: 0, data: null });
  const [selectedTopicForSubtopicViz, setSelectedTopicForSubtopicViz] = useState<string | null>(null);
  const [selectedSubtopicViz, setSelectedSubtopicViz] = useState('WordCloud');
  const [selectedTweet, setSelectedTweet] = useState<DocumentResponse | null>(null);
  const [selectedChannel, setSelectedChannel] = useState<string>('All');
  const [socialMediaViewData, setSocialMediaViewData] = useState<any>(null);
  const [currentSocialMediaPage, setCurrentSocialMediaPage] = useState<'home' | 'socialmedia_view'>('home');
  const [socialMediaPagination, setSocialMediaPagination] = useState<any>(null);
  const [loadingSocialMedia, setLoadingSocialMedia] = useState(false);

  const recordsPerPage = 10;

  const channelOptions = [
    { value: 'All', label: 'All' },
    { value: 'Trustpilot', label: 'Trustpilot' },
    { value: 'Twitter', label: 'Twitter' },
    { value: 'App Store/Google Play', label: 'App Store/Google Play' },
    { value: 'Reddit', label: 'Reddit' }
  ];

  // Scroll to top only when switching to tweet view from home page
  useEffect(() => {
    if (currentTweetPage === 'tweet_view') {
      // Small delay to ensure the view has rendered
      setTimeout(() => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }, 100);
    }
  }, [currentTweetPage]);

  // Scroll to top only when switching to social media view from home page
  useEffect(() => {
    if (currentSocialMediaPage === 'socialmedia_view') {
      // Small delay to ensure the view has rendered
      setTimeout(() => {
        window.scrollTo({ top: 0, behavior: 'smooth' });
      }, 100);
    }
  }, [currentSocialMediaPage]);

  // Handle wordcloud click
  const handleWordClick = (word: string, value: number) => {
    console.log(`Clicked on cluster: ${word} with ${value} documents`);
  };

  // Fetch statistics from API
  useEffect(() => {
    const loadStatistics = async () => {
      try {
        setLoadingStats(true);
        const response = await fetchStatistics('socialmedia', 'banking', selectedChannel === 'All' ? undefined : selectedChannel);
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
  }, [selectedChannel]);

  // Fetch cluster data from API
  useEffect(() => {
    const loadClusterData = async () => {
      try {
        setLoadingClusters(true);
        const response = await fetchClusterData('socialmedia', 'banking', selectedChannel === 'All' ? undefined : selectedChannel);
        if (response.status === 'success') {
          setClusterData(response);
          
          // Transform cluster data to match the Topic interface
          // The API already returns only the clusters for the selected channel
          console.log('Data transformation - selectedChannel:', selectedChannel);
          console.log('Data transformation - clusters from API:', response.dominant_clusters.length);
          console.log('Data transformation - cluster names:', response.dominant_clusters.map(c => c.dominant_cluster_label));
          
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
  }, [selectedChannel]);


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


  const toggleTopicKeyphrases = (topicLabel: string) => {
    setTopicToggles(prev => ({
      ...prev,
      [topicLabel]: !prev[topicLabel]
    }));
  };


  const handleSortColumn = (column: string) => {
    if (sortColumn === column) {
      setSortAscending(!sortAscending);
    } else {
      setSortColumn(column);
      setSortAscending(true);
    }
    setCurrentPage(0);
  };

  const handleShowTweets = async (clusterId: number, subclusterId: string, page: number = 1) => {
    try {
      setLoadingTweets(true);
      
      console.log('handleShowTweets called with:', { clusterId, subclusterId, page });
      
      const response = await fetchTopicAnalysisDocuments(
        'socialmedia',
        clusterId,
        subclusterId,
        page,
        30,
        'banking',
        selectedChannel === 'All' ? undefined : selectedChannel
      );
      
      console.log('API Response:', response);

      if (response.status === 'success') {
        setTweetViewData({
          cluster_id: clusterId,
          subcluster_label: subclusterId,
          tweets: response.documents,
          total: response.pagination.total_documents,
          has_more: response.pagination.has_next,
          pagination: response.pagination
        });
        setTweetPagination(response.pagination);
        setCurrentTweetPage('tweet_view');
      } else {
        console.error('Failed to fetch tweets:', response);
        // Fallback to mock data if API fails
        const mockTweets = [
          {
            _id: '1',
            username: 'John Doe',
            text: 'This is a sample tweet content for demonstration purposes.',
            tweet_id: 'T123456',
            urgency: true,
            dominant_topic: 'Account Issues',
            subtopics: 'Login Problems, Password Reset',
            dominant_cluster_label: 'Account Issues',
            subcluster_label: subclusterId
          },
          {
            _id: '2',
            username: 'Jane Smith',
            text: 'Another sample tweet content for demonstration.',
            tweet_id: 'T123457',
            urgency: false,
            dominant_topic: 'Payment Problems',
            subtopics: 'Failed Transactions',
            dominant_cluster_label: 'Payment Problems',
            subcluster_label: subclusterId
          }
        ];

        const mockPagination = {
          current_page: page,
          page_size: 30,
          total_documents: mockTweets.length,
          total_pages: 1,
          filtered_count: mockTweets.length,
          has_next: false,
          has_previous: false,
          page_document_count: mockTweets.length,
        };

        setTweetViewData({
          cluster_id: clusterId,
          subcluster_label: subclusterId,
          tweets: mockTweets,
          total: mockTweets.length,
          has_more: false,
          pagination: mockPagination
        });
        setTweetPagination(mockPagination);
        setCurrentTweetPage('tweet_view');
      }
    } catch (error) {
      console.error('Error fetching tweets:', error);
      // Fallback to mock data
      const mockTweets = [
        {
          _id: '1',
          username: 'John Doe',
          text: 'This is a sample tweet content for demonstration purposes.',
          tweet_id: 'T123456',
          urgency: true,
          dominant_topic: 'Account Issues',
          subtopics: 'Login Problems, Password Reset',
          dominant_cluster_label: 'Account Issues',
          subcluster_label: subclusterId
        },
        {
          _id: '2',
          username: 'Jane Smith',
          text: 'Another sample tweet content for demonstration.',
          tweet_id: 'T123457',
          urgency: false,
          dominant_topic: 'Payment Problems',
          subtopics: 'Failed Transactions',
          dominant_cluster_label: 'Payment Problems',
          subcluster_label: subclusterId
        }
      ];

      const mockPagination = {
        current_page: page,
        page_size: 30,
        total_documents: mockTweets.length,
        total_pages: 1,
        filtered_count: mockTweets.length,
        has_next: false,
        has_previous: false,
        page_document_count: mockTweets.length,
      };

      setTweetViewData({
        cluster_id: clusterId,
        subcluster_label: subclusterId,
        tweets: mockTweets,
        total: mockTweets.length,
        has_more: false,
        pagination: mockPagination
      });
      setTweetPagination(mockPagination);
      setCurrentTweetPage('tweet_view');
    } finally {
      setLoadingTweets(false);
    }
  };

  const handleTweetPageChange = async (newPage: number) => {
    if (!tweetViewData) return;
    
    const { cluster_id, subcluster_label } = tweetViewData;
    await handleShowTweets(cluster_id, subcluster_label, newPage);
  };

  const handleShowSocialMediaPosts = async (clusterId: number, subclusterId: string, page: number = 1) => {
    try {
      setLoadingSocialMedia(true);
      
      console.log('handleShowSocialMediaPosts called with:', { clusterId, subclusterId, page });
      
      const response = await fetchTopicAnalysisDocuments(
        'socialmedia',
        clusterId,
        subclusterId,
        page,
        30,
        'banking',
        selectedChannel === 'All' ? undefined : selectedChannel
      );
      
      console.log('API Response:', response);

      if (response.status === 'success') {
        setSocialMediaViewData({
          cluster_id: clusterId,
          subcluster_label: subclusterId,
          socialMediaPosts: response.documents,
          total: response.pagination.total_documents,
          has_more: response.pagination.has_next,
          pagination: response.pagination
        });
        setSocialMediaPagination(response.pagination);
        setCurrentSocialMediaPage('socialmedia_view');
      } else {
        console.error('Failed to fetch social media posts:', response);
        // Fallback to mock data if API fails
        const mockSocialMediaPosts = [
          {
            _id: '1',
            username: 'John Doe',
            text: 'This is a sample social media post content for demonstration purposes.',
            tweet_id: 'SM123456',
            urgency: true,
            dominant_topic: 'Account Issues',
            subtopics: 'Login Problems, Password Reset',
            dominant_cluster_label: 'Account Issues',
            subcluster_label: subclusterId,
            channel: 'Twitter',
            like_count: 5,
            retweet_count: 2,
            reply_count: 1,
            quote_count: 0
          },
          {
            _id: '2',
            username: 'Jane Smith',
            text: 'Another sample social media post content for demonstration.',
            tweet_id: 'SM123457',
            urgency: false,
            dominant_topic: 'Payment Problems',
            subtopics: 'Failed Transactions',
            dominant_cluster_label: 'Payment Problems',
            subcluster_label: subclusterId,
            channel: 'Trustpilot',
            rating: 2,
            useful_count: 10
          }
        ];

        const mockPagination = {
          current_page: page,
          page_size: 30,
          total_documents: mockSocialMediaPosts.length,
          total_pages: 1,
          filtered_count: mockSocialMediaPosts.length,
          has_next: false,
          has_previous: false,
          page_document_count: mockSocialMediaPosts.length,
        };

        setSocialMediaViewData({
          cluster_id: clusterId,
          subcluster_label: subclusterId,
          socialMediaPosts: mockSocialMediaPosts,
          total: mockSocialMediaPosts.length,
          has_more: false,
          pagination: mockPagination
        });
        setSocialMediaPagination(mockPagination);
        setCurrentSocialMediaPage('socialmedia_view');
      }
    } catch (error) {
      console.error('Error fetching social media posts:', error);
      // Fallback to mock data
      const mockSocialMediaPosts = [
        {
          _id: '1',
          username: 'John Doe',
          text: 'This is a sample social media post content for demonstration purposes.',
          tweet_id: 'SM123456',
          urgency: true,
          dominant_topic: 'Account Issues',
          subtopics: 'Login Problems, Password Reset',
          dominant_cluster_label: 'Account Issues',
          subcluster_label: subclusterId,
          channel: 'Twitter',
          like_count: 5,
          retweet_count: 2,
          reply_count: 1,
          quote_count: 0
        },
        {
          _id: '2',
          username: 'Jane Smith',
          text: 'Another sample social media post content for demonstration.',
          tweet_id: 'SM123457',
          urgency: false,
          dominant_topic: 'Payment Problems',
          subtopics: 'Failed Transactions',
          dominant_cluster_label: 'Payment Problems',
          subcluster_label: subclusterId,
          channel: 'Trustpilot',
          rating: 2,
          useful_count: 10
        }
      ];

      const mockPagination = {
        current_page: page,
        page_size: 30,
        total_documents: mockSocialMediaPosts.length,
        total_pages: 1,
        filtered_count: mockSocialMediaPosts.length,
        has_next: false,
        has_previous: false,
        page_document_count: mockSocialMediaPosts.length,
      };

      setSocialMediaViewData({
        cluster_id: clusterId,
        subcluster_label: subclusterId,
        socialMediaPosts: mockSocialMediaPosts,
        total: mockSocialMediaPosts.length,
        has_more: false,
        pagination: mockPagination
      });
      setSocialMediaPagination(mockPagination);
      setCurrentSocialMediaPage('socialmedia_view');
    } finally {
      setLoadingSocialMedia(false);
    }
  };

  const handleSocialMediaPageChange = async (newPage: number) => {
    if (!socialMediaViewData) return;
    
    const { cluster_id, subcluster_label } = socialMediaViewData;
    await handleShowSocialMediaPosts(cluster_id, subcluster_label, newPage);
  };

  // Handle tweet selection with toggle functionality
  const handleTweetSelect = (tweet: DocumentResponse) => {
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

  // Handle social media post selection with toggle functionality
  const handleSocialMediaSelect = (post: any) => {
    // If clicking the same post that's already selected, close the details
    if (selectedTweet?._id === post._id) {
      setSelectedTweet(null);
    } else {
      // Otherwise, select the new post
      setSelectedTweet(post);
      
      // Auto-scroll to make the details panel visible
      setTimeout(() => {
        const detailsPanel = document.querySelector('[data-socialmedia-details]');
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


  // Prepare chart data for visualizations
  const getChartData = () => {
    if (!clusterData || !clusterData.dominant_clusters) return [];
    
    // The API already returns only the clusters for the selected channel
    // So we just need to use the data as-is
    console.log('getChartData - selectedChannel:', selectedChannel);
    console.log('getChartData - clusters returned by API:', clusterData.dominant_clusters.length);
    console.log('getChartData - cluster names:', clusterData.dominant_clusters.map(c => c.dominant_cluster_label));
    
    return clusterData.dominant_clusters
      .sort((a, b) => (b.document_count || 0) - (a.document_count || 0))
      .map(cluster => ({
        name: cluster.dominant_cluster_label,
        documents: cluster.document_count || 0,
        urgent: cluster.urgent_count || 0,
        urgentPercentage: cluster.urgent_percentage || 0
      }));
  };

  // Memoize chart data to ensure it updates when clusterData or selectedChannel changes
  const chartData = React.useMemo(() => {
    return getChartData();
  }, [clusterData, selectedChannel, getChartData]);





  // Filter data based on selected topic
  const filteredData = selectedTopic !== 'Show all' 
    ? dataSource.filter(topic => topic.name === selectedTopic)
    : dataSource;

  // Sort filtered data
  const sortedFilteredData = [...filteredData].sort((a, b) => {
    let aValue, bValue;
    
    switch (sortColumn) {
      case 'No. of Tweets':
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

  // Tweet view component
  const TweetViewPage = () => {
    if (!tweetViewData) return null;

    const { tweets: tweetList } = tweetViewData;

    return (
      <div className="max-w-[95vw] mx-auto">
        {/* Header with back button */}
        <div className="flex items-center justify-between mb-4">
          <button
            onClick={() => setCurrentTweetPage('home')}
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
            <div className="bg-gray-800 bg-opacity-80 border border-gray-700 rounded-lg shadow-lg p-4 mb-4 flex items-center justify-between">
              <div className="flex items-center gap-6">
                <div className="relative min-w-[300px]">
                  <input
                    type="text"
                        placeholder="Search social media posts..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="w-full pl-4 pr-4 py-3 bg-gray-700 text-gray-200 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-pink-500 text-sm"
                  />
                </div>
              </div>
              <div className="text-sm text-gray-300">
                Showing {tweetPagination?.page_document_count || 0} of {tweetPagination?.total_documents || 0} social media posts
              </div>
            </div>

            {/* Content Area */}
            <div className="flex-1 flex overflow-hidden">
              {/* Tweet List */}
              <div className="w-full bg-gray-800 bg-opacity-50 overflow-y-auto">
                {loadingTweets ? (
                  <div className="p-4 text-center text-gray-300">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-500 mx-auto mb-2"></div>
                        Loading social media posts...
                  </div>
                ) : tweetList.length === 0 ? (
                  <div className="p-4 text-center text-gray-300">
                    No social media posts match the selected criteria. Try adjusting your filters.
                  </div>
                ) : (
                  <>
                    {/* Column Header */}
                    <div className="sticky top-0 bg-gray-900 bg-opacity-95 border-b border-gray-700 p-4 z-10">
                      <div className="flex items-center justify-between">
                        <div className="flex-1">
                          <h3 className="text-sm font-semibold text-gray-200">Social Media Post Details</h3>
                        </div>
                      </div>
                    </div>
                    {tweetList.map((tweet: DocumentResponse) => (
                      <div key={tweet._id} className="flex">
                        {/* Tweet List Item */}
                        <div
                          onClick={() => handleTweetSelect(tweet)}
                          className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer flex-1 ${
                            selectedTweet?._id === tweet._id ? 'bg-gray-700' : ''
                          }`}
                        >
                          <div className="flex items-center justify-between">
                            <div className="flex-1">
                              <div className="font-medium text-white text-base">
                                {tweet.username || tweet.tweet_id || 'No Username'}
                              </div>
                              <div className="text-sm text-gray-400">
                                {tweet.created_at || 'No Timestamp'}
                              </div>
                            </div>
                            <div className="flex items-center gap-2">
                              {tweet.urgency && (
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
                              <h2 className="text-xl font-semibold text-white">{selectedTweet.username || selectedTweet.tweet_id || 'No Username'}</h2>
                              <button
                                onClick={() => setSelectedTweet(null)}
                                className="p-1 hover:bg-gray-700 rounded-full"
                              >
                                <X className="w-5 h-5 text-gray-300" />
                              </button>
                            </div>
                            <div className="border-b border-gray-600 pb-4 mb-4">
                              <div className="grid grid-cols-2 gap-6">
                                <div className="space-y-2">
                                  <div className="text-sm text-gray-300">Post ID: {selectedTweet.tweet_id || 'N/A'}</div>
                                  <div className="text-sm text-gray-300">Posted at: {selectedTweet.created_at || 'N/A'}</div>
                                  <div className="text-sm text-gray-300">Priority: {selectedTweet.priority || 'N/A'}</div>
                                </div>
                                <div className="space-y-2">
                                  <div className="text-sm text-gray-300">User ID: {selectedTweet.user_id || 'N/A'}</div>
                                  <div className="text-sm text-gray-300">Email: {selectedTweet.email_id || 'N/A'}</div>
                                </div>
                              </div>
                            </div>
                            <div className="space-y-6">
                              <div>
                                <label className="text-sm font-medium text-gray-300">Post Text</label>
                                <div className="mt-1 p-4 bg-gray-900 rounded-lg text-gray-200">
                                  {selectedTweet.text || 'No post text available'}
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
                                <div>
                                  <label className="text-sm font-medium text-gray-300">Retweet Count</label>
                                  <div className="mt-1 text-gray-200">{selectedTweet.retweet_count || 0}</div>
                                </div>
                                <div>
                                  <label className="text-sm font-medium text-gray-300">Like Count</label>
                                  <div className="mt-1 text-gray-200">{selectedTweet.like_count || 0}</div>
                                </div>
                              </div>
                              <div className="grid grid-cols-2 gap-4">
                                <div>
                                  <label className="text-sm font-medium text-gray-300">Reply Count</label>
                                  <div className="mt-1 text-gray-200">{selectedTweet.reply_count || 0}</div>
                                </div>
                                <div>
                                  <label className="text-sm font-medium text-gray-300">Quote Count</label>
                                  <div className="mt-1 text-gray-200">{selectedTweet.quote_count || 0}</div>
                                </div>
                              </div>
                              <div>
                                <label className="text-sm font-medium text-gray-300">Hashtags</label>
                                <div className="mt-1 flex flex-wrap gap-2">
                                  {selectedTweet.hashtags && selectedTweet.hashtags.length > 0 ? (
                                    selectedTweet.hashtags.map((hashtag, index) => (
                                      <span key={index} className="px-2 py-1 bg-blue-600 text-white text-xs rounded-full">
                                        {hashtag}
                                      </span>
                                    ))
                                  ) : (
                                    <span className="text-gray-400">No hashtags</span>
                                  )}
                                </div>
                              </div>
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
                        )}
                      </div>
                    ))}
                    {tweetPagination && tweetPagination.total_pages > 1 && (
                      <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                        <button
                          onClick={() => handleTweetPageChange(tweetPagination.current_page - 1)}
                          disabled={!tweetPagination.has_previous}
                          className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          <ArrowLeft className="w-4 h-4" />
                          Previous
                        </button>
                        <div className="text-sm text-gray-300">
                          Page {tweetPagination.current_page} of {tweetPagination.total_pages} • Showing {tweetPagination.page_document_count} of {tweetPagination.total_documents} social media posts
                        </div>
                        <button
                          onClick={() => handleTweetPageChange(tweetPagination.current_page + 1)}
                          disabled={!tweetPagination.has_next}
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
    );
  };

  // Social Media view component
  const SocialMediaViewPage = () => {
    if (!socialMediaViewData) return null;

    const { socialMediaPosts: socialMediaList } = socialMediaViewData;

    return (
      <div className="max-w-[95vw] mx-auto">
        {/* Header with back button */}
        <div className="flex items-center justify-between mb-4">
          <button
            onClick={() => setCurrentSocialMediaPage('home')}
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
            <div className="bg-gray-800 bg-opacity-80 border border-gray-700 rounded-lg shadow-lg p-4 mb-4 flex items-center justify-between">
              <div className="flex items-center gap-6">
                <div className="relative min-w-[300px]">
                  <input
                    type="text"
                    placeholder="Search social media posts..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="w-full pl-4 pr-4 py-3 bg-gray-700 text-gray-200 border border-gray-600 rounded-lg focus:outline-none focus:ring-2 focus:ring-pink-500 text-sm"
                  />
                </div>
              </div>
              <div className="text-sm text-gray-300">
                Showing {socialMediaPagination?.page_document_count || 0} of {socialMediaPagination?.total_documents || 0} social media posts
              </div>
            </div>

            {/* Content Area */}
            <div className="flex-1 flex overflow-hidden">
              {/* Social Media List */}
              <div className="w-full bg-gray-800 bg-opacity-50 overflow-y-auto">
                {loadingSocialMedia ? (
                  <div className="p-4 text-center text-gray-300">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-pink-500 mx-auto mb-2"></div>
                    Loading social media posts...
                  </div>
                ) : socialMediaList.length === 0 ? (
                  <div className="p-4 text-center text-gray-300">
                    No social media posts match the selected criteria. Try adjusting your filters.
                  </div>
                ) : (
                  <>
                    {socialMediaList.map((post: any, index: number) => (
                      <div key={`${post._id}-${index}-${post.platform || 'unknown'}`} className="flex">
                        {/* Social Media List Item */}
                        <div
                          onClick={() => handleSocialMediaSelect(post)}
                          className={`p-4 border-b border-gray-700 hover:bg-gray-700 cursor-pointer flex-1 ${
                            selectedTweet?._id === post._id ? 'bg-gray-700' : ''
                          }`}
                        >
                          <div className="flex items-center justify-between">
                            <div className="flex-1">
                              <div className="font-medium text-white text-base mb-1">
                                {post.channel === 'Trustpilot' && post.review_title 
                                  ? post.review_title 
                                  : post.username || 'No Username'
                                }
                              </div>
                              <div className="text-sm text-gray-300">
                                {post.channel === 'Trustpilot' 
                                  ? (post.review_id || 'No Review ID')
                                  : post.channel === 'App Store/Google Play'
                                  ? (post.review_id || 'No Review ID')
                                  : post.channel === 'Reddit'
                                  ? (post.post_id || 'No Post ID')
                                  : (post.tweet_id || 'No Tweet ID')
                                }
                              </div>
                              {post.channel === 'Trustpilot' && post.username && (
                                <div className="text-xs text-gray-400 mt-1">By: {post.username}</div>
                              )}
                              {/* Show App Store/Google Play preview text */}
                              {post.channel === 'App Store/Google Play' && post.text && (
                                <div className="text-xs text-gray-400 mt-1 line-clamp-2">
                                  {post.text.length > 100 ? `${post.text.substring(0, 100)}...` : post.text}
                                </div>
                              )}
                              {/* Show platform info for App Store/Google Play */}
                              {post.channel === 'App Store/Google Play' && post.platform && (
                                <div className="text-xs text-gray-400 mt-1">Platform: {post.platform}</div>
                              )}
                            </div>
                            <div className="flex items-center gap-2">
                              {post.channel === 'Trustpilot' && post.urgency && (
                                <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                  Urgent
                                </span>
                              )}
                              {selectedChannel === 'All' && post.channel && (
                                <span className="px-2 py-1 bg-blue-600 text-white text-xs rounded-full">
                                  {post.channel}
                                </span>
                              )}
                              {post.channel === 'Trustpilot' && post.rating && post.rating > 0 && (
                                <span className="px-2 py-1 bg-yellow-600 text-white text-xs rounded-full">
                                  {post.rating}★
                                </span>
                              )}
                              {/* App Store/Google Play specific badges */}
                              {post.channel === 'App Store/Google Play' && (
                                <>
                                  {post.urgency && (
                                    <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                      Urgent
                                    </span>
                                  )}
                                  {post.rating && post.rating > 0 && (
                                    <span className="px-2 py-1 bg-yellow-600 text-white text-xs rounded-full">
                                      {post.rating}★
                                    </span>
                                  )}
                                  {post.platform && (
                                    <span className="px-2 py-1 bg-blue-600 text-white text-xs rounded-full">
                                      {post.platform}
                                    </span>
                                  )}
                                </>
                              )}
                              {/* Twitter urgency only */}
                              {post.channel === 'Twitter' && post.urgency && (
                                <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                  Urgent
                                </span>
                              )}
                              {/* Reddit urgency only */}
                              {post.channel === 'Reddit' && post.urgency && (
                                <span className="px-2 py-1 bg-red-600 text-white text-xs rounded-full">
                                  Urgent
                                </span>
                              )}
                            </div>
                          </div>
                        </div>
                        
                        {/* Social Media Details - appears right next to selected item */}
                        {selectedTweet?._id === post._id && selectedTweet && (
                          <div data-socialmedia-details className="w-2/3 bg-gray-800 bg-opacity-80 border-l border-gray-700 p-6">
                            <div className="flex justify-between items-center mb-4">
                              <h2 className="text-xl font-semibold text-white">
                                {selectedTweet.channel === 'Trustpilot' && selectedTweet.review_title 
                                  ? selectedTweet.review_title 
                                  : selectedTweet.username || selectedTweet.tweet_id || selectedTweet.post_id || 'No Username'
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
                    {socialMediaPagination && socialMediaPagination.total_pages > 1 && (
                      <div className="p-4 flex justify-between items-center bg-gray-800 bg-opacity-80 border-t border-gray-700">
                        <button
                          onClick={() => handleSocialMediaPageChange(socialMediaPagination.current_page - 1)}
                          disabled={!socialMediaPagination.has_previous}
                          className="flex items-center gap-2 px-3 py-1 bg-gray-700 text-white rounded-lg hover:bg-gray-600 disabled:opacity-50 disabled:cursor-not-allowed"
                        >
                          <ArrowLeft className="w-4 h-4" />
                          Previous
                        </button>
                        <div className="text-sm text-gray-300">
                          Page {socialMediaPagination.current_page} of {socialMediaPagination.total_pages} • Showing {socialMediaPagination.page_document_count} of {socialMediaPagination.total_documents} social media posts
                        </div>
                        <button
                          onClick={() => handleSocialMediaPageChange(socialMediaPagination.current_page + 1)}
                          disabled={!socialMediaPagination.has_next}
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
    );
  };

  // Main content
  if (currentTweetPage === 'tweet_view') {
    return (
      <AuthGuard requireAuth={true}>
        <DataLayout>
          <section className="py-12 px-4">
            <TweetViewPage />
          </section>
        </DataLayout>
      </AuthGuard>
    );
  }

  if (currentSocialMediaPage === 'socialmedia_view') {
    return (
      <AuthGuard requireAuth={true}>
        <DataLayout>
          <section className="py-12 px-4">
            <SocialMediaViewPage />
          </section>
        </DataLayout>
      </AuthGuard>
    );
  }

  return (
    <AuthGuard requireAuth={true}>
      <DataLayout>
        <section className="py-12 px-4">
        <div className="max-w-7xl mx-auto">
          {/* Header Section */}
          <div className="text-center mb-12">
            <h1 className="text-4xl md:text-6xl font-bold mb-4 bg-gradient-to-r from-pink-400 to-purple-400 bg-clip-text text-transparent">
              Social Media Dashboard
            </h1>
            <p className="text-xl text-gray-300 max-w-4xl mx-auto mb-8">
              Interactively analyze banking social media data with advanced visualizations and topic modeling insights.
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
                          <path d="M12 0A12 12 0 0 0 0 12a12 12 0 0 0 12 12 12 12 0 0 0 12-12A12 12 0 0 0 12 0zm5.01 4.744c.688 0 1.25.561 1.25 1.249a1.25 1.25 0 0 1-2.498.056l-2.597-.547-.8 3.747c1.824.07 3.48.632 4.674 1.488.308-.309.73-.491 1.207-.491.968 0 1.754.786 1.754 1.754 0 .716-.435 1.333-1.01 1.614a3.111 3.111 0 0 1 .042.52c0 2.694-3.13 4.87-7.004 4.87-3.874 0-7.004-2.176-7.004-4.87 0-.183.015-.366.043-.534A1.748 1.748 0 0 1 4.028 12c0-.968.786-1.754 1.754-1.754.463 0 .898.196 1.207.49 1.207-.883 2.878-1.43 4.744-1.487l.885-4.182a.342.342 0 0 1 .14-.197.35.35 0 0 1 .238-.042l2.906.617a1.214 1.214 0 0 1 1.108-.701zM9.25 12C8.561 12 8 12.562 8 13.25c0 .687.561 1.248 1.25 1.248.687 0 1.248-.561 1.248-1.249 0-.688-.561-1.249-1.249-1.249zm5.5 0c-.687 0-1.248.561-1.248 1.25 0 .687.561 1.248 1.249 1.248.688 0 1.249-.561 1.249-1.249 0-.687-.562-1.249-1.25-1.249zm-5.466 3.99a.327.327 0 0 0-.231.094.33.33 0 0 0 0 .463c.842.842 2.484.913 2.961.913.477 0 2.105-.056 2.961-.913a.361.361 0 0 0 .029-.463.33.33 0 0 0-.464 0c-.547.533-1.684.73-2.512.73-.828 0-1.979-.196-2.512-.73a.326.326 0 0 0-.234-.095z"/>
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

          {/* Basic Statistics Section */}
          <StatisticsCards 
            statistics={statistics}
            loadingStats={loadingStats}
            expandedStats={expandedStats}
            onToggleExpanded={() => setExpandedStats(!expandedStats)}
            dataType="socialmedia"
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
              key={`${selectedChannel}-${selectedViz}`}
              type={selectedViz}
              data={chartData}
              loading={loadingClusters}
              title="Dominant Clusters Visualization"
              description="Tweet count by dominant clusters. Hover over elements for detailed metadata."
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
              onShowEmails={handleShowSocialMediaPosts}
              selectedTopicForSubtopicViz={selectedTopicForSubtopicViz}
              selectedSubtopicViz={selectedSubtopicViz}
              onSubtopicVizChange={setSelectedSubtopicViz}
              dataType={"socialmedia" as 'socialmedia'}
            />


          </div>
        </div>
      </section>
    </DataLayout>
    </AuthGuard>
  );
};

export default TwitterHomePage;