"use client"

import React from 'react';
import { TrendingUp, ChevronUp, ChevronDown, FileText, AlertTriangle, Target, Layers, Activity, Clock } from 'lucide-react';

interface Statistics {
  total_no_of_emails?: number;
  total_urgent_messages?: number;
  urgent_percentage?: number;
  total_dominant_clusters?: number;
  total_subclusters?: number;
  last_run_date?: string | null;
}

interface StatisticsCardsProps {
  statistics: Statistics | null;
  loadingStats: boolean;
  expandedStats: boolean;
  onToggleExpanded: () => void;
  dataType?: 'ticket' | 'email' | 'chat' | 'voice' | 'socialmedia';
}

export const StatisticsCards: React.FC<StatisticsCardsProps> = ({
  statistics,
  loadingStats,
  expandedStats,
  onToggleExpanded,
  dataType = 'ticket'
}) => {
  // Helper function to get the appropriate label based on dataType
  const getTotalLabel = () => {
    switch (dataType) {
      case 'email': return 'Total Emails';
      case 'chat': return 'Total Chats';
      case 'socialmedia': return 'Total Social Media Posts';
      case 'voice': return 'Total Voice Messages';
      default: return 'Total Tickets';
    }
  };

  return (
    <div className="mb-8">
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-2xl font-bold text-white flex items-center gap-2">
          <TrendingUp className="w-6 h-6" />
          Basic Statistics
        </h2>
        <button
          onClick={onToggleExpanded}
          className="flex items-center gap-2 text-gray-300 hover:text-white transition-colors"
        >
          {expandedStats ? <ChevronUp className="w-4 h-4" /> : <ChevronDown className="w-4 h-4" />}
          {expandedStats ? 'Collapse' : 'Expand'}
        </button>
      </div>

      {expandedStats && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          {/* Total Documents */}
          <div className="bg-gray-800 bg-opacity-50 rounded-lg p-6 border border-gray-700 hover:border-pink-500 transition-all">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-gray-400 text-sm font-medium">{getTotalLabel()}</p>
                {loadingStats ? (
                  <div className="animate-pulse bg-gray-700 h-8 w-20 rounded mt-2"></div>
                ) : (
                  <p className="text-3xl font-bold text-white mt-2">
                    {statistics?.total_no_of_emails?.toLocaleString() || '0'}
                  </p>
                )}
              </div>
              <FileText className="w-8 h-8 text-pink-400" />
            </div>
          </div>

          {/* Urgent Messages */}
          <div className="bg-gray-800 bg-opacity-50 rounded-lg p-6 border border-gray-700 hover:border-red-500 transition-all">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-gray-400 text-sm font-medium">Urgent Messages</p>
                {loadingStats ? (
                  <div className="animate-pulse bg-gray-700 h-8 w-20 rounded mt-2"></div>
                ) : (
                  <p className="text-3xl font-bold text-white mt-2">
                    {statistics?.total_urgent_messages?.toLocaleString() || '0'}
                  </p>
                )}
              </div>
              <AlertTriangle className="w-8 h-8 text-red-400" />
            </div>
          </div>

          {/* Urgent Percentage */}
          <div className="bg-gray-800 bg-opacity-50 rounded-lg p-6 border border-gray-700 hover:border-orange-500 transition-all">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-gray-400 text-sm font-medium">Urgent %</p>
                {loadingStats ? (
                  <div className="animate-pulse bg-gray-700 h-8 w-20 rounded mt-2"></div>
                ) : (
                                     <p className="text-3xl font-bold text-white mt-2">
                     {(statistics?.urgent_percentage || 0).toFixed(1)}%
                   </p>
                )}
              </div>
              <Target className="w-8 h-8 text-orange-400" />
            </div>
          </div>

          {/* Dominant Clusters */}
          <div className="bg-gray-800 bg-opacity-50 rounded-lg p-6 border border-gray-700 hover:border-purple-500 transition-all">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-gray-400 text-sm font-medium">Dominant Clusters</p>
                {loadingStats ? (
                  <div className="animate-pulse bg-gray-700 h-8 w-20 rounded mt-2"></div>
                ) : (
                  <p className="text-3xl font-bold text-white mt-2">
                    {statistics?.total_dominant_clusters?.toLocaleString() || '0'}
                  </p>
                )}
              </div>
              <Layers className="w-8 h-8 text-purple-400" />
            </div>
          </div>

          {/* Subclusters */}
          <div className="bg-gray-800 bg-opacity-50 rounded-lg p-6 border border-gray-700 hover:border-blue-500 transition-all">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-gray-400 text-sm font-medium">Subclusters</p>
                {loadingStats ? (
                  <div className="animate-pulse bg-gray-700 h-8 w-20 rounded mt-2"></div>
                ) : (
                  <p className="text-3xl font-bold text-white mt-2">
                    {statistics?.total_subclusters?.toLocaleString() || '0'}
                  </p>
                )}
              </div>
              <Activity className="w-8 h-8 text-blue-400" />
            </div>
          </div>

          {/* Last Run Date */}
          <div className="bg-gray-800 bg-opacity-50 rounded-lg p-6 border border-gray-700 hover:border-green-500 transition-all">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-gray-400 text-sm font-medium">Last Run Date</p>
                {loadingStats ? (
                  <div className="animate-pulse bg-gray-700 h-8 w-32 rounded mt-2"></div>
                ) : (
                  <p className="text-lg font-semibold text-white mt-2">
                    {statistics?.last_run_date || 'Not available'}
                  </p>
                )}
              </div>
              <Clock className="w-8 h-8 text-green-400" />
            </div>
          </div>
        </div>
      )}
    </div>
  );
};
