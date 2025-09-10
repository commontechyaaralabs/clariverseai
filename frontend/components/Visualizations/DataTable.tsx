"use client"

import React from 'react';
import { ArrowLeft, ArrowRight, BarChart3, ChevronDown } from 'lucide-react';
import { ChartComponents } from './ChartComponents';

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

interface DataTableProps {
  data: Topic[];
  loading: boolean;
  selectedTopic: string;
  onTopicChange: (topic: string) => void;
  sortColumn: string;
  sortAscending: boolean;
  onSortColumn: (column: string) => void;
  currentPage: number;
  totalPages: number;
  onPageChange: (page: number) => void;
  topicToggles: {[key: string]: boolean};
  onToggleTopic: (topicName: string) => void;
  onShowSubtopicViz: (topicName: string) => void;
  onShowEmails: (clusterId: number, subclusterId: string) => void;
  selectedTopicForSubtopicViz: string | null;
  selectedSubtopicViz: string;
  onSubtopicVizChange: (viz: string) => void;
  dataType?: 'ticket' | 'email' | 'chat' | 'voice' | 'socialmedia'; // Updated to support Voice and Social Media
}

export const DataTable: React.FC<DataTableProps> = ({
  data,
  loading,
  selectedTopic,
  onTopicChange,
  sortColumn,
  sortAscending,
  onSortColumn,
  currentPage,
  totalPages,
  onPageChange,
  topicToggles,
  onToggleTopic,
  onShowSubtopicViz,
  onShowEmails,
  selectedTopicForSubtopicViz,
  selectedSubtopicViz,
  onSubtopicVizChange,
  dataType = 'ticket'
}) => {
  const recordsPerPage = 10;
  const startIndex = currentPage * recordsPerPage;
  const endIndex = startIndex + recordsPerPage;
  const currentPageData = data.slice(startIndex, endIndex);

  // Get subtopic chart data for a specific topic
  const getSubtopicChartData = (topicName: string) => {
    const topic = data.find(t => t.name === topicName);
    if (!topic) return [];
    
    return topic.subtopics.map((subtopic, index) => ({
      name: subtopic.name,
      documents: subtopic.frequency,
      urgent: subtopic.urgent_count || 0,
      urgentPercentage: subtopic.urgent_percentage || 0,
      color: `hsl(${index * (360 / topic.subtopics.length)}, 70%, 60%)`
    }));
  };

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

  const getShowDocumentLabel = () => {
    switch (dataType) {
      case 'email': return 'Show Emails';
      case 'chat': return 'Show Chats';
      case 'socialmedia': return 'Show Social Media Posts';
      case 'voice': return 'Show Voice Messages';
      default: return 'Show Tickets';
    }
  };

  const getTotalDocumentLabel = () => {
    switch (dataType) {
      case 'email': return 'Total Emails';
      case 'chat': return 'Total Chats';
      case 'socialmedia': return 'Total Social Media Posts';
      case 'voice': return 'Total Voice Messages';
      default: return 'Total Tickets';
    }
  };

  // State for keyphrase expansion
  const [expandedKeyphrases, setExpandedKeyphrases] = React.useState<{ [key: string]: boolean }>({});

  const toggleKeyphrasesExpansion = (topicName: string) => {
    setExpandedKeyphrases(prev => ({
      ...prev,
      [topicName]: !prev[topicName]
    }));
  };

  return (
    <div className="bg-gray-800 rounded-lg p-6 border border-gray-700">
      {/* Topic Selection Dropdown */}
      <div className="mb-6">
        <select
          value={selectedTopic}
          onChange={(e) => onTopicChange(e.target.value)}
          className="px-3 py-2 bg-gray-700 text-white rounded-lg border border-gray-600 focus:outline-none focus:ring-2 focus:ring-pink-500"
        >
          <option value="Show all">Show all</option>
          {data.map(topic => (
            <option key={topic.id} value={topic.name}>{topic.name}</option>
          ))}
        </select>
      </div>

             {/* Table Headers with Sort Buttons */}
       <div className="grid grid-cols-5 gap-4 mb-4">
         <div className="text-sm font-medium text-gray-300">Dominant Cluster Topic</div>
         <button
           onClick={() => onSortColumn(`No. of ${getDocumentLabel()}`)}
           className="text-sm font-medium text-gray-300 hover:text-white text-center flex items-center justify-center gap-1"
         >
           No. of {getDocumentLabel()}
           {sortColumn === `No. of ${getDocumentLabel()}` && (sortAscending ? '▲' : '▼')}
         </button>
         <button
           onClick={() => onSortColumn('No. of Urgent')}
           className="text-sm font-medium text-gray-300 hover:text-white text-center flex items-center justify-center gap-1"
         >
           No. of Urgent
           {sortColumn === 'No. of Urgent' && (sortAscending ? '▲' : '▼')}
         </button>
         <button
           onClick={() => onSortColumn('Urgent %')}
           className="text-sm font-medium text-gray-300 hover:text-white text-center flex items-center justify-center gap-1"
         >
           Urgent %
           {sortColumn === 'Urgent %' && (sortAscending ? '▲' : '▼')}
         </button>
         <button
           onClick={() => onSortColumn('No. of subclusters')}
           className="text-sm font-medium text-gray-300 hover:text-white text-center flex items-center justify-center gap-1"
         >
           No. of subclusters
           {sortColumn === 'No. of subclusters' && (sortAscending ? '▲' : '▼')}
         </button>
       </div>

      <div className="border-t border-gray-600 mb-4"></div>

      {/* Data Rows */}
      {loading ? (
        <div className="space-y-4">
          {Array.from({ length: 5 }).map((_, index) => (
            <div key={index} className="grid grid-cols-5 gap-4 py-4 animate-pulse">
              <div className="h-4 bg-gray-600 rounded"></div>
              <div className="h-4 bg-gray-600 rounded"></div>
              <div className="h-4 bg-gray-600 rounded"></div>
              <div className="h-4 bg-gray-600 rounded"></div>
              <div className="h-4 bg-gray-600 rounded"></div>
            </div>
          ))}
        </div>
      ) : (
        <div className="space-y-4">
          {currentPageData.map((topic) => (
            <div key={topic.id} className="bg-gray-700 rounded-lg border border-gray-600">
              {/* Dropdown Navigation Bar */}
              <div className="grid grid-cols-5 gap-4 py-4 px-4 items-center cursor-pointer hover:bg-gray-600 transition-colors"
                   onClick={() => onToggleTopic(topic.name)}>
                <div className="text-sm text-white font-medium">{topic.name}</div>
                <div className="text-sm text-gray-300 text-center">{topic.frequency.toLocaleString()}</div>
                <div className="text-sm text-gray-300 text-center">{topic.urgent_count || 0}</div>
                <div className="text-sm text-gray-300 text-center">{(topic.urgent_percentage || 0).toFixed(2)}%</div>
                <div className="text-sm text-gray-300 flex items-center justify-between">
                  <span className="text-center flex-1">{topic.subtopics.length}</span>
                  <ChevronDown className={`w-4 h-4 transition-transform duration-200 ${topicToggles[topic.name] ? 'rotate-180' : ''}`} />
                </div>
              </div>

              {/* Expanded content for toggled topics */}
              {topicToggles[topic.name] && (
                <div className="px-4 pb-4 bg-gray-800 rounded-b-lg">
                  {/* Keyphrases */}
                  <div className="mb-4">
                    <h4 className="text-sm font-medium text-white mb-2">Dominant Topics:</h4>
                    <div className="flex flex-wrap gap-2">
                      {topic.keyphrases && topic.keyphrases.length > 0 ? (
                        <>
                          {/* Show first 5 keyphrases always */}
                          {topic.keyphrases.slice(0, 5).map((keyphrase, idx) => (
                            <span key={idx} className="px-3 py-1 bg-gray-600 text-white text-xs rounded-full">
                              {keyphrase}
                            </span>
                          ))}
                          
                          {/* Show remaining keyphrases if expanded */}
                          {expandedKeyphrases[topic.name] && topic.keyphrases.length > 5 && (
                            topic.keyphrases.slice(5).map((keyphrase, idx) => (
                              <span key={idx + 5} className="px-3 py-1 bg-gray-600 text-white text-xs rounded-full">
                                {keyphrase}
                              </span>
                            ))
                          )}
                          
                          {/* Show More button if there are more than 5 keyphrases */}
                          {topic.keyphrases.length > 5 && (
                            <button
                              onClick={() => toggleKeyphrasesExpansion(topic.name)}
                              className="text-gray-300 hover:text-white underline text-xs cursor-pointer"
                            >
                              {expandedKeyphrases[topic.name] ? 'Show Less' : `Show More (+${topic.keyphrases.length - 5})`}
                            </button>
                          )}
                        </>
                      ) : (
                        <span className="text-gray-400 text-sm">No keyphrases available</span>
                      )}
                    </div>
                  </div>

                  {/* Subtopic Visualization Section */}
                  {selectedTopicForSubtopicViz === topic.name && (
                    <div className="mb-6">
                      <div className="flex items-center justify-between mb-4">
                        <h4 className="text-lg font-semibold text-white">Subtopic Visualization for &quot;{topic.name}&quot;</h4>
                        <button
                          onClick={() => onShowSubtopicViz('')}
                          className="text-pink-400 hover:text-pink-300 text-sm flex items-center gap-1"
                        >
                          <ArrowLeft className="w-4 h-4" />
                          Hide Visualization
                        </button>
                      </div>
                      
                      {/* Subtopic Visualization Type Selector */}
                      <div className="flex items-center gap-4 mb-4">
                        <span className="text-gray-300 text-sm font-medium">Visualization:</span>
                        <div className="flex gap-2">
                          {['WordCloud', 'BarChart', 'CircularBar', 'PieChart'].map((viz) => (
                            <button
                              key={viz}
                              onClick={() => onSubtopicVizChange(viz)}
                              className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
                                selectedSubtopicViz === viz 
                                  ? 'bg-pink-500 text-white' 
                                  : 'bg-gray-600 text-gray-300 hover:bg-gray-500 hover:text-white'
                              }`}
                            >
                              <BarChart3 className="w-4 h-4" />
                              {viz}
                            </button>
                          ))}
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Subcluster Analysis */}
                  <div>
                    <div className="flex items-center justify-between mb-2">
                      <h4 className="text-sm font-medium text-white">
                        Subclusters for &apos;{topic.name}&apos; (Cluster ID: {topic.id})
                      </h4>
                      <button
                        onClick={() => onShowSubtopicViz(topic.name)}
                        className="flex items-center gap-2 px-3 py-1 bg-pink-500 hover:bg-pink-600 text-white text-xs rounded-lg transition-colors"
                      >
                        <BarChart3 className="w-3 h-3" />
                        Show Visualization
                      </button>
                    </div>
                    
                                         <div className="grid grid-cols-2 gap-4 mb-4">
                       <div className="bg-gray-600 rounded-lg p-3">
                         <p className="text-xs text-gray-300">{getTotalDocumentLabel()}</p>
                         <p className="text-lg font-bold text-white">{topic.frequency}</p>
                       </div>
                       <div className="bg-gray-600 rounded-lg p-3">
                         <p className="text-xs text-gray-300">Subcluster Count</p>
                         <p className="text-lg font-bold text-white">{topic.subtopics.length}</p>
                       </div>
                     </div>

                    {/* Inline Visualization */}
                    {selectedTopicForSubtopicViz === topic.name && (
                      <div className="mb-4">
                        <div className="flex items-center justify-between mb-2">
                          <h5 className="text-sm font-medium text-white">Visualization:</h5>
                          <select
                            value={selectedSubtopicViz}
                            onChange={(e) => onSubtopicVizChange(e.target.value)}
                            className="px-2 py-1 bg-gray-700 text-white text-xs rounded border border-gray-600 focus:outline-none focus:ring-1 focus:ring-pink-500"
                          >
                            <option value="WordCloud">Word Cloud</option>
                            <option value="BarChart">Bar Chart</option>
                            <option value="CircularBar">Circular Bar</option>
                            <option value="PieChart">Pie Chart</option>
                          </select>
                        </div>
                        <ChartComponents
                          type={selectedSubtopicViz}
                          data={getSubtopicChartData(topic.name)}
                          loading={false}
                          title={`Subtopic Analysis for "${topic.name}"`}
                          description="Document count by subtopics within the selected dominant topic."
                        />
                      </div>
                    )}

                    <h5 className="text-sm font-medium text-white mb-2">Subcluster Frequency Analysis</h5>
                    
                    {/* Subcluster Table */}
                    <div className="overflow-x-auto">
                                             <table className="w-full text-sm">
                         <thead>
                           <tr className="border-b border-gray-600">
                             <th className="text-left py-2 text-gray-300">Subcluster Label</th>
                             <th className="text-left py-2 text-gray-300">No. of {getDocumentLabel()}</th>
                             <th className="text-left py-2 text-gray-300">Urgent Count</th>
                             <th className="text-left py-2 text-gray-300">Urgent %</th>
                             <th className="text-left py-2 text-gray-300">{getShowDocumentLabel()}</th>
                           </tr>
                         </thead>
                        <tbody>
                          {topic.subtopics.map((subtopic, idx) => (
                            <tr key={idx} className="border-b border-gray-700">
                              <td className="py-2 text-white">{subtopic.name}</td>
                              <td className="py-2 text-gray-300">{subtopic.frequency}</td>
                              <td className="py-2 text-gray-300">{subtopic.urgent_count || 0}</td>
                              <td className="py-2 text-gray-300">{(subtopic.urgent_percentage || 0).toFixed(2)}%</td>
                                                             <td className="py-2">
                                 <button
                                   onClick={() => onShowEmails(parseInt(topic.id), subtopic.subcluster_id || subtopic.name)}
                                   className="text-pink-400 hover:text-pink-300 text-sm"
                                 >
                                   {getShowDocumentLabel()}
                                 </button>
                               </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Pagination */}
      <div className="flex items-center justify-between mt-6">
        <button
          onClick={() => onPageChange(Math.max(0, currentPage - 1))}
          disabled={currentPage === 0}
          className="flex items-center gap-2 text-pink-400 hover:text-pink-300 disabled:text-gray-500 disabled:cursor-not-allowed"
        >
          <ArrowLeft className="w-4 h-4" />
          Previous
        </button>
        
        <span className="text-gray-300">
          Page {currentPage + 1} of {Math.max(1, totalPages)}
        </span>
        
        <button
          onClick={() => onPageChange(currentPage + 1)}
          disabled={currentPage >= totalPages - 1}
          className="flex items-center gap-2 text-pink-400 hover:text-pink-300 disabled:text-gray-500 disabled:cursor-not-allowed"
        >
          Next
          <ArrowRight className="w-4 h-4" />
        </button>
      </div>
    </div>
  );
};
