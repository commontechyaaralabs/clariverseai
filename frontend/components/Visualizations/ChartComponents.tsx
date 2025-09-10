"use client"

import React from 'react';
import dynamic from 'next/dynamic';
import { BarChart3, PieChart as PieChartIcon } from 'lucide-react';
import WordCloud from '@/components/WordCloud';

// Import Chart.js auto registration
import 'chart.js/auto';

// Dynamic imports for Chart.js components
const Bar = dynamic(() => import('react-chartjs-2').then((mod) => mod.Bar), {
  ssr: false,
});

const Pie = dynamic(() => import('react-chartjs-2').then((mod) => mod.Pie), {
  ssr: false,
});

const PolarArea = dynamic(() => import('react-chartjs-2').then((mod) => mod.PolarArea), {
  ssr: false,
});

interface ChartDataItem {
  name: string;
  documents: number;
  urgent: number;
  urgentPercentage: number;
}

interface ChartComponentsProps {
  type: string;
  data: ChartDataItem[];
  loading: boolean;
  title?: string;
  description?: string;
  onWordClick?: (word: string, value: number) => void;
}

export const ChartComponents: React.FC<ChartComponentsProps> = ({
  type,
  data,
  loading,
  title = "Chart",
  description = "Data visualization",
  onWordClick
}) => {
  if (loading) {
    return (
      <div className="bg-gray-800 rounded-lg p-8 text-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-pink-500 mx-auto mb-4"></div>
        <p className="text-gray-400">Loading chart data...</p>
      </div>
    );
  }

  if (data.length === 0) {
    return (
      <div className="bg-gray-800 rounded-lg p-8 text-center">
        <BarChart3 className="w-12 h-12 mx-auto mb-4 text-gray-400" />
        <h3 className="text-lg font-semibold text-white mb-2">No Data Available</h3>
        <p className="text-gray-400">No data found for visualization</p>
      </div>
    );
  }

  if (type === 'WordCloud') {
    const wordCloudData = data.map(item => ({
      text: item.name,
      value: item.documents
    }));
    
    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-white mb-4">{title}</h3>
        <p className="text-gray-400 mb-4 text-sm">
          {description}
          <br />
          <span className="text-pink-300">💡 Tip: Use the zoom controls (+/-) to zoom, drag to pan.</span>
        </p>
        <div className="w-full flex justify-center">
          <div className="inline-block">
            <WordCloud 
              data={wordCloudData} 
              width={Math.min(800, wordCloudData.length * 60)} // Reduced dynamic width
              height={Math.min(400, wordCloudData.length * 30)} // Reduced dynamic height
              className="mx-auto"
              onWordClick={onWordClick}
            />
          </div>
        </div>
        {wordCloudData.length > 0 && (
          <div className="mt-4 text-center">
            <p className="text-gray-400 text-sm">
              Showing {wordCloudData.length} items with {wordCloudData.reduce((sum, item) => sum + item.value, 0).toLocaleString()} total documents
            </p>
          </div>
        )}
      </div>
    );
  }

  if (type === 'BarChart') {
    const chartData = {
      labels: data.map(item => item.name),
      datasets: [
        {
          label: 'Total Documents',
          data: data.map(item => item.documents),
          backgroundColor: 'rgba(59, 130, 246, 0.8)',
          borderColor: 'rgba(59, 130, 246, 1)',
          borderWidth: 1,
        },
      ],
    };

    const chartOptions = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          displayColors: false,
          callbacks: {
            label: function(context: any) {
              const item = data[context.dataIndex];
              return [
                `Total Documents: ${item.documents.toLocaleString()}`,
                `Urgent: ${item.urgent.toLocaleString()}`,
                `Non-Urgent: ${(item.documents - item.urgent).toLocaleString()}`,
                `Urgent %: ${item.urgentPercentage.toFixed(1)}%`
              ];
            }
          }
        }
      },
      scales: {
        y: {
          beginAtZero: true,
          grid: {
            color: 'rgba(156, 163, 175, 0.2)',
          },
          ticks: {
            color: '#9CA3AF',
          },
          title: {
            display: true,
            text: 'Documents',
            color: '#9CA3AF',
          }
        },
        x: {
          grid: {
            color: 'rgba(156, 163, 175, 0.2)',
          },
          ticks: {
            color: '#9CA3AF',
          },
          title: {
            display: true,
            text: 'Categories',
            color: '#9CA3AF',
          }
        }
      }
    };

    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-white mb-4">{title}</h3>
        <p className="text-gray-400 mb-4 text-sm">
          {description}
        </p>
        
        <div style={{ width: '100%', height: '400px' }}>
          <Bar data={chartData} options={chartOptions} />
        </div>
        
        {/* Summary */}
        <div className="mt-4 p-4 bg-gray-700 rounded-lg">
          <h4 className="text-sm font-semibold text-white mb-3 text-center">Summary</h4>
          <div className="grid grid-cols-4 gap-3 text-center">
            <div>
              <div className="text-lg font-bold text-white">
                {data.reduce((sum, item) => sum + item.documents, 0).toLocaleString()}
              </div>
              <div className="text-xs text-gray-400">Total Docs</div>
            </div>
            <div>
              <div className="text-lg font-bold text-red-400">
                {data.reduce((sum, item) => sum + item.urgent, 0).toLocaleString()}
              </div>
              <div className="text-xs text-gray-400">Total Urgent</div>
            </div>
            <div>
              <div className="text-lg font-bold text-orange-400">
                {(data.reduce((sum, item) => sum + item.urgent, 0) / data.reduce((sum, item) => sum + item.documents, 0) * 100).toFixed(1)}%
              </div>
              <div className="text-xs text-gray-400">Avg Urgent %</div>
            </div>
            <div>
              <div className="text-lg font-bold text-green-400">
                {data.length}
              </div>
              <div className="text-xs text-gray-400">Categories</div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (type === 'CircularBar') {
    const chartData = {
      labels: data.map(item => item.name),
      datasets: [{
        label: 'Documents',
        data: data.map(item => item.documents),
        backgroundColor: data.map((_, index) => 
          `hsla(${index * (360 / data.length)}, 70%, 60%, 0.9)`
        ),
        borderColor: data.map((_, index) => 
          `hsl(${index * (360 / data.length)}, 70%, 60%)`
        ),
        borderWidth: 4,
        hoverBorderWidth: 6,
        hoverBackgroundColor: data.map((_, index) => 
          `hsla(${index * (360 / data.length)}, 70%, 70%, 1)`
        ),
      }]
    };

    const maxDocuments = Math.max(...data.map(item => item.documents));
    const minDocuments = Math.min(...data.map(item => item.documents));
    const minScale = Math.max(1, Math.floor(minDocuments * 0.1));
    const maxScale = Math.ceil(maxDocuments * 1.2);
    
    const chartOptions = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: function(context: any) {
              const item = data[context.dataIndex];
              return [
                `Documents: ${item.documents.toLocaleString()}`,
                `Urgent: ${item.urgent.toLocaleString()}`,
                `Urgent %: ${item.urgentPercentage.toFixed(1)}%`
              ];
            }
          }
        }
      },
      scales: {
        r: {
          beginAtZero: true,
          min: minScale,
          max: maxScale,
          grid: {
            color: '#374151'
          },
          ticks: {
            color: '#9CA3AF',
            backdropColor: 'transparent',
            stepSize: Math.ceil((maxScale - minScale) / 10),
            callback: function(value: any) {
              return value.toLocaleString();
            }
          },
          pointLabels: {
            display: false
          }
        }
      },
      elements: {
        arc: {
          borderWidth: 3,
          borderColor: 'rgba(255, 255, 255, 0.3)'
        }
      }
    };

    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-white mb-4">{title}</h3>
        <p className="text-gray-400 mb-4 text-sm">
          {description}
        </p>
        
        <div className="flex gap-6">
          <div className="flex-1" style={{ height: '600px' }}>
            <PolarArea data={chartData} options={chartOptions} />
          </div>
          
          <div className="w-64 max-h-96 overflow-y-auto pr-2">
            <h4 className="text-lg font-semibold text-white mb-4">Labels</h4>
            <div className="space-y-2">
              {data.map((item, index) => (
                <div key={index} className="group hover:bg-gray-700 rounded-lg p-3 transition-all duration-200 border border-transparent hover:border-pink-500">
                  <div className="flex items-center space-x-3">
                    <div 
                      className="w-4 h-4 rounded-full"
                      style={{ backgroundColor: `hsl(${index * (360 / data.length)}, 70%, 60%)` }}
                    ></div>
                    <span className="text-sm text-gray-300 group-hover:text-white transition-colors font-medium truncate">
                      {item.name}
                    </span>
                  </div>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  }

  if (type === 'PieChart') {
    const chartData = {
      labels: data.map(item => item.name),
      datasets: [
        {
          label: 'Documents',
          data: data.map(item => item.documents),
          backgroundColor: data.map((_, index) => 
            `hsl(${index * (360 / data.length)}, 70%, 60%)`
          ),
          borderColor: data.map((_, index) => 
            `hsl(${index * (360 / data.length)}, 70%, 50%)`
          ),
          borderWidth: 2,
          hoverOffset: 4,
        },
      ],
    };

    const chartOptions = {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: function(context: unknown) {
              const item = data[(context as { dataIndex: number }).dataIndex];
              const total = data.reduce((sum: number, item: ChartDataItem) => sum + item.documents, 0);
              const percentage = ((item.documents / total) * 100).toFixed(1);
              return [
                `Documents: ${item.documents.toLocaleString()}`,
                `Percentage: ${percentage}%`,
                `Urgent %: ${item.urgentPercentage.toFixed(1)}%`
              ];
            }
          }
        }
      }
    };

    const total = data.reduce((sum: number, item: ChartDataItem) => sum + item.documents, 0);
    
    return (
      <div className="bg-gray-800 rounded-lg p-6">
        <h3 className="text-lg font-semibold text-white mb-4">{title}</h3>
        <p className="text-gray-400 mb-4 text-sm">
          {description}
        </p>
        
        <div className="flex gap-6">
          <div className="flex-1" style={{ height: '500px' }}>
            <Pie data={chartData} options={chartOptions} />
          </div>
          
          <div className="w-64 bg-gray-700 rounded-lg p-4">
            <h4 className="text-sm font-semibold text-white mb-3">Legend</h4>
            <div className="max-h-96 overflow-y-auto pr-2">
              {data.map((item, index) => {
                const percentage = ((item.documents / total) * 100).toFixed(1);
                return (
                  <div key={index} className="flex items-center space-x-3 mb-3 p-2 rounded hover:bg-gray-600">
                    <div 
                      className="w-4 h-4 rounded-full"
                      style={{ backgroundColor: `hsl(${index * (360 / data.length)}, 70%, 60%)` }}
                    ></div>
                    <div className="flex-1 min-w-0">
                      <div className="text-sm text-white font-medium truncate">{item.name}</div>
                      <div className="text-xs text-gray-400">
                        {item.documents.toLocaleString()} ({percentage}%)
                      </div>
                      <div className="text-xs text-orange-400">
                        Urgent: {item.urgentPercentage.toFixed(1)}%
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        </div>
        
        <div className="mt-6 p-6 bg-gray-700 rounded-lg">
          <h4 className="text-lg font-semibold text-white mb-4 text-center">Overall Summary</h4>
          <div className="grid grid-cols-3 gap-4 text-center">
            <div>
              <div className="text-3xl font-bold text-white">
                {total.toLocaleString()}
              </div>
              <div className="text-sm text-gray-400">Total Documents</div>
            </div>
            <div>
              <div className="text-3xl font-bold text-blue-400">
                {data.length}
              </div>
              <div className="text-sm text-gray-400">Total Categories</div>
            </div>
            <div>
              <div className="text-3xl font-bold text-orange-400">
                {(data.reduce((sum: number, item: ChartDataItem) => sum + item.urgentPercentage, 0) / data.length).toFixed(1)}%
              </div>
              <div className="text-sm text-gray-400">Avg Urgent %</div>
            </div>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 rounded-lg p-8 text-center">
      <div className="text-gray-400 mb-4">
        {type === 'BarChart' && <BarChart3 className="w-12 h-12 mx-auto mb-4" />}
        {type === 'PieChart' && <PieChartIcon className="w-12 h-12 mx-auto mb-4" />}
      </div>
      <h3 className="text-lg font-semibold text-white mb-2">{type}</h3>
      <p className="text-gray-400">Visualization will be implemented here</p>
    </div>
  );
};
