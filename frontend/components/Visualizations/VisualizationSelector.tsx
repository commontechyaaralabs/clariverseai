"use client"

import React from 'react';
import { BarChart3, Cloud, Circle, PieChart as PieChartIcon } from 'lucide-react';

interface VisualizationSelectorProps {
  selectedViz: string;
  onVizChange: (viz: string) => void;
  title?: string;
}

export const VisualizationSelector: React.FC<VisualizationSelectorProps> = ({
  selectedViz,
  onVizChange,
  title = "Visualization"
}) => {
  const visualizationTypes = [
    { key: 'WordCloud', label: 'Word Cloud', icon: Cloud },
    { key: 'BarChart', label: 'Bar Chart', icon: BarChart3 },
    { key: 'CircularBar', label: 'Circular Bar', icon: Circle },
    { key: 'PieChart', label: 'Pie Chart', icon: PieChartIcon }
  ];

  return (
    <div className="flex items-center gap-4">
      <span className="text-gray-300 text-sm font-medium">{title}:</span>
      <div className="flex gap-2">
        {visualizationTypes.map(({ key, label, icon: Icon }) => (
          <button
            key={key}
            onClick={() => onVizChange(key)}
            className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm font-medium transition-all duration-200 ${
              selectedViz === key 
                ? 'bg-pink-500 text-white' 
                : 'bg-gray-700 text-gray-300 hover:bg-gray-600 hover:text-white'
            }`}
          >
            <Icon className="w-4 h-4" />
            {label}
          </button>
        ))}
      </div>
    </div>
  );
};
