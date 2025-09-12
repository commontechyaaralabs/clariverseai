'use client';

import React, { useState } from 'react';
import { ChevronDown, ChevronRight, Ticket, Mail, MessageCircle, Home, BarChart3, X, Share2, Phone } from 'lucide-react';
import { useRouter } from 'next/navigation';

interface TreeNode {
  key: string;
  title: string;
  icon?: React.ReactNode;
  children?: TreeNode[];
  disabled?: boolean;
}

interface SidebarProps {
  className?: string;
  onClose?: () => void;
}

const Sidebar: React.FC<SidebarProps> = ({ className = '', onClose }) => {
  const router = useRouter();
  const [expandedKeys, setExpandedKeys] = useState<string[]>(['tickets', 'email', 'chat', 'socialmedia', 'voice']);
  const [selectedKeys, setSelectedKeys] = useState<string[]>([]);

  const treeData: TreeNode[] = [
    {
      key: 'tickets',
      title: 'Tickets',
      icon: <Ticket className="w-4 h-4" />,
      children: [
        {
          key: 'ticket-support-home',
          title: 'Home',
          icon: <Home className="w-4 h-4" />,
        },
        {
          key: 'ticket-support-topic',
          title: 'Topic Analysis',
          icon: <BarChart3 className="w-4 h-4" />,
        },
      ],
    },
    {
      key: 'email',
      title: 'Email',
      icon: <Mail className="w-4 h-4" />,
      children: [
        {
          key: 'email-home',
          title: 'Home',
          icon: <Home className="w-4 h-4" />,
        },
        {
          key: 'email-topic',
          title: 'Topic Analysis',
          icon: <BarChart3 className="w-4 h-4" />,
        },
      ],
    },
    {
      key: 'chat',
      title: 'Chat',
      icon: <MessageCircle className="w-4 h-4" />,
      children: [
        {
          key: 'chat-home',
          title: 'Home',
          icon: <Home className="w-4 h-4" />,
        },
        {
          key: 'chat-topic',
          title: 'Topic Analysis',
          icon: <BarChart3 className="w-4 h-4" />,
        },
      ],
    },
    {
      key: 'socialmedia',
      title: 'Social Media',
      icon: <Share2 className="w-4 h-4" />,
      children: [
        {
          key: 'socialmedia-home',
          title: 'Home',
          icon: <Home className="w-4 h-4" />,
        },
        {
          key: 'socialmedia-topic',
          title: 'Topic Analysis',
          icon: <BarChart3 className="w-4 h-4" />,
        },
      ],
    },
    {
      key: 'voice',
      title: 'Voice',
      icon: <Phone className="w-4 h-4" />,
      children: [
        {
          key: 'voice-home',
          title: 'Home',
          icon: <Home className="w-4 h-4" />,
        },
        {
          key: 'voice-topic',
          title: 'Topic Analysis',
          icon: <BarChart3 className="w-4 h-4" />,
        },
      ],
    },
  ];

  const toggleExpanded = (key: string) => {
    setExpandedKeys(prev => 
      prev.includes(key) 
        ? prev.filter(k => k !== key)
        : [...prev, key]
    );
  };

  const handleSelect = (key: string, hasChildren: boolean) => {
    if (hasChildren) {
      toggleExpanded(key);
    } else {
      setSelectedKeys([key]);
      if (key === 'ticket-support-home') {
        router.push('/data/tickets/home');
      }
      if (key === 'ticket-support-topic') {
        router.push('/data/tickets/topic_analysis');
      }
      if (key === 'email-home') {
        router.push('/data/email/home');
      }
      if (key === 'email-topic') {
        router.push('/data/email/topic_analysis');
      }
      if (key === 'chat-home') {
        router.push('/data/chat/home');
      }
      if (key === 'chat-topic') {
        router.push('/data/chat/topic_analysis');
      }
      if (key === 'socialmedia-home') {
        router.push('/data/socialmedia/home');
      }
      if (key === 'socialmedia-topic') {
        router.push('/data/socialmedia/topic_analysis');
      }
      if (key === 'voice-home') {
        router.push('/data/voice/home');
      }
      if (key === 'voice-topic') {
        router.push('/data/voice/topic_analysis');
      }
      // You can add more routes for other keys here if needed
      console.log('selected', key);
      
      // Close sidebar after navigation
      if (onClose) {
        onClose();
      }
    }
  };

  const renderTreeNode = (node: TreeNode, level: number = 0) => {
    const isExpanded = expandedKeys.includes(node.key);
    const isSelected = selectedKeys.includes(node.key);
    const hasChildren = Boolean(node.children && node.children.length > 0);
    
    return (
      <div key={node.key} className="select-none">
        <div
          className={`flex items-center gap-3 py-2.5 px-4 cursor-pointer rounded-md transition-colors duration-200 ${
            isSelected && !hasChildren
              ? 'bg-purple-900 text-purple-200 border-l-4 border-purple-500' 
              : 'hover:bg-gray-800 text-gray-300'
          } ${node.disabled ? 'opacity-50 cursor-not-allowed' : ''}`}
          style={{ paddingLeft: `${16 + level * 20}px` }}
          onClick={() => !node.disabled && handleSelect(node.key, hasChildren)}
        >
          {/* Expand/Collapse Icon */}
          {hasChildren && (
            <span className="flex-shrink-0 text-gray-400">
              {isExpanded ? (
                <ChevronDown className="w-5 h-5" />
              ) : (
                <ChevronRight className="w-5 h-5" />
              )}
            </span>
          )}
          
          {/* Node Icon */}
          {node.icon && (
            <span className="flex-shrink-0 text-purple-400">
              <div className="w-5 h-5">{node.icon}</div>
            </span>
          )}
          
          {/* Node Title */}
          <span className="flex-1 text-sm font-medium tracking-wide">
            {node.title}
          </span>
        </div>
        
        {/* Children */}
        {hasChildren && isExpanded && (
          <div className="ml-2">
            {node.children!.map(child => renderTreeNode(child, level + 1))}
          </div>
        )}
      </div>
    );
  };

  return (
    <div className={`w-64 bg-gray-900 border-r border-gray-800 h-full overflow-y-auto ${className}`}>
      {/* Header Section */}
      <div className="h-[72px] flex items-center justify-between px-6 border-b border-gray-800">
        <button 
          onClick={onClose}
          className="p-2 hover:bg-gray-800 rounded-lg text-gray-400 hover:text-white transition-colors duration-200"
        >
          <X className="w-6 h-6" />
        </button>
      </div>

      {/* Navigation items */}
      <div className="p-6">
        <div className="space-y-2">
          {treeData.map(node => renderTreeNode(node))}
        </div>
      </div>
    </div>
  );
};

export default Sidebar;