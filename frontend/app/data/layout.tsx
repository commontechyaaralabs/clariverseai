'use client';

import React, { useState } from 'react';
import Sidebar from '@/components/Sidebar/Sidebar';
import { Header } from '@/components/Header/Header';

interface DataLayoutProps {
  children: React.ReactNode;
}

const DataLayout: React.FC<DataLayoutProps> = ({ children }) => {
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);

  const toggleSidebar = () => {
    setIsSidebarOpen(!isSidebarOpen);
  };

  const closeSidebar = () => {
    setIsSidebarOpen(false);
  };

  return (
    <div className="min-h-screen relative overflow-hidden">
      {/* Background */}
      <div 
        className="fixed inset-0 z-0" 
        style={{ background: 'linear-gradient(135deg, #0a0a0a 0%, #1a0a1a 50%, #0a0a1a 100%)' }} 
      />
      
      {/* Gradient Overlay */}
      <div
        className="fixed inset-0 z-10 pointer-events-none"
        style={{
          background: 'linear-gradient(135deg, rgba(185, 10, 189, 0.3) 0%, rgba(83, 50, 255, 0.3) 100%)',
          mixBlendMode: 'multiply',
        }}
      />

      {/* Header */}
      <Header 
        transparent={true} 
        isLoggedIn={true} 
        isSidebarOpen={isSidebarOpen}
        onToggleSidebar={toggleSidebar}
      />

      {/* Sidebar */}
      <div className={`fixed inset-y-0 left-0 z-40 transform transition-transform duration-300 ease-in-out ${
        isSidebarOpen ? 'translate-x-0' : '-translate-x-full'
      }`}>
        <Sidebar onClose={closeSidebar} />
      </div>

      {/* Sidebar Overlay */}
      {isSidebarOpen && (
        <div 
          className="fixed inset-0 z-30"
          onClick={closeSidebar}
        />
      )}

      {/* Main Content */}
      <div className={`relative z-20 pt-[72px] transition-all duration-300 ${isSidebarOpen ? 'filter blur-sm' : ''}`}>
        {children}
      </div>
    </div>
  );
};

export default DataLayout; 