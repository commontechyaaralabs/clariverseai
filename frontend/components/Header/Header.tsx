import React from 'react';
import { HeaderContent } from './HeaderContent';

interface HeaderProps {
  className?: string;
  transparent?: boolean;
  isLoggedIn?: boolean;
  isSidebarOpen?: boolean;
  onToggleSidebar?: () => void;
  onLoginClick?: () => void;
}

const Header: React.FC<HeaderProps> = ({
  className = "",
  transparent = true,
  isLoggedIn = false,
  isSidebarOpen = false,
  onToggleSidebar,
  onLoginClick
}) => {
  return (
    <nav className={`fixed top-0 w-full z-50 px-6 py-4 ${className}`}>
      {/* Background with morph and smudge effects */}
      <div className={`absolute inset-0 ${
        transparent 
          ? 'bg-black/20 backdrop-blur-xl' 
          : 'bg-gray-900/30 backdrop-blur-2xl'
      }`} style={{
        maskImage: 'linear-gradient(to bottom, black 0%, black 70%, transparent 100%)',
        WebkitMaskImage: 'linear-gradient(to bottom, black 0%, black 70%, transparent 100%)',
        filter: 'blur(0.5px)',
      }}></div>
      
      {/* Header content without effects */}
      <div className="relative z-10">
        <HeaderContent 
          isLoggedIn={isLoggedIn} 
          isSidebarOpen={isSidebarOpen}
          onToggleSidebar={onToggleSidebar}
          onLoginClick={onLoginClick}
        />
      </div>
    </nav>
  );
};

export { Header };