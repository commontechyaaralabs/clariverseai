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
    <nav className={`fixed top-0 w-full z-50 px-6 py-4 ${
      transparent ? 'bg-transparent backdrop-blur-sm' : 'bg-gray-900'
    } ${className}`}>
      <HeaderContent 
        isLoggedIn={isLoggedIn} 
        isSidebarOpen={isSidebarOpen}
        onToggleSidebar={onToggleSidebar}
        onLoginClick={onLoginClick}
      />
    </nav>
  );
};

export { Header };