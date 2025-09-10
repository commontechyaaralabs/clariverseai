'use client';

import React from 'react';
import { useSession } from 'next-auth/react';
import { Logo } from './Logo';
import { NavigationLinks } from './NavigationLinks';
import { LoginButton } from './LoginButton';
import UserProfile from '../Userprofile/UserProfile';
import { Menu, X } from 'lucide-react';

interface HeaderContentProps {
  className?: string;
  isLoggedIn?: boolean;
  isSidebarOpen?: boolean;
  onToggleSidebar?: () => void;
  onLoginClick?: () => void;
}

const HeaderContent: React.FC<HeaderContentProps> = ({ 
  className = "",
  isLoggedIn: _isLoggedIn = false,
  isSidebarOpen = false,
  onToggleSidebar,
  onLoginClick
}) => {
  const { status } = useSession();
  const isAuthenticated = status === 'authenticated';

  return (
    <div className={`flex justify-between items-center ${className}`}>
      <div className="flex items-center gap-4">
        {isAuthenticated && (
          <button
            onClick={onToggleSidebar}
            className="p-2 hover:bg-gray-800 hover:bg-opacity-50 rounded-lg text-white transition-colors duration-200"
          >
            {isSidebarOpen ? (
              <X className="w-6 h-6" />
            ) : (
              <Menu className="w-6 h-6" />
            )}
          </button>
        )}
        <Logo />
      </div>
      <div className="flex items-center gap-8">
        {isAuthenticated ? (
          <UserProfile />
        ) : (
          <>
            <NavigationLinks />
            <LoginButton onLoginClick={onLoginClick} />
          </>
        )}
      </div>
    </div>
  );
};

export { HeaderContent };