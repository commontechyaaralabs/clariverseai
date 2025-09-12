'use client';

import React, { useState, useEffect } from 'react';
import { LogOut, User, Settings, Loader2 } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { useSession, signOut } from 'next-auth/react';

interface UserData {
  id: string;
  email: string;
  full_name: string;
  created_at?: string;
}

const UserProfile = () => {
  const [isOpen, setIsOpen] = useState(false);
  const [userData, setUserData] = useState<UserData | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const router = useRouter();
  const { data: session, status } = useSession();

  // Debug logging
  useEffect(() => {
    console.log('UserProfile - Session status:', status);
    console.log('UserProfile - Session data:', session);
  }, [session, status]);

  // Fetch user data when component mounts or session changes
  useEffect(() => {
    const fetchUserData = async () => {
      if (status !== 'authenticated') return;

      console.log('UserProfile - Fetching user data...');
      setIsLoading(true);
      setError(null);

      try {
        const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "https://clariversev1-153115538723.us-central1.run.app";
        const headers: HeadersInit = {
          "Content-Type": "application/json",
        };

        // Add authorization header if available
        if (session?.accessToken) {
          headers["Authorization"] = `Bearer ${session.accessToken}`;
        }

        const response = await fetch(`${apiBaseUrl}/api/auth/me`, {
          headers,
        });

        console.log('UserProfile - API response status:', response.status);

        if (response.ok) {
          const data = await response.json();
          console.log('UserProfile - User data received:', data);
          setUserData(data);
        } else {
          console.error('UserProfile - Failed to fetch user data:', response.status);
          setError('Failed to load user data');
        }
      } catch (error) {
        console.error('UserProfile - Error fetching user data:', error);
        setError('Error loading user data');
      } finally {
        setIsLoading(false);
      }
    };

    if (status === 'authenticated') {
      fetchUserData();
    }
  }, [session, status]);

  const handleLogout = async () => {
    setIsLoading(true);
    setIsOpen(false);
    
    try {
      await signOut({ 
        callbackUrl: "/",
        redirect: true 
      });
      
      setUserData(null);
      setError(null);
      
      setTimeout(() => {
        router.push('/');
      }, 100);
      
    } catch (error) {
      console.error('Logout error:', error);
      try {
        localStorage.removeItem('next-auth.session-token');
        sessionStorage.clear();
        router.push('/');
        router.refresh();
      } catch (fallbackError) {
        console.error('Fallback logout error:', fallbackError);
        window.location.href = '/';
      }
    } finally {
      setIsLoading(false);
    }
  };

  // Show loading state while session is loading
  if (status === 'loading') {
    console.log('UserProfile - Loading session...');
    return (
      <div className="flex items-center justify-center w-10 h-10 rounded-full bg-gray-700">
        <Loader2 className="w-5 h-5 text-white animate-spin" />
      </div>
    );
  }

  // Show nothing if not authenticated
  if (status === 'unauthenticated') {
    console.log('UserProfile - User not authenticated, hiding component');
    return null;
  }

  // Get user initials for avatar
  const getUserInitials = (name: string) => {
    return name
      .split(' ')
      .map(word => word.charAt(0))
      .join('')
      .toUpperCase()
      .slice(0, 2);
  };

  // Use session data as fallback if userData is not available
  const displayName = userData?.full_name || session?.user?.name || 'User';
  const displayEmail = userData?.email || session?.user?.email || 'user@example.com';
  const initials = getUserInitials(displayName);

  console.log('UserProfile - Rendering with:', {
    displayName,
    displayEmail,
    initials,
    userData,
    session,
    status
  });

  return (
    <div className="relative">
      {/* Profile Button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
        disabled={isLoading}
        className="flex items-center justify-center w-10 h-10 rounded-full overflow-hidden focus:outline-none focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 focus:ring-offset-gray-800 disabled:opacity-50"
      >
        {isLoading ? (
          <Loader2 className="w-5 h-5 text-white animate-spin" />
        ) : (
          <div className="w-full h-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center">
            <span className="text-white text-lg font-semibold">{initials}</span>
          </div>
        )}
      </button>

      {/* Dropdown Menu */}
      {isOpen && (
        <>
          <div
            className="fixed inset-0 z-40"
            onClick={() => setIsOpen(false)}
          />
          
          <div className="absolute right-0 mt-2 w-64 rounded-md shadow-lg bg-gray-800 ring-1 ring-black ring-opacity-5 z-50">
            <div className="py-1">
              {/* User Info Section */}
              <div className="px-4 py-3 border-b border-gray-700">
                {userData ? (
                  <>
                    <div className="flex items-center mb-2">
                      <div className="w-8 h-8 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center mr-3">
                        <span className="text-white text-sm font-semibold">{initials}</span>
                      </div>
                      <div className="flex-1">
                        <p className="text-sm font-medium text-white">{userData.full_name}</p>
                        <p className="text-xs text-gray-400">{userData.email}</p>
                      </div>
                    </div>
                    {userData.created_at && (
                      <p className="text-xs text-gray-500">
                        Member since {new Date(userData.created_at).toLocaleDateString()}
                      </p>
                    )}
                  </>
                ) : error ? (
                  <div className="text-center py-2">
                    <p className="text-sm text-red-400">{error}</p>
                    <button
                      onClick={() => window.location.reload()}
                      className="text-xs text-purple-400 hover:text-purple-300 mt-1"
                    >
                      Retry
                    </button>
                  </div>
                ) : (
                  // Show session data as fallback
                  <div className="flex items-center mb-2">
                    <div className="w-8 h-8 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center mr-3">
                      <span className="text-white text-sm font-semibold">{initials}</span>
                    </div>
                    <div className="flex-1">
                      <p className="text-sm font-medium text-white">{displayName}</p>
                      <p className="text-xs text-gray-400">{displayEmail}</p>
                    </div>
                  </div>
                )}
              </div>

              {/* Menu Items */}
              <div className="py-1">
                <button
                  onClick={() => {
                    setIsOpen(false);
                    // Add profile settings navigation here
                  }}
                  className="flex items-center w-full px-4 py-2 text-sm text-gray-300 hover:bg-gray-700 hover:text-white transition-colors"
                >
                  <User className="w-4 h-4 mr-2" />
                  Profile Settings
                </button>
                
                <button
                  onClick={() => {
                    setIsOpen(false);
                    // Add account settings navigation here
                  }}
                  className="flex items-center w-full px-4 py-2 text-sm text-gray-300 hover:bg-gray-700 hover:text-white transition-colors"
                >
                  <Settings className="w-4 h-4 mr-2" />
                  Account Settings
                </button>
              </div>

              {/* Logout Button */}
              <div className="border-t border-gray-700 pt-1">
                <button
                  onClick={handleLogout}
                  disabled={isLoading}
                  className="flex items-center w-full px-4 py-2 text-sm text-red-400 hover:bg-red-900 hover:text-red-300 transition-colors disabled:opacity-50"
                >
                  {isLoading ? (
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  ) : (
                    <LogOut className="w-4 h-4 mr-2" />
                  )}
                  {isLoading ? 'Signing out...' : 'Sign Out'}
                </button>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  );
};

export default UserProfile; 