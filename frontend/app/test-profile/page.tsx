"use client";

import React from 'react';
import { useSession } from 'next-auth/react';
import { Header } from '@/components/Header/Header';
import UserProfile from '@/components/Userprofile/UserProfile';

const TestProfilePage = () => {
  const { data: session, status } = useSession();

  return (
    <div className="min-h-screen bg-gray-900">
      {/* Header with UserProfile */}
      <Header transparent={false} />
      
      {/* Test Content */}
      <div className="pt-20 px-6">
        <div className="max-w-4xl mx-auto">
          <h1 className="text-3xl font-bold text-white mb-6">UserProfile Test Page</h1>
          
          <div className="bg-gray-800 rounded-lg p-6 mb-6">
            <h2 className="text-xl font-semibold text-white mb-4">Session Information</h2>
            <div className="space-y-2 text-gray-300">
              <p><strong>Status:</strong> {status}</p>
              <p><strong>Session:</strong> {session ? 'Available' : 'Not available'}</p>
              {session && (
                <>
                  <p><strong>User:</strong> {session.user?.name || 'No name'}</p>
                  <p><strong>Email:</strong> {session.user?.email || 'No email'}</p>
                  <p><strong>Access Token:</strong> {session.accessToken ? 'Available' : 'Not available'}</p>
                </>
              )}
            </div>
          </div>

          <div className="bg-gray-800 rounded-lg p-6">
            <h2 className="text-xl font-semibold text-white mb-4">UserProfile Component</h2>
            <p className="text-gray-300 mb-4">The UserProfile component should appear in the top-right corner of the header.</p>
            <div className="flex justify-end">
              <UserProfile />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
};

export default TestProfilePage; 