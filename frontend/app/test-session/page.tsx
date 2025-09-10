"use client";

import React from 'react';
import { useSession, signOut } from 'next-auth/react';

const TestSessionPage = () => {
  const { data: session, status } = useSession();

  const handleLogout = async () => {
    try {
      await signOut({ 
        callbackUrl: "/",
        redirect: true 
      });
    } catch (error) {
      console.error('Logout error:', error);
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white p-8">
      <div className="max-w-2xl mx-auto">
        <h1 className="text-3xl font-bold mb-6">Session Test Page</h1>
        
        <div className="bg-gray-800 rounded-lg p-6 mb-6">
          <h2 className="text-xl font-semibold mb-4">Session Information</h2>
          <div className="space-y-2">
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
          <h2 className="text-xl font-semibold mb-4">Test Logout</h2>
          <button
            onClick={handleLogout}
            className="bg-red-600 hover:bg-red-700 text-white px-4 py-2 rounded-lg transition-colors"
          >
            Test Logout
          </button>
        </div>
      </div>
    </div>
  );
};

export default TestSessionPage;
