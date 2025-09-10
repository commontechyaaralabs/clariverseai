"use client";

import React from 'react';
import { useSession } from 'next-auth/react';

const TestSimplePage = () => {
  const { data: session, status } = useSession();

  return (
    <div className="min-h-screen bg-gray-900 text-white p-8">
      <div className="max-w-2xl mx-auto">
        <h1 className="text-3xl font-bold mb-6">Simple Session Test</h1>
        
        <div className="bg-gray-800 rounded-lg p-6">
          <h2 className="text-xl font-semibold mb-4">Session Status</h2>
          <div className="space-y-2">
            <p><strong>Status:</strong> {status}</p>
            <p><strong>Session:</strong> {session ? 'Available' : 'Not available'}</p>
            {session && (
              <>
                <p><strong>User:</strong> {session.user?.name || 'No name'}</p>
                <p><strong>Email:</strong> {session.user?.email || 'No email'}</p>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default TestSimplePage;
