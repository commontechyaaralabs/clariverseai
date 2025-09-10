"use client";

import { useSession } from "next-auth/react";

interface ApiResponse<T> {
  data?: T;
  error?: string;
  status: number;
}

export function useApi() {
  const { data: session } = useSession();

  const apiCall = async <T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<ApiResponse<T>> => {
    const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "https://clariversev1-107731139870.us-central1.run.app";
    
    const headers: any = {
      "Content-Type": "application/json",
      ...options.headers,
    };

    // Add authorization header if user is authenticated and has accessToken
    if (session?.accessToken) {
      headers["Authorization"] = `Bearer ${session.accessToken}`;
      console.log('useApi - Adding Authorization header with token');
    } else {
      console.log('useApi - No accessToken available in session');
    }

    console.log('useApi - Making request to:', `${apiBaseUrl}${endpoint}`);
    console.log('useApi - Session data:', session);

    try {
      const response = await fetch(`${apiBaseUrl}${endpoint}`, {
        ...options,
        headers,
      });

      console.log('useApi - Response status:', response.status);

      const data = await response.json();

      if (!response.ok) {
        console.error('useApi - Request failed:', data);
        return {
          error: data.detail || `HTTP ${response.status}: ${response.statusText}`,
          status: response.status,
        };
      }

      console.log('useApi - Request successful:', data);
      return {
        data,
        status: response.status,
      };
    } catch (error) {
      console.error("API call error:", error);
      return {
        error: "Network error occurred",
        status: 0,
      };
    }
  };

  return { apiCall };
} 