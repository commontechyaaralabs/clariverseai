import { useSession } from "next-auth/react";
import { useCallback } from "react";

// Custom hook for making authenticated API calls
export function useAuthenticatedApi() {
  const { data: session, status } = useSession();

  const makeAuthenticatedRequest = useCallback(async (
    url: string, 
    options: RequestInit = {}
  ): Promise<Response> => {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
      ...(options.headers as Record<string, string> || {}),
    };

    // Add authorization header if session exists
    if (session?.accessToken) {
      headers['Authorization'] = `Bearer ${session.accessToken}`;
    }

    return fetch(url, {
      ...options,
      headers,
    });
  }, [session?.accessToken]);

  return {
    makeAuthenticatedRequest,
    isAuthenticated: !!session,
    isLoading: status === 'loading',
    session,
  };
}
