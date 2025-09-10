"use client";

import { useSession } from "next-auth/react";
import { useRouter } from "next/navigation";
import { useEffect } from "react";

interface AuthGuardProps {
  children: React.ReactNode;
  requireAuth?: boolean;
}

export function AuthGuard({ children, requireAuth = false }: AuthGuardProps) {
  const { data: session, status } = useSession();
  const router = useRouter();

  useEffect(() => {
    console.log("AuthGuard: Status:", status, "Session:", !!session, "Pathname:", window.location.pathname);
    
    if (status === "loading") return;

    // If user is authenticated and on login/signup pages, redirect to home page
    if (session && (window.location.pathname === "/login" || window.location.pathname === "/signup")) {
      console.log("AuthGuard: Redirecting authenticated user from login/signup to home");
      router.replace("/home");
      return;
    }

    // If user is not authenticated and trying to access protected pages, redirect to login
    if (!session && requireAuth && (window.location.pathname.startsWith("/home") || window.location.pathname.startsWith("/data"))) {
      console.log("AuthGuard: Redirecting unauthenticated user from protected page to login");
      // Clear any stored session data
      try {
        localStorage.removeItem('next-auth.session-token');
        sessionStorage.clear();
      } catch (error) {
        console.error('Error clearing session data:', error);
      }
      
      router.replace("/login");
      return;
    }

    // If user is not authenticated and on protected routes, redirect to login
    if (!session && requireAuth) {
      console.log("AuthGuard: Redirecting unauthenticated user to login");
      router.replace("/login");
      return;
    }
  }, [session, status, router, requireAuth]);

  // Show loading state while checking authentication
  if (status === "loading") {
    return (
      <div className="min-h-screen bg-black flex items-center justify-center">
        <div className="text-center">
          <div className="w-16 h-16 rounded-full bg-gradient-to-r from-pink-500 to-purple-600 flex items-center justify-center mb-4 mx-auto">
            <div className="w-8 h-8 border-4 border-white border-t-transparent rounded-full animate-spin"></div>
          </div>
          <p className="text-gray-400">Loading...</p>
        </div>
      </div>
    );
  }

  // Don't render children if not authenticated and requireAuth is true
  if (!session && requireAuth) {
    return null;
  }

  return <>{children}</>;
} 