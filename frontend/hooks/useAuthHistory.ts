"use client";

import { useRouter } from "next/navigation";

export function useAuthHistory() {
  const router = useRouter();

  const navigateToHome = () => {
    // Navigate to the main dashboard (home page)
    router.push("/home");
  };

  const navigateToLogin = (message?: string) => {
    // First, add the landing page to history
    window.history.pushState(null, "", "/");
    // Then navigate to login with optional message
    const url = message ? `/login?message=${encodeURIComponent(message)}` : "/login";
    router.push(url);
  };

  const navigateToSignup = () => {
    // First, add the landing page to history
    window.history.pushState(null, "", "/");
    // Then navigate to signup
    router.push("/signup");
  };

  const navigateToLanding = () => {
    // Navigate to landing page
    router.push("/");
  };

  return {
    navigateToHome,
    navigateToLogin,
    navigateToSignup,
    navigateToLanding,
  };
} 