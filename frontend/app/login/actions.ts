"use server";

interface LoginData {
  email: string;
  password: string;
}

interface LoginResult {
  success: boolean;
  error?: string;
}

export async function loginAction(data: LoginData): Promise<LoginResult> {
  try {
    // Server-side validation
    if (!data.email || !data.password) {
      return {
        success: false,
        error: "Email and password are required!"
      };
    }

    // Email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(data.email)) {
      return {
        success: false,
        error: "Please enter a valid email address"
      };
    }

    // For login, we'll use NextAuth's signIn method in the client component
    // This server action can be used for additional server-side validation
    // or if you want to handle login entirely on the server side
    
    return {
      success: true
    };
  } catch (error) {
    console.error("Server-side login error:", error);
    return {
      success: false,
      error: "Error processing login request."
    };
  }
} 