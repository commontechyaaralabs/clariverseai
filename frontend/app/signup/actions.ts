"use server";

interface SignupData {
  fullName: string;
  email: string;
  password: string;
  confirmPassword: string;
}

interface SignupResult {
  success: boolean;
  error?: string;
}

export async function signupAction(data: SignupData): Promise<SignupResult> {
  try {
    // Server-side validation
    if (!data.fullName || !data.email || !data.password || !data.confirmPassword) {
      return {
        success: false,
        error: "All fields are required!"
      };
    }

    if (data.password !== data.confirmPassword) {
      return {
        success: false,
        error: "Passwords do not match"
      };
    }

    if (data.password.length < 8) {
      return {
        success: false,
        error: "Password must be at least 8 characters long"
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

    // API call to local backend
    const apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "https://clariversev1-153115538723.us-central1.run.app";
    const response = await fetch(`${apiBaseUrl}/api/auth/register`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify({
        full_name: data.fullName,
        email: data.email,
        password: data.password,
      }),
    });

    if (response.ok) {
      return {
        success: true
      };
    } else {
      // Handle different error scenarios
      const errorData = await response.json();
      
      if (response.status === 409) {
        return {
          success: false,
          error: "Email already exists! Please use a different email address."
        };
      } else if (response.status === 422) {
        // Validation errors
                 if (errorData.detail && Array.isArray(errorData.detail)) {
           const validationErrors = errorData.detail.map((err: { msg: string }) => err.msg).join(", ");
          return {
            success: false,
            error: `Validation error: ${validationErrors}`
          };
        } else {
          return {
            success: false,
            error: errorData.detail || "Validation failed"
          };
        }
      } else if (response.status === 400) {
        return {
          success: false,
          error: errorData.detail || "Invalid request data"
        };
      } else {
        return {
          success: false,
          error: errorData.detail || "Registration failed. Please try again."
        };
      }
    }
  } catch (error) {
    console.error("Server-side registration error:", error);
    return {
      success: false,
      error: "Error connecting to the server. Please check your internet connection."
    };
  }
} 