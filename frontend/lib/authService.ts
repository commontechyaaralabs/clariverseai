// Authentication service for API calls

export interface LoginCredentials {
  email: string;
  password: string;
}

export interface RegisterData {
  email: string;
  password: string;
  full_name: string;
}

export interface UserData {
  id: string;
  email: string;
  full_name: string;
  created_at?: string;
}

export interface AuthResponse {
  access_token: string;
  token_type: string;
}

export interface ApiResponse<T> {
  data?: T;
  error?: string;
  status: number;
}

class AuthService {
  private apiBaseUrl: string;

  constructor() {
    this.apiBaseUrl = process.env.NEXT_PUBLIC_API_BASE_URL || "https://clariversev1-107731139870.us-central1.run.app";
  }

  private async makeRequest<T>(
    endpoint: string,
    options: RequestInit = {}
  ): Promise<ApiResponse<T>> {
    const headers: HeadersInit = {
      "Content-Type": "application/json",
      ...options.headers,
    };

    try {
      const response = await fetch(`${this.apiBaseUrl}${endpoint}`, {
        ...options,
        headers,
      });

      const data = await response.json();

      if (!response.ok) {
        return {
          error: data.detail || `HTTP ${response.status}: ${response.statusText}`,
          status: response.status,
        };
      }

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
  }

  async login(credentials: LoginCredentials): Promise<ApiResponse<AuthResponse>> {
    return this.makeRequest<AuthResponse>('/api/auth/login', {
      method: 'POST',
      body: JSON.stringify(credentials),
    });
  }

  async register(userData: RegisterData): Promise<ApiResponse<UserData>> {
    return this.makeRequest<UserData>('/api/auth/register', {
      method: 'POST',
      body: JSON.stringify(userData),
    });
  }

  async getCurrentUser(accessToken: string): Promise<ApiResponse<UserData>> {
    return this.makeRequest<UserData>('/api/auth/me', {
      headers: {
        "Authorization": `Bearer ${accessToken}`,
      },
    });
  }

  async getUsers(accessToken: string, skip: number = 0, limit: number = 100): Promise<ApiResponse<UserData[]>> {
    return this.makeRequest<UserData[]>(`/api/auth/users?skip=${skip}&limit=${limit}`, {
      headers: {
        "Authorization": `Bearer ${accessToken}`,
      },
    });
  }

  async getUserById(accessToken: string, userId: string): Promise<ApiResponse<UserData>> {
    return this.makeRequest<UserData>(`/api/auth/users/${userId}`, {
      headers: {
        "Authorization": `Bearer ${accessToken}`,
      },
    });
  }

  async deleteUser(accessToken: string, userId: string): Promise<ApiResponse<{ message: string }>> {
    return this.makeRequest<{ message: string }>(`/api/auth/users/${userId}`, {
      method: 'DELETE',
      headers: {
        "Authorization": `Bearer ${accessToken}`,
      },
    });
  }
}

export const authService = new AuthService(); 