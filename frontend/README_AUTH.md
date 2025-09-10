# Authentication System Documentation

This document explains how to use the authentication system with the API endpoints.

## Overview

The authentication system integrates with your FastAPI backend using NextAuth.js for session management. It provides:

- User registration and login
- JWT token-based authentication
- User profile management
- Secure logout functionality
- Protected route handling

## API Endpoints

### Authentication Endpoints

| Method | Endpoint | Description | Auth Required |
|--------|----------|-------------|---------------|
| POST | `/api/auth/register` | Register a new user | No |
| POST | `/api/auth/login` | Login and get access token | No |
| GET | `/api/auth/me` | Get current user information | Yes |
| GET | `/api/auth/users` | List all users (admin only) | Yes |
| GET | `/api/auth/users/{user_id}` | Get user by ID (admin only) | Yes |
| DELETE | `/api/auth/users/{user_id}` | Delete user (admin only) | Yes |

## Components

### 1. UserProfile Component

The `UserProfile` component displays user information and provides logout functionality.

**Features:**
- Shows user avatar with initials
- Displays user name, email, and member since date
- Provides logout functionality
- Handles loading and error states
- Redirects to landing page after logout

**Usage:**
```tsx
import UserProfile from '@/components/Userprofile/UserProfile';

// In your header or navigation
<UserProfile />
```

### 2. AuthGuard Component

The `AuthGuard` component protects routes and handles authentication state.

**Features:**
- Protects routes from unauthorized access
- Redirects unauthenticated users to landing page
- Redirects authenticated users away from login/signup pages
- Shows loading state during authentication checks

**Usage:**
```tsx
import { AuthGuard } from '@/components/AuthGuard';

// Protect a page
<AuthGuard requireAuth={true}>
  <ProtectedPage />
</AuthGuard>

// Allow both authenticated and unauthenticated users
<AuthGuard requireAuth={false}>
  <PublicPage />
</AuthGuard>
```

### 3. LoginForm Component

The `LoginForm` component provides a complete login interface.

**Features:**
- Email and password input fields
- Show/hide password functionality
- Loading states and error handling
- Integration with NextAuth.js
- Redirects to home page after successful login

**Usage:**
```tsx
import { LoginForm } from '@/components/Auth/LoginForm';

// In your login page
<LoginForm />
```

## Hooks and Utilities

### 1. useApi Hook

The `useApi` hook provides authenticated API calls.

**Features:**
- Automatically adds authorization headers
- Handles API errors consistently
- TypeScript support
- Session-based authentication

**Usage:**
```tsx
import { useApi } from '@/hooks/useApi';

const { apiCall } = useApi();

// Make authenticated API call
const response = await apiCall<UserData>('/api/auth/me');
if (response.data) {
  // Handle success
} else if (response.error) {
  // Handle error
}
```

### 2. useAuthHistory Hook

The `useAuthHistory` hook provides navigation utilities.

**Features:**
- Navigate to home page
- Navigate to login page
- Navigate to signup page
- Navigate to landing page

**Usage:**
```tsx
import { useAuthHistory } from '@/hooks/useAuthHistory';

const { navigateToHome, navigateToLanding } = useAuthHistory();

// Navigate to home
navigateToHome();

// Navigate to landing page
navigateToLanding();
```

## Authentication Service

The `authService` provides direct API access for authentication operations.

**Features:**
- Login with credentials
- Register new users
- Get current user information
- Manage users (admin only)
- TypeScript interfaces for all operations

**Usage:**
```tsx
import { authService } from '@/lib/authService';

// Login
const loginResponse = await authService.login({
  email: 'user@example.com',
  password: 'password123'
});

// Register
const registerResponse = await authService.register({
  email: 'user@example.com',
  password: 'password123',
  full_name: 'John Doe'
});

// Get current user
const userResponse = await authService.getCurrentUser(accessToken);
```

## Configuration

### Environment Variables

Set these environment variables in your `.env.local` file:

```env
NEXT_PUBLIC_API_URL=https://clariversev1-107731139870.us-central1.run.app
NEXTAUTH_SECRET=your-secret-key-here
NEXTAUTH_URL=http://localhost:3000
```

### NextAuth Configuration

The authentication is configured in `lib/authOptions.ts`:

```tsx
export const authOptions: NextAuthOptions = {
  providers: [
    CredentialsProvider({
      // ... configuration
    }),
  ],
  callbacks: {
    // ... callbacks
  },
  // ... other options
};
```

## Usage Examples

### 1. Protected Page

```tsx
// app/home/page.tsx
import { AuthGuard } from '@/components/AuthGuard';
import UserProfile from '@/components/Userprofile/UserProfile';

export default function HomePage() {
  return (
    <AuthGuard requireAuth={true}>
      <div className="min-h-screen bg-gray-900">
        <header className="flex justify-between items-center p-6">
          <h1 className="text-white text-2xl font-bold">Dashboard</h1>
          <UserProfile />
        </header>
        {/* Your protected content */}
      </div>
    </AuthGuard>
  );
}
```

### 2. Login Page

```tsx
// app/login/page.tsx
import { LoginForm } from '@/components/Auth/LoginForm';
import { AuthGuard } from '@/components/AuthGuard';

export default function LoginPage() {
  return (
    <AuthGuard requireAuth={false}>
      <div className="min-h-screen bg-gray-900 flex items-center justify-center p-4">
        <LoginForm />
      </div>
    </AuthGuard>
  );
}
```

### 3. Landing Page

```tsx
// app/page.tsx
import { LandingPage } from '@/components/LandingPage';

export default function LandingPageComponent() {
  return <LandingPage />;
}
```

## Logout Flow

When a user clicks the logout button:

1. **Immediate UI Response**: Dropdown closes and loading state shows
2. **NextAuth SignOut**: Calls NextAuth's signOut function
3. **Session Cleanup**: Clears local session data
4. **Redirect**: Redirects to landing page (`/`)
5. **Fallback**: If NextAuth fails, manual cleanup and redirect

## Error Handling

The system handles various error scenarios:

- **Network Errors**: Shows user-friendly error messages
- **Authentication Errors**: Redirects to appropriate pages
- **API Errors**: Displays specific error messages from the backend
- **Session Expiry**: Automatically redirects to landing page

## Security Features

- **JWT Tokens**: Secure token-based authentication
- **Session Management**: NextAuth.js handles session security
- **Protected Routes**: AuthGuard prevents unauthorized access
- **Token Expiry**: Automatic session cleanup
- **CSRF Protection**: Built into NextAuth.js

## Testing

### Manual Testing

1. **Register a new user**:
   ```bash
   curl -X POST "https://clariversev1-107731139870.us-central1.run.app/api/auth/register" \
     -H "Content-Type: application/json" \
     -d '{
       "email": "test@example.com",
       "password": "password123",
       "full_name": "Test User"
     }'
   ```

2. **Login**:
   ```bash
   curl -X POST "https://clariversev1-107731139870.us-central1.run.app/api/auth/login" \
     -H "Content-Type: application/json" \
     -d '{
       "email": "test@example.com",
       "password": "password123"
     }'
   ```

3. **Get user info** (use token from login):
   ```bash
   curl -X GET "https://clariversev1-107731139870.us-central1.run.app/api/auth/me" \
     -H "Authorization: Bearer YOUR_TOKEN_HERE"
   ```

### Frontend Testing

1. Navigate to `/login`
2. Enter credentials and submit
3. Verify redirect to `/home`
4. Check user profile dropdown shows correct information
5. Click logout and verify redirect to landing page

## Troubleshooting

### Common Issues

1. **CORS Errors**: Ensure your API allows requests from your frontend domain
2. **Token Issues**: Check that the API is returning the correct token format
3. **Redirect Loops**: Verify AuthGuard configuration
4. **Session Not Persisting**: Check NextAuth configuration and environment variables

### Debug Mode

Enable debug logging by setting:

```env
NEXTAUTH_DEBUG=true
```

This will show detailed authentication logs in the console.

## API Integration

The frontend integrates with your FastAPI backend using these endpoints:

- **Login**: `POST /api/auth/login`
- **Register**: `POST /api/auth/register`
- **Get User**: `GET /api/auth/me`
- **List Users**: `GET /api/auth/users` (admin only)
- **Get User by ID**: `GET /api/auth/users/{user_id}` (admin only)
- **Delete User**: `DELETE /api/auth/users/{user_id}` (admin only)

All authenticated endpoints require the `Authorization: Bearer <token>` header. 