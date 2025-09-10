# Authentication System

This directory contains the authentication system for the API, similar to the structure in `backend/src/auth`.

## Files

- `auth_config.py` - JWT configuration settings
- `constants.py` - Error codes and constants
- `exceptions.py` - Custom HTTP exceptions
- `schemas.py` - Pydantic models for authentication
- `jwt.py` - JWT token creation and parsing utilities
- `service.py` - Authentication service with user management
- `dependencies.py` - FastAPI dependencies for authentication
- `router.py` - Authentication endpoints

## Features

### User Management
- User registration with email and password
- User login with JWT token generation
- Password hashing using SHA-256
- User profile management

### Authentication
- JWT-based authentication
- Token expiration (60 days)
- Optional authentication for some endpoints
- Admin role support

### Endpoints

#### Public Endpoints
- `POST /api/auth/register` - Register a new user
- `POST /api/auth/login` - Login and get access token

#### Protected Endpoints
- `GET /api/auth/me` - Get current user information
- `GET /api/auth/users` - List all users (admin only)
- `GET /api/auth/users/{user_id}` - Get user by ID (admin only)
- `DELETE /api/auth/users/{user_id}` - Delete user (admin only)

#### Topic Analysis Endpoints (Now Protected)
- `GET /api/topic-analysis/clusters` - Get cluster options (requires auth)
- `POST /api/topic-analysis/documents` - Get topic analysis documents (requires auth)

## Usage

### 1. Register a User
```bash
curl -X POST "http://localhost:8000/api/auth/register" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "password123",
    "full_name": "John Doe"
  }'
```

### 2. Login and Get Token
```bash
curl -X POST "http://localhost:8000/api/auth/login" \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "password123"
  }'
```

### 3. Use Token for Protected Endpoints
```bash
curl -X GET "http://localhost:8000/api/topic-analysis/clusters?data_type=ticket&domain=banking" \
  -H "Authorization: Bearer YOUR_TOKEN_HERE"
```

## Testing

Run the test script to verify the authentication system:

```bash
cd backend/ranjith/api
python test_auth.py
```

## Security Notes

- Passwords are hashed using SHA-256
- JWT tokens expire after 60 days
- Admin endpoints require admin privileges
- All topic analysis endpoints now require authentication
- CORS is configured to allow all origins (modify for production)

## Database

The authentication system uses a `users` collection in MongoDB with the following structure:

```json
{
  "_id": "user_id",
  "email": "user@example.com",
  "password": "hashed_password",
  "full_name": "John Doe",
  "is_admin": false,
  "created_at": "2024-01-01T00:00:00Z",
  "updated_at": "2024-01-01T00:00:00Z"
}
``` 