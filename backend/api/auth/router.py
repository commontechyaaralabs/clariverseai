from fastapi import APIRouter, Depends, HTTPException, status
from pymongo import database

from dependencies import get_database
from .dependencies import get_current_user, get_current_admin
from .exceptions import InvalidCredentials
from .jwt import create_access_token
from .schemas import UserCreate, UserAuth, UserResponse, AccessTokenResponse
from .service import AuthService

router = APIRouter(prefix="/auth", tags=["authentication"])


@router.post("/register", response_model=UserResponse)
async def register(
    user_data: UserCreate,
    db: database.Database = Depends(get_database)
):
    """Register a new user"""
    auth_service = AuthService(db)
    return await auth_service.create_user(user_data)


@router.post("/login", response_model=AccessTokenResponse)
async def login(
    user_data: UserAuth,
    db: database.Database = Depends(get_database)
):
    """Login user and return access token"""
    auth_service = AuthService(db)
    user = await auth_service.authenticate_user(user_data)
    
    if not user:
        raise InvalidCredentials()
    
    access_token = create_access_token(
        user_id=user["_id"],
        is_admin=user.get("is_admin", False)
    )
    
    return AccessTokenResponse(access_token=access_token)


@router.get("/me", response_model=UserResponse)
async def get_current_user_info(
    current_user: dict = Depends(get_current_user)
):
    """Get current user information"""
    return UserResponse(
        id=current_user["_id"],
        email=current_user["email"],
        full_name=current_user["full_name"],
        created_at=current_user["created_at"]
    )


@router.get("/users", response_model=list[UserResponse])
async def list_users(
    skip: int = 0,
    limit: int = 100,
    current_admin: dict = Depends(get_current_admin),
    db: database.Database = Depends(get_database)
):
    """List all users (admin only)"""
    auth_service = AuthService(db)
    users = await auth_service.list_users(skip=skip, limit=limit)
    
    return [
        UserResponse(
            id=user["_id"],
            email=user["email"],
            full_name=user["full_name"],
            created_at=user["created_at"]
        )
        for user in users
    ]


@router.get("/users/{user_id}", response_model=UserResponse)
async def get_user(
    user_id: str,
    current_admin: dict = Depends(get_current_admin),
    db: database.Database = Depends(get_database)
):
    """Get user by ID (admin only)"""
    auth_service = AuthService(db)
    user = await auth_service.get_user_by_id(user_id)
    
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return UserResponse(
        id=user["_id"],
        email=user["email"],
        full_name=user["full_name"],
        created_at=user["created_at"]
    )


@router.delete("/users/{user_id}")
async def delete_user(
    user_id: str,
    current_admin: dict = Depends(get_current_admin),
    db: database.Database = Depends(get_database)
):
    """Delete user (admin only)"""
    auth_service = AuthService(db)
    success = await auth_service.delete_user(user_id)
    
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found"
        )
    
    return {"message": "User deleted successfully"} 