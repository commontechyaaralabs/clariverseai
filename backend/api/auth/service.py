import hashlib
import secrets
from datetime import datetime
from typing import Optional

from pymongo import database

from .exceptions import InvalidCredentials, EmailTaken
from .schemas import UserCreate, UserAuth, UserResponse


class AuthService:
    def __init__(self, db: database.Database):
        self.db = db
        self.users_collection = db["users"]

    def _hash_password(self, password: str) -> str:
        """Hash a password using SHA-256"""
        return hashlib.sha256(password.encode()).hexdigest()

    def _verify_password(self, password: str, hashed_password: str) -> bool:
        """Verify a password against its hash"""
        return self._hash_password(password) == hashed_password

    def _generate_user_id(self) -> str:
        """Generate a unique user ID"""
        return secrets.token_urlsafe(16)

    async def create_user(self, user_data: UserCreate) -> UserResponse:
        """Create a new user"""
        # Check if email already exists
        existing_user = self.users_collection.find_one({"email": user_data.email})
        if existing_user:
            raise EmailTaken()

        # Create user document
        user_id = self._generate_user_id()
        hashed_password = self._hash_password(user_data.password)
        
        user_doc = {
            "_id": user_id,
            "email": user_data.email,
            "password": hashed_password,
            "full_name": user_data.full_name,
            "is_admin": False,
            "created_at": datetime.utcnow(),
            "updated_at": datetime.utcnow()
        }
        
        self.users_collection.insert_one(user_doc)
        
        return UserResponse(
            id=user_id,
            email=user_data.email,
            full_name=user_data.full_name,
            created_at=user_doc["created_at"]
        )

    async def authenticate_user(self, user_data: UserAuth) -> Optional[dict]:
        """Authenticate a user with email and password"""
        user = self.users_collection.find_one({"email": user_data.email})
        
        if not user:
            return None
            
        if not self._verify_password(user_data.password, user["password"]):
            return None
            
        return user

    async def get_user_by_id(self, user_id: str) -> Optional[dict]:
        """Get user by ID"""
        return self.users_collection.find_one({"_id": user_id})

    async def get_user_by_email(self, email: str) -> Optional[dict]:
        """Get user by email"""
        return self.users_collection.find_one({"email": email})

    async def update_user(self, user_id: str, update_data: dict) -> Optional[dict]:
        """Update user information"""
        update_data["updated_at"] = datetime.utcnow()
        result = self.users_collection.update_one(
            {"_id": user_id},
            {"$set": update_data}
        )
        
        if result.modified_count > 0:
            return await self.get_user_by_id(user_id)
        return None

    async def delete_user(self, user_id: str) -> bool:
        """Delete a user"""
        result = self.users_collection.delete_one({"_id": user_id})
        return result.deleted_count > 0

    async def list_users(self, skip: int = 0, limit: int = 100) -> list[dict]:
        """List all users with pagination"""
        cursor = self.users_collection.find().skip(skip).limit(limit)
        return list(cursor) 