from fastapi import Depends, HTTPException, status

from . import jwt
from .constants import ErrorCode
from .schemas import JWTData
from dependencies import get_database


async def get_current_user(
    jwt_data: JWTData = Depends(jwt.parse_jwt_user_data),
    db=Depends(get_database),
) -> dict:
    """Get the current authenticated user"""
    from .service import AuthService

    auth_service = AuthService(db)
    user = await auth_service.get_user_by_id(jwt_data.user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=ErrorCode.AUTHORIZATION_FAILED,
        )

    return user


async def get_current_admin(user: dict = Depends(get_current_user)) -> dict:
    """Check if the current user is an admin"""
    if not user.get("is_admin", False):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=ErrorCode.AUTHORIZATION_FAILED,
        )

    return user 