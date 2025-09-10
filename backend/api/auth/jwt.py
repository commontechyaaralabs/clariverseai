import time
from typing import Optional

from fastapi import Depends, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt

from .auth_config import auth_config
from .constants import ErrorCode
from .exceptions import AuthError
from .schemas import JWTData

security = HTTPBearer()


def create_access_token(user_id: str, is_admin: bool = False) -> str:
    """Create a JWT access token for the user"""
    payload = {
        "user_id": user_id,
        "is_admin": is_admin,
        "exp": time.time() + auth_config.JWT_EXP,
    }
    return jwt.encode(payload, auth_config.JWT_SECRET, algorithm=auth_config.JWT_ALG)


def parse_jwt_user_data(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> JWTData:
    """Parse JWT token and return user data"""
    token = credentials.credentials
    try:
        payload = jwt.decode(token, auth_config.JWT_SECRET, algorithms=[auth_config.JWT_ALG])
    except JWTError:
        raise AuthError(ErrorCode.INVALID_TOKEN)

    return JWTData(**payload)


def parse_jwt_user_data_optional(
    request: Request,
) -> Optional[JWTData]:
    """Parse JWT token from request (if any) and return user data or None"""
    authorization: str = request.headers.get("Authorization", "")

    if not authorization:
        return None

    try:
        scheme, token = authorization.split()
        if scheme.lower() != "bearer":
            return None
    except ValueError:
        return None

    try:
        payload = jwt.decode(token, auth_config.JWT_SECRET, algorithms=[auth_config.JWT_ALG])
        return JWTData(**payload)
    except JWTError:
        return None 