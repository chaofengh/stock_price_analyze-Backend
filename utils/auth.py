import os
from dataclasses import dataclass
from typing import Any, Dict, Optional

import jwt


JWT_SECRET = os.getenv("JWT_SECRET", "your_jwt_secret")
JWT_ALGORITHM = "HS256"


@dataclass(frozen=True)
class AuthResult:
    user_id: int
    payload: Dict[str, Any]


class AuthError(Exception):
    pass


def _parse_bearer_token(authorization_header: Optional[str]) -> str:
    if not authorization_header:
        raise AuthError("Missing Authorization header")
    parts = authorization_header.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer" or not parts[1].strip():
        raise AuthError("Invalid Authorization header")
    return parts[1].strip()


def authenticate_bearer_token(authorization_header: Optional[str]) -> AuthResult:
    token = _parse_bearer_token(authorization_header)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError as exc:
        raise AuthError("Token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise AuthError("Invalid token") from exc

    raw_user_id = payload.get("user_id")
    try:
        user_id = int(raw_user_id)
    except Exception as exc:
        raise AuthError("Invalid token payload") from exc

    return AuthResult(user_id=user_id, payload=payload)
