"""Example: JWT authentication handler for WSE."""
import logging

logger = logging.getLogger("wse.examples.auth")


async def jwt_auth_handler(websocket) -> str | None:
    """
    Example auth handler using PyJWT.

    WSE calls this during connection setup. Return a user_id string
    to allow the connection, or None to reject it.

    Install: pip install pyjwt[crypto]
    """
    import jwt  # noqa: E402 - local import for optional dependency

    SECRET_KEY = "your-secret-key"  # Load from environment in production
    ALGORITHM = "HS256"

    # Try query parameter first (standard for WebSocket)
    token = websocket.query_params.get("token")

    # Fallback to Authorization header
    if not token:
        auth_header = websocket.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]

    # Fallback to cookie
    if not token:
        token = websocket.cookies.get("access_token")

    if not token:
        logger.warning("No authentication token provided")
        return None

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("sub")
        if not user_id:
            logger.warning("Token missing 'sub' claim")
            return None
        return user_id
    except jwt.ExpiredSignatureError:
        logger.warning("Token expired")
        return None
    except jwt.InvalidTokenError as e:
        logger.warning("Invalid token: %s", e)
        return None
