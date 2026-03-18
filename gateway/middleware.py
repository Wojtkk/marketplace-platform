import logging
import time
from typing import Callable
from collections import defaultdict

import requests
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response, JSONResponse

logger = logging.getLogger(__name__)

AUTH_SERVICE_URL = "http://auth-service:8005"


class AuthMiddleware(BaseHTTPMiddleware):
    EXEMPT_PATHS = {"/api/health", "/docs", "/openapi.json"}

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        if request.url.path in self.EXEMPT_PATHS:
            return await call_next(request)

        auth_header = request.headers.get("Authorization")
        if not auth_header or not auth_header.startswith("Bearer "):
            return JSONResponse(
                status_code=401,
                content={"detail": "Missing or invalid Authorization header"},
            )

        token = auth_header.split("Bearer ", 1)[1]
        try:
            validation_response = requests.get(
                f"{AUTH_SERVICE_URL}/validate",
                headers={"Authorization": f"Bearer {token}"},
                timeout=5,
            )
            if validation_response.status_code != 200:
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Invalid or expired token"},
                )
            user_data = validation_response.json()
            request.state.user_id = user_data.get("user_id")
            request.state.user_roles = user_data.get("roles", [])
        except requests.ConnectionError:
            logger.error("Auth service unreachable")
            return JSONResponse(
                status_code=503,
                content={"detail": "Authentication service unavailable"},
            )
        except requests.Timeout:
            logger.error("Auth service timeout")
            return JSONResponse(
                status_code=504,
                content={"detail": "Authentication service timeout"},
            )

        return await call_next(request)


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, max_requests: int = 100, window_seconds: int = 60):
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._request_counts: dict[str, list[float]] = defaultdict(list)

    def _get_client_key(self, request: Request) -> str:
        forwarded = request.headers.get("X-Forwarded-For")
        if forwarded:
            return forwarded.split(",")[0].strip()
        return request.client.host if request.client else "unknown"

    def _is_rate_limited(self, client_key: str) -> bool:
        now = time.time()
        window_start = now - self.window_seconds
        self._request_counts[client_key] = [
            ts for ts in self._request_counts[client_key] if ts > window_start
        ]
        if len(self._request_counts[client_key]) >= self.max_requests:
            return True
        self._request_counts[client_key].append(now)
        return False

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        client_key = self._get_client_key(request)
        if self._is_rate_limited(client_key):
            return JSONResponse(
                status_code=429,
                content={"detail": "Rate limit exceeded. Try again later."},
                headers={"Retry-After": str(self.window_seconds)},
            )
        return await call_next(request)


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start_time = time.time()
        client_ip = request.client.host if request.client else "unknown"

        logger.info(
            "Incoming request: %s %s from %s",
            request.method,
            request.url.path,
            client_ip,
        )

        response = await call_next(request)

        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Response: %s %s -> %d (%.2fms)",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
        )

        response.headers["X-Request-Duration-Ms"] = f"{duration_ms:.2f}"
        return response
