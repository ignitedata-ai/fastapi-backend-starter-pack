from fastapi import APIRouter

api_router = APIRouter(prefix="/v1")

# Include all route modules


__all__ = ["api_router"]
