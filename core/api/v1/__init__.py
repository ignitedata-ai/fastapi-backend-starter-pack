from fastapi import APIRouter

api_router = APIRouter()

# Include all route modules
from core.api.v1.routes.connector import router as connector_router
from core.api.v1.routes.data_source import router as data_source_router
api_router.include_router(connector_router)
api_router.include_router(data_source_router)


__all__ = ["api_router"]
