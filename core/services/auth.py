"""
Authentication and authorization utilities
"""
from typing import Optional
from uuid import UUID
from fastapi import Depends, HTTPException, Request, status
from core.config import settings


# Default tenant ID for development
DEFAULT_TENANT_ID = UUID("00000000-0000-0000-0000-000000000001")


async def get_current_tenant_id(request: Request) -> UUID:
    """
    Extract tenant ID from request
    
    Priority:
    1. From request.state (set by middleware)
    2. From x-tenant-id header
    3. Default tenant (in development mode only)
    """
    # Try from request state (set by middleware)
    if hasattr(request.state, "tenant_id") and request.state.tenant_id:
        try:
            return UUID(request.state.tenant_id)
        except (ValueError, TypeError):
            pass
    
    # Try from header directly
    tenant_header = request.headers.get("x-tenant-id")
    if tenant_header:
        try:
            return UUID(tenant_header)
        except (ValueError, TypeError):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid tenant ID format"
            )
    
    # In development, use default tenant
    if settings.app_env == "development":
        return DEFAULT_TENANT_ID
    
    # In production, tenant ID is required
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Tenant ID is required. Please provide X-Tenant-ID header"
    )


async def get_optional_tenant_id(request: Request) -> Optional[UUID]:
    """
    Get tenant ID if available, otherwise return None
    """
    try:
        return await get_current_tenant_id(request)
    except HTTPException:
        return None
    

async def get_current_user(request: Request) -> list[UUID]:
    """
    Get the current user ID from the request
    """
    # Example: return a single UUID (replace with actual logic as needed)
    return [
        UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf6"),
        UUID("f81d4fae-7dec-11d0-a765-00a0c91e6bf7"),
    ]