from datetime import datetime
from typing import Optional, Any
from pydantic import BaseModel, Field
from uuid import UUID


class BaseSchema(BaseModel):
    """Base schema with common fields"""
    
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }


class PaginationParams(BaseModel):
    """Pagination parameters"""
    page: int = Field(default=1, ge=1)
    page_size: int = Field(default=20, ge=1, le=100)
    sort_by: Optional[str] = Field(default="created_at")
    sort_order: str = Field(default="desc", pattern="^(asc|desc)$")


class PaginatedResponse(BaseModel):
    """Paginated response wrapper"""
    items: list[Any]
    total: int
    page: int
    page_size: int
    total_pages: int


class ErrorResponse(BaseModel):
    """Error response"""
    error: str
    message: str
    details: Optional[dict] = None
    request_id: Optional[str] = None