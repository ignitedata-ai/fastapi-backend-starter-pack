import uuid
import re
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from core.api.v1.schemas.base import BaseSchema
from core.models.enums import ConnectorKind, RunStatus, RunType


def simple_slugify(text: str) -> str:
    """Simple Python 3 compatible slugify function"""
    # Convert to lowercase and replace spaces and special chars with hyphens
    slug = re.sub(r'[^\w\s-]', '', text.lower())
    slug = re.sub(r'[\s_-]+', '-', slug)
    return slug.strip('-')


class DataSourceBase(BaseModel):
    """Base data source schema"""

    name: str = Field(..., description="Data source display name")
    slug: Optional[str] = Field(default=None, description="URL-friendly identifier")
    connector_key: str = Field(..., description="Connector type identifier")
    connector_version: str = Field(..., description="Connector version")
    tags: List[str] = Field(default_factory=list, description="Tags for categorization")

    @model_validator(mode="before")
    @classmethod
    def auto_slug(cls, data):
        # data is the raw input dict
        if isinstance(data, dict) and not data.get("slug") and data.get("name"):
            data = {**data}
            data["slug"] = simple_slugify(f"{data['name']}-{uuid.uuid4()}")
        return data


class DataSourceCreate(DataSourceBase):
    """
    Create data source schema with separated config and credentials

    Example:
    {
        "name": "Production Database",
        "connector_key": "postgresql",
        "connector_version": "1.0.0",
        "config": {
            "host": "db.example.com",
            "port": 5432,
            "database": "production"
        },
        "credentials": {
            "username": "db_user",
            "password": "secret_password"
        }
    }
    """

    config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Non-sensitive configuration (host, port, database, etc.)",
    )
    credentials: Dict[str, Any] = Field(
        default_factory=dict,
        description="Sensitive credentials (passwords, tokens, API keys, etc.)",
    )
    network_profile: Dict[str, Any] = Field(
        default_factory=dict, description="Network configuration (proxy, VPN, etc.)"
    )


class DataSourceUpdate(BaseModel):
    """Update data source schema with optional config and credentials"""

    name: Optional[str] = None
    config: Optional[Dict[str, Any]] = Field(
        None, description="Update non-sensitive configuration"
    )
    credentials: Optional[Dict[str, Any]] = Field(
        None, description="Update sensitive credentials (will be encrypted)"
    )
    network_profile: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    connection_status: Optional[str] = None


class DataSourceResponse(BaseSchema):
    """Data source response schema with separated config and credentials"""

    id: UUID
    tenant_id: UUID
    name: str
    slug: str
    connector_key: str
    connector_version: str
    kind: ConnectorKind
    config: Dict[str, Any] = Field(
        default_factory=dict,
        description="Public configuration (non-sensitive fields only)",
    )
    has_credentials: bool = Field(default=True)
    credentials: Optional[Dict[str, Any]] = Field(
        None,
        description="Sensitive credentials (passwords, tokens, API keys, etc.)",
    )
    network_profile: Dict[str, Any]
    tags: List[str]
    connection_status: str
    created_at: datetime
    updated_at: datetime
    last_health_at: Optional[datetime] = None

    # Hidden field to ingest ORM attribute but not serialize it
    secret_uri: Optional[str] = Field(default=None, exclude=True, repr=False)

    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    @model_validator(mode="after")
    def set_has_credentials(self):
        # Set has_credentials based on credentials field
        self.has_credentials = bool(self.credentials)
        return self

    @classmethod
    def from_orm(cls, obj):
        # Legacy method - now handled directly in API endpoints
        # This method is kept for backward compatibility with list endpoints
        data = {
            "id": obj.id,
            "tenant_id": obj.tenant_id,
            "name": obj.name,
            "slug": obj.slug,
            "connector_key": obj.connector_key,
            "connector_version": obj.connector_version,
            "kind": obj.kind,
            "config": obj.config_json or {},  # For list view, show combined config
            "has_credentials": bool(getattr(obj, "secret_uri", None)),
            "network_profile": obj.network_profile,
            "tags": obj.tags,
            "connection_status": obj.connection_status,
            "created_at": obj.created_at,
            "updated_at": obj.updated_at,
            "last_health_at": obj.last_health_at,
        }
        return cls(**data)


class DataSourceStatus(BaseModel):
    """Data source status"""

    id: UUID
    name: str
    connection_status: str
    metadata_status: Optional[RunStatus] = None
    profile_status: Optional[RunStatus] = None
    last_metadata_run: Optional[datetime] = None
    last_profile_run: Optional[datetime] = None
    assets_count: int = 0
    fields_count: int = 0


class DataSourceListResponse(BaseModel):
    """List of data sources response"""

    items: List[DataSourceResponse]
    total: int
    skip: int = 0
    limit: int = 100


class RunDataSourceRequest(BaseModel):
    """Request to run data source operation"""

    run_type: RunType
    params: Dict[str, Any] = Field(default_factory=dict)
