from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, Field

from core.api.v1.schemas.base import BaseSchema
from core.models.enums import ConnectorKind


class ConnectorCapabilities(BaseModel):
    """Connector capabilities"""

    discover: bool = False
    profile: bool = False
    sample: bool = False
    lineage: bool = False
    sync: bool = False


class ConnectorDefinitionBase(BaseModel):
    """Base connector definition schema"""

    key: str
    version: str
    kind: ConnectorKind
    display_name: str
    capabilities: ConnectorCapabilities
    connection_schema: Dict[str, Any] = Field(default_factory=dict)
    secret_schema: Dict[str, Any] = Field(default_factory=dict)
    image_ref: Optional[str] = None
    docs_url: Optional[str] = None
    is_enabled: bool = True


class ConnectorDefinitionCreate(ConnectorDefinitionBase):
    """Create connector definition schema"""

    pass


class ConnectorDefinitionUpdate(BaseModel):
    """Update connector definition schema"""

    display_name: Optional[str] = None
    capabilities: Optional[ConnectorCapabilities] = None
    connection_schema: Optional[Dict[str, Any]] = None
    secret_schema: Optional[Dict[str, Any]] = None
    image_ref: Optional[str] = None
    docs_url: Optional[str] = None
    is_enabled: Optional[bool] = None


class ConnectorDefinitionResponse(ConnectorDefinitionBase, BaseSchema):
    """Connector definition response schema"""

    id: UUID
    created_at: datetime


class ConnectorDefinitionList(BaseModel):
    """List of connector definitions"""

    connectors: List[ConnectorDefinitionResponse]
    total: int


class TestConnectionRequest(BaseModel):
    """Test connection request"""

    connector_key: str
    connector_version: str
    config: Dict[str, Any]
    secrets: Optional[Dict[str, Any]] = None


class TestConnectionResponse(BaseModel):
    """Test connection response"""

    success: bool
    message: str
    details: Optional[Dict[str, Any]] = None
