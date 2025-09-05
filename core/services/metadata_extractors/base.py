"""
Base metadata extractor interface and common utilities
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
from uuid import UUID

from core.models.enums import AssetType


class MetadataExtractionStatus(str, Enum):
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass
class DatabaseMetadata:
    """Metadata for a database/catalog"""
    name: str
    qualified_name: str
    properties: Dict[str, Any]
    comment: Optional[str] = None


@dataclass
class SchemaMetadata:
    """Metadata for a schema/namespace"""
    name: str
    qualified_name: str
    database_name: str
    properties: Dict[str, Any]
    comment: Optional[str] = None


@dataclass
class TableMetadata:
    """Metadata for a table/view"""
    name: str
    qualified_name: str
    schema_name: str
    database_name: str
    table_type: str  # 'TABLE', 'VIEW', 'MATERIALIZED_VIEW'
    properties: Dict[str, Any]
    comment: Optional[str] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None


@dataclass
class ColumnMetadata:
    """Metadata for a column/field"""
    name: str
    table_qualified_name: str
    ordinal_position: int
    data_type: str
    is_nullable: bool
    default_value: Optional[str] = None
    comment: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference: Optional[str] = None
    properties: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.properties is None:
            self.properties = {}


@dataclass
class ExtractionResult:
    """Result of metadata extraction"""
    status: MetadataExtractionStatus
    databases: List[DatabaseMetadata]
    schemas: List[SchemaMetadata]
    tables: List[TableMetadata]
    columns: List[ColumnMetadata]
    errors: List[str] = None
    warnings: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []


class BaseMetadataExtractor(ABC):
    """Base class for all metadata extractors"""
    
    def __init__(self, data_source_id: UUID, tenant_id: UUID, config: Dict[str, Any], credentials: Dict[str, Any]):
        self.data_source_id = data_source_id
        self.tenant_id = tenant_id
        self.config = config
        self.credentials = credentials
    
    @abstractmethod
    async def extract_metadata(self) -> ExtractionResult:
        """
        Extract metadata from the data source
        
        Returns:
            ExtractionResult containing all discovered metadata
        """
        pass
    
    @abstractmethod
    async def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Test connection to the data source
        
        Returns:
            Tuple of (success, error_message)
        """
        pass
    
    @abstractmethod
    def get_supported_asset_types(self) -> List[AssetType]:
        """
        Get list of asset types supported by this extractor
        
        Returns:
            List of supported AssetType enums
        """
        pass
    
    def _build_qualified_name(self, *parts: str) -> str:
        """Build a qualified name from parts, filtering out None values"""
        clean_parts = [str(part) for part in parts if part is not None]
        return ".".join(clean_parts)
    
    def _sanitize_identifier(self, identifier: str) -> str:
        """Sanitize database identifiers for storage"""
        if not identifier:
            return ""
        # Remove quotes and normalize case if needed
        return identifier.strip().strip('"').strip("'").strip('`')
    
    def _normalize_data_type(self, raw_type: str) -> str:
        """Normalize database-specific data types to common format"""
        if not raw_type:
            return "UNKNOWN"
        
        # Convert to uppercase and remove size specifications for basic normalization
        normalized = raw_type.upper()
        
        # Basic type mappings
        type_mappings = {
            "INTEGER": "INT",
            "BIGINTEGER": "BIGINT",
            "DOUBLE PRECISION": "DOUBLE",
            "CHARACTER VARYING": "VARCHAR",
            "CHARACTER": "CHAR",
            "TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ",
            "TIMESTAMP WITHOUT TIME ZONE": "TIMESTAMP"
        }
        
        for original, normalized_type in type_mappings.items():
            if normalized.startswith(original):
                return normalized_type + normalized[len(original):]
        
        return normalized