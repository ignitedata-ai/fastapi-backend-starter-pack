"""
Databricks metadata extractor
"""
from typing import Dict, Any, List, Optional, Tuple
import httpx
import asyncio

from core.models.enums import AssetType
from core.logging import CentralizedLogger
from .base import (
    BaseMetadataExtractor,
    ExtractionResult,
    DatabaseMetadata,
    SchemaMetadata,
    TableMetadata,
    ColumnMetadata,
    MetadataExtractionStatus
)

logger = CentralizedLogger(__name__)


class DatabricksMetadataExtractor(BaseMetadataExtractor):
    """Databricks Unity Catalog metadata extractor"""
    
    def __init__(self, data_source_id, tenant_id, config: Dict[str, Any], credentials: Dict[str, Any]):
        super().__init__(data_source_id, tenant_id, config, credentials)
        
        # Databricks configuration
        self.workspace_url = config.get("workspace_url")  # e.g., https://dbc-abc123-def4.cloud.databricks.com
        self.catalog_name = config.get("catalog", "main")
        self.http_path = config.get("http_path")  # e.g., /sql/1.0/warehouses/abc123def456
        
        # Authentication
        self.access_token = credentials.get("access_token")  # Personal access token
        # Alternative: service principal authentication
        self.client_id = credentials.get("client_id")
        self.client_secret = credentials.get("client_secret")
        
        # HTTP client settings
        self.timeout = int(config.get("timeout", 30))
        
    def get_supported_asset_types(self) -> List[AssetType]:
        return [
            AssetType.DATABASE,  # Databricks Catalogs
            AssetType.SCHEMA,    # Databricks Schemas
            AssetType.TABLE,     # Tables and Delta tables
            AssetType.VIEW,      # Views
            AssetType.COLUMN     # Columns
        ]
    
    async def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test connection to Databricks"""
        try:
            headers = self._get_auth_headers()
            if not headers:
                return False, "Authentication credentials not configured"
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Test connection by listing catalogs
                response = await client.get(
                    f"{self.workspace_url}/api/2.1/unity-catalog/catalogs",
                    headers=headers
                )
                
                if response.status_code == 200:
                    return True, None
                else:
                    return False, f"HTTP {response.status_code}: {response.text}"
        
        except Exception as e:
            logger.error(
                "databricks_connection_test_failed",
                data_source_id=str(self.data_source_id),
                error=str(e)
            )
            return False, str(e)
    
    async def extract_metadata(self) -> ExtractionResult:
        """Extract metadata from Databricks Unity Catalog"""
        databases = []  # Catalogs in Databricks
        schemas = []
        tables = []
        columns = []
        errors = []
        warnings = []
        
        try:
            logger.info(
                "databricks_metadata_extraction_started",
                data_source_id=str(self.data_source_id),
                workspace_url=self.workspace_url,
                catalog=self.catalog_name
            )
            
            headers = self._get_auth_headers()
            if not headers:
                errors.append("Authentication credentials not configured")
                return ExtractionResult(
                    status=MetadataExtractionStatus.FAILED,
                    databases=databases,
                    schemas=schemas,
                    tables=tables,
                    columns=columns,
                    errors=errors,
                    warnings=warnings
                )
            
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Extract catalogs (databases)
                databases = await self._extract_catalogs(client, headers)
                
                # For each catalog, extract schemas
                for catalog in databases:
                    catalog_schemas = await self._extract_schemas(client, headers, catalog.name)
                    schemas.extend(catalog_schemas)
                    
                    # For each schema, extract tables
                    for schema in catalog_schemas:
                        if schema.database_name == catalog.name:  # Only process schemas for this catalog
                            schema_tables = await self._extract_tables(client, headers, catalog.name, schema.name)
                            tables.extend(schema_tables)
                            
                            # For each table, extract columns
                            for table in schema_tables:
                                table_columns = await self._extract_columns(client, headers, catalog.name, schema.name, table.name)
                                columns.extend(table_columns)
            
            status = MetadataExtractionStatus.SUCCESS
            if errors:
                status = MetadataExtractionStatus.PARTIAL if (databases or tables) else MetadataExtractionStatus.FAILED
            
            logger.info(
                "databricks_metadata_extraction_completed",
                data_source_id=str(self.data_source_id),
                catalogs_found=len(databases),
                schemas_found=len(schemas),
                tables_found=len(tables),
                columns_found=len(columns),
                errors=len(errors),
                warnings=len(warnings)
            )
            
            return ExtractionResult(
                status=status,
                databases=databases,
                schemas=schemas,
                tables=tables,
                columns=columns,
                errors=errors,
                warnings=warnings
            )
        
        except Exception as e:
            error_msg = f"Databricks metadata extraction failed: {str(e)}"
            logger.error(
                "databricks_metadata_extraction_error",
                data_source_id=str(self.data_source_id),
                error=error_msg
            )
            errors.append(error_msg)
            
            return ExtractionResult(
                status=MetadataExtractionStatus.FAILED,
                databases=databases,
                schemas=schemas,
                tables=tables,
                columns=columns,
                errors=errors,
                warnings=warnings
            )
    
    def _get_auth_headers(self) -> Optional[Dict[str, str]]:
        """Get authentication headers for Databricks API"""
        if self.access_token:
            return {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json"
            }
        elif self.client_id and self.client_secret:
            # For service principal authentication, you'd need to implement OAuth flow
            logger.warning("Service principal authentication not implemented yet")
            return None
        else:
            return None
    
    async def _extract_catalogs(self, client: httpx.AsyncClient, headers: Dict[str, str]) -> List[DatabaseMetadata]:
        """Extract catalog (database) information from Databricks"""
        catalogs = []
        
        try:
            response = await client.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/catalogs",
                headers=headers
            )
            response.raise_for_status()
            
            data = response.json()
            
            for catalog_data in data.get("catalogs", []):
                catalog_name = catalog_data.get("name")
                
                # Skip if we're filtering to a specific catalog and this isn't it
                if self.catalog_name != "*" and catalog_name != self.catalog_name:
                    continue
                
                catalog = DatabaseMetadata(
                    name=self._sanitize_identifier(catalog_name),
                    qualified_name=self._build_qualified_name("databricks", self.workspace_url.split("//")[1], catalog_name),
                    properties={
                        "workspace_url": self.workspace_url,
                        "catalog_type": catalog_data.get("catalog_type"),
                        "provider": catalog_data.get("provider_name"),
                        "share_name": catalog_data.get("share_name"),
                        "created_at": catalog_data.get("created_at"),
                        "updated_at": catalog_data.get("updated_at"),
                        "connector_type": "databricks"
                    },
                    comment=catalog_data.get("comment")
                )
                
                catalogs.append(catalog)
        
        except Exception as e:
            logger.error(
                "databricks_extract_catalogs_failed",
                data_source_id=str(self.data_source_id),
                error=str(e)
            )
            raise
        
        return catalogs
    
    async def _extract_schemas(self, client: httpx.AsyncClient, headers: Dict[str, str], catalog_name: str) -> List[SchemaMetadata]:
        """Extract schema information from a Databricks catalog"""
        schemas = []
        
        try:
            response = await client.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/schemas",
                headers=headers,
                params={"catalog_name": catalog_name}
            )
            response.raise_for_status()
            
            data = response.json()
            
            for schema_data in data.get("schemas", []):
                schema_name = schema_data.get("name")
                
                schema = SchemaMetadata(
                    name=self._sanitize_identifier(schema_name),
                    qualified_name=self._build_qualified_name("databricks", self.workspace_url.split("//")[1], catalog_name, schema_name),
                    database_name=catalog_name,
                    properties={
                        "catalog_name": catalog_name,
                        "owner": schema_data.get("owner"),
                        "created_at": schema_data.get("created_at"),
                        "updated_at": schema_data.get("updated_at"),
                        "connector_type": "databricks"
                    },
                    comment=schema_data.get("comment")
                )
                
                schemas.append(schema)
        
        except Exception as e:
            logger.error(
                "databricks_extract_schemas_failed",
                data_source_id=str(self.data_source_id),
                catalog=catalog_name,
                error=str(e)
            )
            raise
        
        return schemas
    
    async def _extract_tables(self, client: httpx.AsyncClient, headers: Dict[str, str], catalog_name: str, schema_name: str) -> List[TableMetadata]:
        """Extract table information from a Databricks schema"""
        tables = []
        
        try:
            response = await client.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/tables",
                headers=headers,
                params={
                    "catalog_name": catalog_name,
                    "schema_name": schema_name
                }
            )
            response.raise_for_status()
            
            data = response.json()
            
            for table_data in data.get("tables", []):
                table_name = table_data.get("name")
                table_type = table_data.get("table_type", "TABLE")
                
                # Map Databricks table types
                if table_type in ["VIEW", "MATERIALIZED_VIEW"]:
                    asset_type = "VIEW"
                else:
                    asset_type = "TABLE"
                
                table = TableMetadata(
                    name=self._sanitize_identifier(table_name),
                    qualified_name=self._build_qualified_name("databricks", self.workspace_url.split("//")[1], catalog_name, schema_name, table_name),
                    schema_name=schema_name,
                    database_name=catalog_name,
                    table_type=asset_type,
                    properties={
                        "catalog_name": catalog_name,
                        "schema_name": schema_name,
                        "table_type": table_type,
                        "data_source_format": table_data.get("data_source_format"),
                        "storage_location": table_data.get("storage_location"),
                        "owner": table_data.get("owner"),
                        "created_at": table_data.get("created_at"),
                        "updated_at": table_data.get("updated_at"),
                        "connector_type": "databricks"
                    },
                    comment=table_data.get("comment")
                )
                
                tables.append(table)
        
        except Exception as e:
            logger.error(
                "databricks_extract_tables_failed",
                data_source_id=str(self.data_source_id),
                catalog=catalog_name,
                schema=schema_name,
                error=str(e)
            )
            raise
        
        return tables
    
    async def _extract_columns(self, client: httpx.AsyncClient, headers: Dict[str, str], catalog_name: str, schema_name: str, table_name: str) -> List[ColumnMetadata]:
        """Extract column information from a Databricks table"""
        columns = []
        
        try:
            # Get table info which includes column information
            response = await client.get(
                f"{self.workspace_url}/api/2.1/unity-catalog/tables/{catalog_name}.{schema_name}.{table_name}",
                headers=headers
            )
            response.raise_for_status()
            
            table_info = response.json()
            columns_data = table_info.get("columns", [])
            
            for i, column_data in enumerate(columns_data):
                column_name = column_data.get("name")
                data_type = self._normalize_data_type(column_data.get("type_text", ""))
                
                column = ColumnMetadata(
                    name=self._sanitize_identifier(column_name),
                    table_qualified_name=self._build_qualified_name("databricks", self.workspace_url.split("//")[1], catalog_name, schema_name, table_name),
                    ordinal_position=i + 1,
                    data_type=data_type,
                    is_nullable=column_data.get("nullable", True),
                    default_value=None,  # Databricks API doesn't always provide this
                    comment=column_data.get("comment"),
                    is_primary_key=False,  # Would need additional logic to determine this
                    is_foreign_key=False,  # Would need additional logic to determine this
                    properties={
                        "catalog_name": catalog_name,
                        "schema_name": schema_name,
                        "table_name": table_name,
                        "type_text": column_data.get("type_text"),
                        "type_name": column_data.get("type_name"),
                        "type_precision": column_data.get("type_precision"),
                        "type_scale": column_data.get("type_scale"),
                        "connector_type": "databricks"
                    }
                )
                
                columns.append(column)
        
        except Exception as e:
            logger.error(
                "databricks_extract_columns_failed",
                data_source_id=str(self.data_source_id),
                catalog=catalog_name,
                schema=schema_name,
                table=table_name,
                error=str(e)
            )
            raise
        
        return columns