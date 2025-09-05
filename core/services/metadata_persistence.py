"""
Service for persisting metadata to the database
"""
from typing import Dict, Any, List, Optional
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, delete, update
from sqlalchemy.dialects.postgresql import insert

from core.models import Asset, AssetField
from core.models.enums import AssetType
from core.logging import CentralizedLogger
from core.services.metadata_extractors.base import (
    ExtractionResult,
    DatabaseMetadata,
    SchemaMetadata,
    TableMetadata,
    ColumnMetadata
)

logger = CentralizedLogger(__name__)


class MetadataPersistenceService:
    """Service for persisting extracted metadata to the database"""
    
    def __init__(self, db: AsyncSession, data_source_id: UUID, tenant_id: UUID):
        self.db = db
        self.data_source_id = data_source_id
        self.tenant_id = tenant_id
    
    async def persist_metadata(self, extraction_result: ExtractionResult) -> Dict[str, int]:
        """
        Persist extracted metadata to the database
        
        Args:
            extraction_result: Result from metadata extraction
            
        Returns:
            Dictionary with counts of persisted entities
        """
        counts = {
            "databases": 0,
            "schemas": 0,
            "tables": 0,
            "columns": 0,
            "errors": 0
        }
        
        try:
            # Clear existing metadata for this data source
            logger.info("starting_cleanup", data_source_id=str(self.data_source_id))
            await self._cleanup_existing_metadata()
            logger.info("cleanup_completed", data_source_id=str(self.data_source_id))
            
            # Persist in order: databases -> schemas -> tables -> columns
            logger.info("persisting_databases", count=len(extraction_result.databases))
            counts["databases"] = await self._persist_databases(extraction_result.databases)
            logger.info("databases_persisted", count=counts["databases"])
            
            logger.info("persisting_schemas", count=len(extraction_result.schemas))
            counts["schemas"] = await self._persist_schemas(extraction_result.schemas)
            logger.info("schemas_persisted", count=counts["schemas"])
            
            logger.info("persisting_tables", count=len(extraction_result.tables))
            counts["tables"] = await self._persist_tables(extraction_result.tables)
            logger.info("tables_persisted", count=counts["tables"])
            
            logger.info("persisting_columns", count=len(extraction_result.columns))
            counts["columns"] = await self._persist_columns(extraction_result.columns)
            logger.info("columns_persisted", count=counts["columns"])
            
            # Commit all changes
            logger.info("committing_transaction")
            await self.db.commit()
            logger.info("transaction_committed")
            
            logger.info(
                "metadata_persisted_successfully",
                data_source_id=str(self.data_source_id),
                tenant_id=str(self.tenant_id),
                counts=counts
            )
            
        except Exception as e:
            await self.db.rollback()
            counts["errors"] = 1
            logger.error(
                "metadata_persistence_failed",
                data_source_id=str(self.data_source_id),
                tenant_id=str(self.tenant_id),
                error=str(e),
                error_type=type(e).__name__,
                extraction_result_summary={
                    "databases": len(extraction_result.databases),
                    "schemas": len(extraction_result.schemas), 
                    "tables": len(extraction_result.tables),
                    "columns": len(extraction_result.columns)
                }
            )
            import traceback
            logger.error("full_traceback", traceback=traceback.format_exc())
            raise
        
        return counts
    
    async def _cleanup_existing_metadata(self):
        """Remove existing metadata for this data source"""
        try:
            # Delete in reverse order due to foreign key constraints
            # AssetFields first
            await self.db.execute(
                delete(AssetField).where(
                    AssetField.tenant_id == self.tenant_id,
                    AssetField.asset_id.in_(
                        select(Asset.id).where(
                            Asset.data_source_id == self.data_source_id,
                            Asset.tenant_id == self.tenant_id
                        )
                    )
                )
            )
            
            # Then Assets
            await self.db.execute(
                delete(Asset).where(
                    Asset.data_source_id == self.data_source_id,
                    Asset.tenant_id == self.tenant_id
                )
            )
            
            logger.info(
                "existing_metadata_cleaned",
                data_source_id=str(self.data_source_id),
                tenant_id=str(self.tenant_id)
            )
            
        except Exception as e:
            logger.error(
                "metadata_cleanup_failed",
                data_source_id=str(self.data_source_id),
                tenant_id=str(self.tenant_id),
                error=str(e)
            )
            raise
    
    async def _persist_databases(self, databases: List[DatabaseMetadata]) -> int:
        """Persist database assets"""
        count = 0
        
        for db_meta in databases:
            try:
                asset = Asset(
                    tenant_id=self.tenant_id,
                    data_source_id=self.data_source_id,
                    type=AssetType.DATABASE,
                    qualified_name=db_meta.qualified_name,
                    display_name=db_meta.name,
                    native_identity={
                        "name": db_meta.name,
                        "type": "database"
                    },
                    properties=db_meta.properties
                )
                
                self.db.add(asset)
                count += 1
                
            except Exception as e:
                logger.warning(
                    "database_persistence_failed",
                    database_name=db_meta.name,
                    error=str(e)
                )
                continue
        
        # Flush to get IDs for child objects
        try:
            await self.db.flush()
        except Exception as e:
            logger.error(
                "database_flush_failed",
                error=str(e),
                error_type=type(e).__name__,
                count=count
            )
            raise
        return count
    
    async def _persist_schemas(self, schemas: List[SchemaMetadata]) -> int:
        """Persist schema assets"""
        count = 0
        
        # Get database assets to link schemas to their parents
        database_assets = await self._get_assets_by_type(AssetType.DATABASE)
        db_lookup = {asset.display_name: asset for asset in database_assets}
        
        for schema_meta in schemas:
            try:
                parent_asset = db_lookup.get(schema_meta.database_name)
                
                asset = Asset(
                    tenant_id=self.tenant_id,
                    data_source_id=self.data_source_id,
                    type=AssetType.SCHEMA,
                    qualified_name=schema_meta.qualified_name,
                    display_name=schema_meta.name,
                    parent_id=parent_asset.id if parent_asset else None,
                    native_identity={
                        "name": schema_meta.name,
                        "database_name": schema_meta.database_name,
                        "type": "schema"
                    },
                    properties=schema_meta.properties
                )
                
                self.db.add(asset)
                count += 1
                
            except Exception as e:
                logger.warning(
                    "schema_persistence_failed",
                    schema_name=schema_meta.name,
                    database_name=schema_meta.database_name,
                    error=str(e)
                )
                continue
        
        try:
            await self.db.flush()
        except Exception as e:
            logger.error(
                "schema_flush_failed",
                error=str(e),
                error_type=type(e).__name__,
                count=count
            )
            raise
        return count
    
    async def _persist_tables(self, tables: List[TableMetadata]) -> int:
        """Persist table assets"""
        count = 0
        
        # Get schema assets to link tables to their parents
        schema_assets = await self._get_assets_by_type(AssetType.SCHEMA)
        schema_lookup = {asset.display_name: asset for asset in schema_assets}
        
        for table_meta in tables:
            try:
                parent_asset = schema_lookup.get(table_meta.schema_name)
                
                # Determine asset type
                asset_type = AssetType.VIEW if table_meta.table_type == 'VIEW' else AssetType.TABLE
                
                asset = Asset(
                    tenant_id=self.tenant_id,
                    data_source_id=self.data_source_id,
                    type=asset_type,
                    qualified_name=table_meta.qualified_name,
                    display_name=table_meta.name,
                    parent_id=parent_asset.id if parent_asset else None,
                    native_identity={
                        "name": table_meta.name,
                        "schema_name": table_meta.schema_name,
                        "database_name": table_meta.database_name,
                        "table_type": table_meta.table_type,
                        "type": "table"
                    },
                    properties={
                        **table_meta.properties,
                        "row_count": int(table_meta.row_count) if table_meta.row_count is not None else None,
                        "size_bytes": int(table_meta.size_bytes) if table_meta.size_bytes is not None else None,
                        "comment": table_meta.comment
                    }
                )
                
                self.db.add(asset)
                count += 1
                
            except Exception as e:
                logger.error(
                    "table_persistence_failed",
                    table_name=table_meta.name,
                    schema_name=table_meta.schema_name,
                    qualified_name=table_meta.qualified_name,
                    table_type=table_meta.table_type,
                    row_count=table_meta.row_count,
                    row_count_type=type(table_meta.row_count).__name__,
                    size_bytes=table_meta.size_bytes,
                    size_bytes_type=type(table_meta.size_bytes).__name__,
                    properties=str(table_meta.properties),
                    error=str(e),
                    error_type=type(e).__name__
                )
                import traceback
                logger.error("table_persistence_traceback", traceback=traceback.format_exc())
                continue
        
        try:
            await self.db.flush()
        except Exception as e:
            logger.error(
                "table_flush_failed",
                error=str(e),
                error_type=type(e).__name__,
                count=count
            )
            raise
        return count
    
    async def _persist_columns(self, columns: List[ColumnMetadata]) -> int:
        """Persist column fields"""
        count = 0
        
        # Get table assets to link columns to their parents
        table_assets = await self._get_assets_by_type([AssetType.TABLE, AssetType.VIEW])
        table_lookup = {asset.qualified_name: asset for asset in table_assets}
        
        for col_meta in columns:
            try:
                parent_asset = table_lookup.get(col_meta.table_qualified_name)
                if not parent_asset:
                    logger.warning(
                        "parent_table_not_found_for_column",
                        column_name=col_meta.name,
                        table_qualified_name=col_meta.table_qualified_name
                    )
                    continue
                
                # Ensure ordinal_position is an integer
                ordinal_pos = int(col_meta.ordinal_position) if col_meta.ordinal_position is not None else None
                
                # Handle default_expression - ensure it's a string and not too long
                default_expr = None
                if col_meta.default_value is not None:
                    default_expr = str(col_meta.default_value)
                    if len(default_expr) > 1000:  # Truncate if too long
                        default_expr = default_expr[:1000]
                
                field = AssetField(
                    tenant_id=self.tenant_id,
                    asset_id=parent_asset.id,
                    name=col_meta.name,
                    ordinal_position=ordinal_pos,
                    data_type=col_meta.data_type,
                    is_nullable=col_meta.is_nullable,
                    default_expression=default_expr,
                    comment=col_meta.comment,
                    properties={
                        **col_meta.properties,
                        "is_primary_key": col_meta.is_primary_key,
                        "is_foreign_key": col_meta.is_foreign_key,
                        "foreign_key_reference": col_meta.foreign_key_reference
                    }
                )
                
                self.db.add(field)
                count += 1
                
            except Exception as e:
                logger.error(
                    "column_persistence_failed",
                    column_name=col_meta.name,
                    table_qualified_name=col_meta.table_qualified_name,
                    ordinal_position=col_meta.ordinal_position,
                    ordinal_position_type=type(col_meta.ordinal_position).__name__,
                    data_type=col_meta.data_type,
                    properties=str(col_meta.properties),
                    error=str(e)
                )
                continue
        
        try:
            await self.db.flush()
        except Exception as e:
            logger.error(
                "column_flush_failed",
                error=str(e),
                error_type=type(e).__name__,
                count=count
            )
            raise
        return count
    
    async def _get_assets_by_type(self, asset_types) -> List[Asset]:
        """Get assets by type for this data source"""
        if not isinstance(asset_types, list):
            asset_types = [asset_types]
        
        query = select(Asset).where(
            Asset.data_source_id == self.data_source_id,
            Asset.tenant_id == self.tenant_id,
            Asset.type.in_(asset_types)
        )
        
        result = await self.db.execute(query)
        return result.scalars().all()