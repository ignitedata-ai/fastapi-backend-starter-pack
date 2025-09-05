from typing import Dict, Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.database.session import get_session_manager
from core.logging import get_logger
from core.models import ConnectorDefinition, ConnectorRun, DataSource
from core.models.enums import RunStatus
from core.services.data_source_service import DataSourceService
from core.services.metadata_extractors.factory import MetadataExtractorFactory
from core.services.metadata_persistence import MetadataPersistenceService

logger = get_logger(__name__)
session_manager = get_session_manager()


class MetadataSync:
    """Service for syncing metadata from data sources"""

    def __init__(self, data_source_id: UUID, tenant_id: UUID, run_id: Optional[UUID] = None):
        self.data_source_id = data_source_id
        self.tenant_id = tenant_id
        self.run_id = run_id

    async def sync(self):
        """Main sync method using the new extensible architecture"""
        async with session_manager.get_session() as db:
            try:
                # Update run status
                if self.run_id:
                    await self._update_run_status(db, RunStatus.RUNNING)

                # Get data source and connector definition
                logger.info(
                    "metadata_sync_initializing",
                    data_source_id=str(self.data_source_id),
                    tenant_id=str(self.tenant_id),
                    run_id=str(self.run_id) if self.run_id else None,
                )

                data_source, connector = await self._get_data_source_with_connector(db)
                if not data_source:
                    raise ValueError(f"Data source {self.data_source_id} not found")
                if not connector:
                    raise ValueError(f"Connector definition not found for {data_source.connector_key}")

                logger.info(
                    "data_source_and_connector_retrieved",
                    data_source_id=str(self.data_source_id),
                    data_source_name=data_source.name,
                    connector_key=data_source.connector_key,
                    connector_version=data_source.connector_version,
                )

                # Separate config and credentials using service
                service = DataSourceService(db)
                if data_source.config_json:
                    public_config, credentials = service._separate_config_and_credentials(data_source.config_json, connector)
                else:
                    public_config, credentials = {}, {}

                # Create metadata extractor
                logger.info(
                    "creating_metadata_extractor",
                    connector_key=data_source.connector_key,
                    config_keys=list(public_config.keys()),
                    credentials_keys=list(credentials.keys()),
                )

                extractor = MetadataExtractorFactory.create_extractor(
                    connector_key=data_source.connector_key,
                    data_source_id=self.data_source_id,
                    tenant_id=self.tenant_id,
                    config=public_config,
                    credentials=credentials,
                )

                if not extractor:
                    supported_connectors = MetadataExtractorFactory.get_supported_connectors()
                    raise ValueError(
                        f"No metadata extractor available for connector: {data_source.connector_key}. "
                        f"Supported connectors: {supported_connectors}"
                    )

                # Test connection first
                logger.info(
                    "connection_test_starting",
                    data_source_id=str(self.data_source_id),
                    connector_key=data_source.connector_key,
                )

                connection_ok, error_msg = await extractor.test_connection()

                logger.info(
                    "connection_test_completed",
                    data_source_id=str(self.data_source_id),
                    connector_key=data_source.connector_key,
                    connection_status="success" if connection_ok else "failed",
                    error_message=error_msg if not connection_ok else None,
                )

                if not connection_ok:
                    raise ValueError(f"Connection test failed: {error_msg}")

                # Update run status to RUNNING
                if self.run_id:
                    await self._update_run_status(db, RunStatus.RUNNING)

                # Extract metadata
                logger.info(
                    "metadata_extraction_started",
                    data_source_id=str(self.data_source_id),
                    connector_key=data_source.connector_key,
                )

                extraction_result = await extractor.extract_metadata()

                # Persist metadata to database
                logger.info(
                    "metadata_persistence_starting",
                    data_source_id=str(self.data_source_id),
                    extraction_status=extraction_result.status.value,
                    extracted_databases=len(extraction_result.databases) if hasattr(extraction_result, 'databases') else 0,
                    extracted_tables=len([
                        t for db in getattr(extraction_result, 'databases', []) 
                        for t in getattr(db, 'tables', [])
                    ]),
                    extraction_errors=len(extraction_result.errors),
                    extraction_warnings=len(extraction_result.warnings),
                )

                persistence_service = MetadataPersistenceService(
                    db=db, data_source_id=self.data_source_id, tenant_id=self.tenant_id
                )

                persistence_counts = await persistence_service.persist_metadata(extraction_result)

                logger.info(
                    "metadata_persistence_completed",
                    data_source_id=str(self.data_source_id),
                    persistence_counts=persistence_counts,
                )

                # Update run status with results
                if self.run_id:
                    success_status = RunStatus.SUCCEEDED
                    if extraction_result.errors:
                        success_status = (
                            RunStatus.PARTIAL
                            if persistence_counts["databases"] > 0 or persistence_counts["tables"] > 0
                            else RunStatus.FAILED
                        )

                    await self._update_run_status(
                        db,
                        success_status,
                        error_message="; ".join(extraction_result.errors) if extraction_result.errors else None,
                        metadata_counts=persistence_counts,
                    )

                logger.info(
                    "metadata_sync_completed",
                    data_source_id=str(self.data_source_id),
                    connector_key=data_source.connector_key,
                    extraction_status=extraction_result.status.value,
                    persistence_counts=persistence_counts,
                    errors=len(extraction_result.errors),
                    warnings=len(extraction_result.warnings),
                )

            except Exception as e:
                connector_key = None
                try:
                    if 'data_source' in locals() and data_source:
                        connector_key = data_source.connector_key
                except:
                    pass

                logger.error(
                    "metadata_sync_failed",
                    data_source_id=str(self.data_source_id),
                    tenant_id=str(self.tenant_id),
                    run_id=str(self.run_id) if self.run_id else None,
                    connector_key=connector_key,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    exc_info=True,
                )
                if self.run_id:
                    await self._update_run_status(db, RunStatus.FAILED, str(e))
                raise

    async def _get_data_source_with_connector(self, db: AsyncSession):
        """Get data source with its connector definition."""
        query = (
            select(DataSource, ConnectorDefinition)
            .join(
                ConnectorDefinition,
                (ConnectorDefinition.key == DataSource.connector_key)
                & (ConnectorDefinition.version == DataSource.connector_version),
            )
            .where(DataSource.id == self.data_source_id)
        )

        result = await db.execute(query)
        row = result.first()
        return row if row else (None, None)

    async def _update_run_status(
        self,
        db: AsyncSession,
        status: RunStatus,
        error_message: Optional[str] = None,
        metadata_counts: Optional[Dict[str, int]] = None,
    ):
        """Update connector run status."""
        if not self.run_id:
            return

        from datetime import datetime, timezone

        from sqlalchemy import update

        update_values = {
            "status": status,
            "error_message": error_message,
            "finished_at": datetime.now(timezone.utc)
            if status in [RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.PARTIAL]
            else None,
        }

        # Add metadata counts to run metrics if provided
        if metadata_counts:
            update_values["metrics"] = {
                "metadata_counts": metadata_counts,
                "extraction_timestamp": datetime.now(timezone.utc).isoformat(),
            }

        stmt = update(ConnectorRun).where(ConnectorRun.id == self.run_id).values(**update_values)

        await db.execute(stmt)
        await db.commit()


# Helper function to run metadata sync (called from API)
async def run_metadata_sync(data_source_id: str, tenant_id: str, run_id: Optional[str] = None):
    """Run metadata sync for a data source.

    Args:
        data_source_id: UUID string of the data source
        tenant_id: UUID string of the tenant
        run_id: Optional UUID string of the connector run

    """
    try:
        sync_service = MetadataSync(
            data_source_id=UUID(data_source_id),
            tenant_id=UUID(tenant_id),
            run_id=UUID(run_id) if run_id else None,
        )

        await sync_service.sync()

    except Exception as e:
        logger.error(
            "run_metadata_sync_failed",
            data_source_id=data_source_id,
            tenant_id=tenant_id,
            run_id=run_id,
            error_type=type(e).__name__,
            error_message=str(e),
            exc_info=True,
        )
        raise
