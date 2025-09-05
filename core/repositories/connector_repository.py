"""Repository layer for connector operations"""

from typing import List, Optional
from uuid import UUID

from opentelemetry import trace
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.logging import get_logger
from core.models.connector import ConnectorDefinition
from core.models.enums import ConnectorKind
from core.repositories.base import BaseRepository

logger = get_logger(__name__)


class ConnectorRepository(BaseRepository[ConnectorDefinition, dict, dict]):
    """Repository for connector definitions"""

    def __init__(self, db: AsyncSession):
        super().__init__(ConnectorDefinition, db)
        self.tracer = trace.get_tracer(__name__)

    async def get_all(
        self, kind: Optional[str] = None, is_enabled: bool = True
    ) -> List[ConnectorDefinition]:
        """Get all connector definitions with optional filtering"""
        with self.tracer.start_as_current_span("connector.repository.get_all"):
            logger.info(
                "connector_repository_get_all_start",
                operation="get_all",
                filter_kind=kind or "none",
                filter_enabled=is_enabled,
            )
            
            try:
                stmt = select(ConnectorDefinition).where(ConnectorDefinition.is_enabled == is_enabled)
                
                if kind:
                    # Convert string to ConnectorKind enum
                    connector_kind = ConnectorKind(kind)
                    stmt = stmt.where(ConnectorDefinition.kind == connector_kind)
                    logger.debug(
                        "connector_repository_kind_filter_applied",
                        operation="get_all",
                        filter_kind=kind,
                        filter_kind_enum=connector_kind.value,
                    )
                
                result = await self.session.execute(stmt)
                connectors = list(result.scalars().all())
                
                # Group by kind for detailed metrics
                kinds_count = {}
                for c in connectors:
                    kinds_count[c.kind.value] = kinds_count.get(c.kind.value, 0) + 1
                
                logger.info(
                    "connector_repository_get_all_success",
                    operation="get_all",
                    filter_kind=kind,
                    filter_enabled=is_enabled,
                    total_count=len(connectors),
                    connector_keys=[c.key for c in connectors],
                    kinds_breakdown=kinds_count,
                    success=True,
                )
                
                return connectors
                
            except Exception as e:
                logger.error(
                    "connector_repository_get_all_error",
                    operation="get_all",
                    filter_kind=kind,
                    filter_enabled=is_enabled,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def get_by_id(self, connector_id: UUID) -> Optional[ConnectorDefinition]:
        """Get a connector definition by ID"""
        return await self.get(connector_id)

    async def get_by_key_and_version(
        self, key: str, version: str = "latest"
    ) -> Optional[ConnectorDefinition]:
        """Get a connector definition by key and version"""
        with self.tracer.start_as_current_span("connector.repository.get_by_key_and_version"):
            logger.info(
                "connector_repository_get_by_key_version_start",
                operation="get_by_key_and_version",
                connector_key=key,
                connector_version=version,
            )
            
            try:
                # If version is "latest", get the most recent version
                if version == "latest":
                    stmt = (
                        select(ConnectorDefinition)
                        .where(ConnectorDefinition.key == key)
                        .order_by(ConnectorDefinition.version.desc())
                        .limit(1)
                    )
                    logger.debug(
                        "connector_repository_using_latest_version",
                        operation="get_by_key_and_version",
                        connector_key=key,
                        version_strategy="latest",
                    )
                else:
                    stmt = select(ConnectorDefinition).where(
                        ConnectorDefinition.key == key,
                        ConnectorDefinition.version == version
                    )
                    logger.debug(
                        "connector_repository_using_specific_version",
                        operation="get_by_key_and_version",
                        connector_key=key,
                        version_strategy="specific",
                        target_version=version,
                    )
                
                result = await self.session.execute(stmt)
                connector = result.scalar_one_or_none()
                
                if connector:
                    logger.info(
                        "connector_repository_get_by_key_version_found",
                        operation="get_by_key_and_version",
                        connector_key=key,
                        requested_version=version,
                        found=True,
                        connector_kind=connector.kind.value,
                        actual_version=connector.version,
                        connector_enabled=connector.is_enabled,
                        success=True,
                    )
                else:
                    logger.warning(
                        "connector_repository_get_by_key_version_not_found",
                        operation="get_by_key_and_version",
                        connector_key=key,
                        requested_version=version,
                        found=False,
                        success=True,  # Operation succeeded, just no result
                    )
                
                return connector
                
            except Exception as e:
                logger.error(
                    "connector_repository_get_by_key_version_error",
                    operation="get_by_key_and_version",
                    connector_key=key,
                    requested_version=version,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise