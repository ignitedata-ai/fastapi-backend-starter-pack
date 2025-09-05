"""Repository layer for data source operations"""

from typing import List, Optional, Sequence
from uuid import UUID

from opentelemetry import trace
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import BinaryExpression

from core.logging import get_logger
from core.models import Asset, AssetField, ConnectorRun, DataSource
from core.models.enums import ConnectorKind, RunType
from core.repositories.base import BaseRepository

logger = get_logger(__name__)


class DataSourceRepository(BaseRepository[DataSource, dict, dict]):
    """Repository for data source operations"""

    def __init__(self, db: AsyncSession):
        super().__init__(DataSource, db)
        self.tracer = trace.get_tracer(__name__)

    async def get_by_tenant_id(
        self,
        tenant_id: UUID,
        skip: int = 0,
        limit: int = 100,
        search: Optional[str] = None,
        kind: Optional[ConnectorKind] = None,
        tags: Optional[List[str]] = None,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> Sequence[DataSource]:
        """Get data sources for a tenant with filtering and pagination"""
        with self.tracer.start_as_current_span("data_source.repository.get_by_tenant_id"):
            logger.info(
                "data_source_repository_get_by_tenant_start",
                operation="get_by_tenant_id",
                tenant_id=str(tenant_id),
                skip=skip,
                limit=limit,
                search=search or "none",
                kind=kind.value if kind else "none",
                tags=tags or [],
            )
            
            try:
                filters = [DataSource.tenant_id == tenant_id]
                
                # Apply search filter
                if search:
                    search_filter = or_(
                        DataSource.name.ilike(f"%{search}%"),
                        DataSource.slug.ilike(f"%{search}%"),
                    )
                    filters.append(search_filter)
                
                # Apply kind filter
                if kind:
                    filters.append(DataSource.kind == kind)
                
                # Apply tags filter
                if tags:
                    filters.append(DataSource.tags.overlap(tags))
                
                # Determine sort order
                sort_column = getattr(DataSource, sort_by, DataSource.created_at)
                order_by = sort_column.desc() if sort_order == "desc" else sort_column
                
                data_sources = await self.get_multi(
                    skip=skip,
                    limit=limit,
                    filters=filters,
                    order_by=order_by
                )
                
                logger.info(
                    "data_source_repository_get_by_tenant_success",
                    operation="get_by_tenant_id",
                    tenant_id=str(tenant_id),
                    total_count=len(data_sources),
                    search=search,
                    kind=kind.value if kind else None,
                    success=True,
                )
                
                return data_sources
                
            except Exception as e:
                logger.error(
                    "data_source_repository_get_by_tenant_error",
                    operation="get_by_tenant_id",
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def count_by_tenant_id(
        self,
        tenant_id: UUID,
        search: Optional[str] = None,
        kind: Optional[ConnectorKind] = None,
        tags: Optional[List[str]] = None,
    ) -> int:
        """Count data sources for a tenant with filtering"""
        with self.tracer.start_as_current_span("data_source.repository.count_by_tenant_id"):
            logger.info(
                "data_source_repository_count_by_tenant_start",
                operation="count_by_tenant_id",
                tenant_id=str(tenant_id),
                search=search or "none",
                kind=kind.value if kind else "none",
                tags=tags or [],
            )
            
            try:
                filters = [DataSource.tenant_id == tenant_id]
                
                # Apply search filter
                if search:
                    search_filter = or_(
                        DataSource.name.ilike(f"%{search}%"),
                        DataSource.slug.ilike(f"%{search}%"),
                    )
                    filters.append(search_filter)
                
                # Apply kind filter
                if kind:
                    filters.append(DataSource.kind == kind)
                
                # Apply tags filter
                if tags:
                    filters.append(DataSource.tags.overlap(tags))
                
                count = await self.count(filters=filters)
                
                logger.info(
                    "data_source_repository_count_by_tenant_success",
                    operation="count_by_tenant_id",
                    tenant_id=str(tenant_id),
                    count=count,
                    search=search,
                    kind=kind.value if kind else None,
                    success=True,
                )
                
                return count
                
            except Exception as e:
                logger.error(
                    "data_source_repository_count_by_tenant_error",
                    operation="count_by_tenant_id",
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def get_by_id_and_tenant(
        self, data_source_id: UUID, tenant_id: UUID
    ) -> Optional[DataSource]:
        """Get a data source by ID and tenant ID"""
        with self.tracer.start_as_current_span("data_source.repository.get_by_id_and_tenant"):
            logger.info(
                "data_source_repository_get_by_id_tenant_start",
                operation="get_by_id_and_tenant",
                data_source_id=str(data_source_id),
                tenant_id=str(tenant_id),
            )
            
            try:
                stmt = select(DataSource).where(
                    and_(
                        DataSource.id == data_source_id,
                        DataSource.tenant_id == tenant_id
                    )
                )
                
                result = await self.session.execute(stmt)
                data_source = result.scalar_one_or_none()
                
                if data_source:
                    logger.info(
                        "data_source_repository_get_by_id_tenant_found",
                        operation="get_by_id_and_tenant",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=True,
                        data_source_name=data_source.name,
                        success=True,
                    )
                else:
                    logger.warning(
                        "data_source_repository_get_by_id_tenant_not_found",
                        operation="get_by_id_and_tenant",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=False,
                        success=True,
                    )
                
                return data_source
                
            except Exception as e:
                logger.error(
                    "data_source_repository_get_by_id_tenant_error",
                    operation="get_by_id_and_tenant",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def get_assets_count(self, data_source_id: UUID) -> int:
        """Get the count of assets for a data source"""
        with self.tracer.start_as_current_span("data_source.repository.get_assets_count"):
            try:
                stmt = select(func.count(Asset.id)).where(
                    Asset.data_source_id == data_source_id
                )
                result = await self.session.execute(stmt)
                count = result.scalar() or 0
                
                logger.debug(
                    "data_source_repository_assets_count",
                    operation="get_assets_count",
                    data_source_id=str(data_source_id),
                    assets_count=count,
                )
                
                return count
                
            except Exception as e:
                logger.error(
                    "data_source_repository_assets_count_error",
                    operation="get_assets_count",
                    data_source_id=str(data_source_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    exc_info=True,
                )
                raise

    async def get_fields_count(self, data_source_id: UUID) -> int:
        """Get the count of asset fields for a data source"""
        with self.tracer.start_as_current_span("data_source.repository.get_fields_count"):
            try:
                stmt = (
                    select(func.count(AssetField.id))
                    .select_from(AssetField)
                    .join(Asset)
                    .where(Asset.data_source_id == data_source_id)
                )
                result = await self.session.execute(stmt)
                count = result.scalar() or 0
                
                logger.debug(
                    "data_source_repository_fields_count",
                    operation="get_fields_count",
                    data_source_id=str(data_source_id),
                    fields_count=count,
                )
                
                return count
                
            except Exception as e:
                logger.error(
                    "data_source_repository_fields_count_error",
                    operation="get_fields_count",
                    data_source_id=str(data_source_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    exc_info=True,
                )
                raise

    async def get_latest_run(
        self, data_source_id: UUID, run_type: RunType
    ) -> Optional[ConnectorRun]:
        """Get the latest connector run for a data source by type"""
        with self.tracer.start_as_current_span("data_source.repository.get_latest_run"):
            try:
                stmt = (
                    select(ConnectorRun)
                    .where(
                        and_(
                            ConnectorRun.data_source_id == data_source_id,
                            ConnectorRun.run_type == run_type,
                        )
                    )
                    .order_by(ConnectorRun.started_at.desc())
                    .limit(1)
                )
                
                result = await self.session.execute(stmt)
                run = result.scalar_one_or_none()
                
                logger.debug(
                    "data_source_repository_latest_run",
                    operation="get_latest_run",
                    data_source_id=str(data_source_id),
                    run_type=run_type.value,
                    found=run is not None,
                    run_id=str(run.id) if run else None,
                )
                
                return run
                
            except Exception as e:
                logger.error(
                    "data_source_repository_latest_run_error",
                    operation="get_latest_run",
                    data_source_id=str(data_source_id),
                    run_type=run_type.value,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    exc_info=True,
                )
                raise

    async def get_runs(
        self,
        data_source_id: UUID,
        tenant_id: UUID,
        run_type: Optional[RunType] = None,
        limit: int = 10,
    ) -> List[ConnectorRun]:
        """Get connector runs for a data source"""
        with self.tracer.start_as_current_span("data_source.repository.get_runs"):
            try:
                stmt = select(ConnectorRun).where(
                    and_(
                        ConnectorRun.data_source_id == data_source_id,
                        ConnectorRun.tenant_id == tenant_id
                    )
                )
                
                if run_type:
                    stmt = stmt.where(ConnectorRun.run_type == run_type)
                    
                stmt = stmt.order_by(ConnectorRun.started_at.desc()).limit(limit)
                
                result = await self.session.execute(stmt)
                runs = list(result.scalars().all())
                
                logger.debug(
                    "data_source_repository_get_runs",
                    operation="get_runs",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    run_type=run_type.value if run_type else "all",
                    runs_count=len(runs),
                )
                
                return runs
                
            except Exception as e:
                logger.error(
                    "data_source_repository_get_runs_error",
                    operation="get_runs",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    exc_info=True,
                )
                raise