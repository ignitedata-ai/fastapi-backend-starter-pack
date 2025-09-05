"""API routes for data source endpoints with exception handling."""

from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, BackgroundTasks, Depends, Query
from opentelemetry import trace
from sqlalchemy.ext.asyncio import AsyncSession

from core.api.v1.schemas.base import PaginatedResponse, PaginationParams
from core.api.v1.schemas.data_source import (
    DataSourceCreate,
    DataSourceResponse,
    DataSourceStatus,
    DataSourceUpdate,
    RunDataSourceRequest,
)
from core.database.session import get_db_session
from core.exceptions import (
    BadRequestException,
    InternalServerException,
    NotFoundException,
)
from core.logging import get_logger
from core.models import ConnectorRun
from core.models.enums import ConnectorKind, RunStatus, RunType
from core.services.auth import get_current_user
from core.services.data_source_service import DataSourceService

router = APIRouter(prefix="/v1/data_sources", tags=["data_sources"])
logger = get_logger(__name__)
tracer = trace.get_tracer(__name__)


@router.post("/", response_model=DataSourceResponse)
async def create_data_source(
    data: DataSourceCreate,
    background_tasks: BackgroundTasks,
    user: list[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Create a new data source."""
    with tracer.start_as_current_span("data_source.api.create_data_source"):
        logger.info(
            "data_source_api_create_start",
            endpoint="/api/v1/data_sources",
            method="POST",
            data_source_name=data.name,
            connector_key=data.connector_key,
            tenant_id=str(user[1]),
        )

        try:
            service = DataSourceService(db)
            data_source = await service.create_data_source(data, user[0], user[1])

            # Create a connector run to track the metadata extraction
            connector_run = ConnectorRun(
                tenant_id=user[1],
                data_source_id=data_source.id,
                run_type=RunType.METADATA,
                trigger="data_source_creation",
                params={
                    "connector_key": data.connector_key,
                    "connector_version": data.connector_version,
                    "trigger_event": "create_data_source",
                },
                status=RunStatus.QUEUED,
            )

            db.add(connector_run)
            await db.commit()
            await db.refresh(connector_run)

            # Capture current trace context before starting background task
            from opentelemetry import context as otel_context
            current_context = otel_context.get_current()
            
            # Start metadata sync in background with run tracking
            background_tasks.add_task(
                start_metadata_sync, 
                str(data_source.id), 
                str(user[1]), 
                str(connector_run.id),
                current_context
            )

            logger.info(
                "data_source_api_create_success",
                endpoint="/api/v1/data_sources",
                method="POST",
                data_source_id=str(data_source.id),
                data_source_name=data_source.name,
                tenant_id=str(user[1]),
                connector_key=data.connector_key,
                connector_run_id=str(connector_run.id),
                http_status=201,
                success=True,
            )

            return DataSourceResponse.from_orm(data_source)

        except NotFoundException as e:
            logger.error(
                "data_source_api_create_not_found",
                endpoint="/api/v1/data_sources",
                method="POST",
                data_source_name=data.name,
                connector_key=data.connector_key,
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            raise e

        except BadRequestException as e:
            logger.error(
                "data_source_api_create_bad_request",
                endpoint="/api/v1/data_sources",
                method="POST",
                data_source_name=data.name,
                connector_key=data.connector_key,
                error_type="bad_request",
                error_message=str(e),
                http_status=400,
                success=False,
            )
            raise e

        except Exception as e:
            logger.error(
                "data_source_api_create_internal_error",
                endpoint="/api/v1/data_sources",
                method="POST",
                data_source_name=data.name,
                connector_key=data.connector_key,
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            await db.rollback()
            raise InternalServerException(f"Failed to create data source: {str(e)}") from e


@router.get("/", response_model=PaginatedResponse)
async def list_data_sources(
    pagination: PaginationParams = Depends(),
    search: Optional[str] = Query(None),
    kind: Optional[ConnectorKind] = Query(None),
    tags: Optional[List[str]] = Query(None),
    user: List[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """List data sources with pagination and filtering."""
    with tracer.start_as_current_span("data_source.api.list_data_sources"):
        logger.info(
            "data_source_api_list_start",
            endpoint="/api/v1/data_sources",
            method="GET",
            tenant_id=str(user[1]),
            page=pagination.page,
            page_size=pagination.page_size,
            search=search or "none",
            kind=kind.value if kind else "none",
        )

        try:
            service = DataSourceService(db)
            data_sources, total_count = await service.list_data_sources(
                tenant_id=user[1],
                page=pagination.page,
                page_size=pagination.page_size,
                search=search,
                kind=kind,
                tags=tags,
                sort_by=pagination.sort_by or "created_at",
                sort_order=pagination.sort_order or "desc",
            )

            total_pages = (total_count + pagination.page_size - 1) // pagination.page_size

            response = PaginatedResponse(
                items=[DataSourceResponse.from_orm(ds) for ds in data_sources],
                total=total_count,
                page=pagination.page,
                page_size=pagination.page_size,
                total_pages=total_pages,
            )

            logger.info(
                "data_source_api_list_success",
                endpoint="/api/v1/data_sources",
                method="GET",
                tenant_id=str(user[1]),
                total_count=total_count,
                returned_count=len(data_sources),
                page=pagination.page,
                page_size=pagination.page_size,
                http_status=200,
                success=True,
            )

            return response

        except Exception as e:
            logger.error(
                "data_source_api_list_internal_error",
                endpoint="/api/v1/data_sources",
                method="GET",
                tenant_id=str(user[1]),
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            raise InternalServerException(f"Failed to list data sources: {str(e)}") from e


@router.get("/{data_source_id}", response_model=DataSourceResponse)
async def get_data_source(
    data_source_id: UUID,
    include_credentials: bool = Query(False, description="Include decrypted credentials in the response"),
    user: List[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Get data source by ID with separated config and credentials."""
    with tracer.start_as_current_span("data_source.api.get_data_source"):
        logger.info(
            "data_source_api_get_start",
            endpoint="/api/v1/data_sources/{data_source_id}",
            method="GET",
            data_source_id=str(data_source_id),
            tenant_id=str(user[1]),
            include_credentials=include_credentials,
        )

        try:
            service = DataSourceService(db)
            data_source, public_config, credentials = await service.get_data_source_with_credentials(
                data_source_id, user[1], include_credentials
            )

            # Create response with separated config and credentials
            response_data = {
                "id": data_source.id,
                "tenant_id": data_source.tenant_id,
                "name": data_source.name,
                "slug": data_source.slug,
                "connector_key": data_source.connector_key,
                "connector_version": data_source.connector_version,
                "kind": data_source.kind,
                "config": public_config or {},
                "credentials": credentials if include_credentials else None,
                "has_credentials": bool(credentials),
                "network_profile": data_source.network_profile,
                "tags": data_source.tags,
                "connection_status": data_source.connection_status,
                "created_at": data_source.created_at,
                "updated_at": data_source.updated_at,
                "last_health_at": data_source.last_health_at,
            }

            logger.info(
                "data_source_api_get_success",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                data_source_name=data_source.name,
                config_fields=list(public_config.keys()) if public_config else [],
                credential_fields=list(credentials.keys()) if credentials and include_credentials else ["<hidden>"],
                include_credentials=include_credentials,
                http_status=200,
                success=True,
            )

            return DataSourceResponse(**response_data)

        except NotFoundException as e:
            logger.error(
                "data_source_api_get_not_found",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            raise e

        except Exception as e:
            logger.error(
                "data_source_api_get_internal_error",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            raise InternalServerException(f"Failed to get data source: {str(e)}") from e


@router.patch("/{data_source_id}", response_model=DataSourceResponse)
async def update_data_source(
    data_source_id: UUID,
    data: DataSourceUpdate,
    user: List[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Update data source."""
    with tracer.start_as_current_span("data_source.api.update_data_source"):
        logger.info(
            "data_source_api_update_start",
            endpoint="/api/v1/data_sources/{data_source_id}",
            method="PATCH",
            data_source_id=str(data_source_id),
            tenant_id=str(user[1]),
        )

        try:
            service = DataSourceService(db)
            updated_data_source = await service.update_data_source(data_source_id, user[1], data, user[0])

            logger.info(
                "data_source_api_update_success",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="PATCH",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                data_source_name=updated_data_source.name,
                http_status=200,
                success=True,
            )

            return DataSourceResponse.from_orm(updated_data_source)

        except NotFoundException as e:
            logger.error(
                "data_source_api_update_not_found",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="PATCH",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            raise e

        except BadRequestException as e:
            logger.error(
                "data_source_api_update_bad_request",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="PATCH",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type="bad_request",
                error_message=str(e),
                http_status=400,
                success=False,
            )
            raise e

        except Exception as e:
            logger.error(
                "data_source_api_update_internal_error",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="PATCH",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            await db.rollback()
            raise InternalServerException(f"Failed to update data source: {str(e)}") from e


@router.delete("/{data_source_id}")
async def delete_data_source(
    data_source_id: UUID,
    user: List[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Delete data source (soft delete)."""
    with tracer.start_as_current_span("data_source.api.delete_data_source"):
        logger.info(
            "data_source_api_delete_start",
            endpoint="/api/v1/data_sources/{data_source_id}",
            method="DELETE",
            data_source_id=str(data_source_id),
            tenant_id=str(user[1]),
        )

        try:
            service = DataSourceService(db)
            success = await service.delete_data_source(data_source_id, user[1])

            logger.info(
                "data_source_api_delete_success",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="DELETE",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                http_status=200,
                success=success,
            )

            return {"message": "Data source deleted successfully"}

        except NotFoundException as e:
            logger.error(
                "data_source_api_delete_not_found",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="DELETE",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            raise e

        except Exception as e:
            logger.error(
                "data_source_api_delete_internal_error",
                endpoint="/api/v1/data_sources/{data_source_id}",
                method="DELETE",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            await db.rollback()
            raise InternalServerException(f"Failed to delete data source: {str(e)}") from e


@router.get("/{data_source_id}/status", response_model=DataSourceStatus)
async def get_data_source_status(
    data_source_id: UUID,
    user: List[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Get data source status including run statuses."""
    with tracer.start_as_current_span("data_source.api.get_data_source_status"):
        logger.info(
            "data_source_api_status_start",
            endpoint="/api/v1/data_sources/{data_source_id}/status",
            method="GET",
            data_source_id=str(data_source_id),
            tenant_id=str(user[1]),
        )

        try:
            service = DataSourceService(db)
            status = await service.get_data_source_status(data_source_id, user[1])

            logger.info(
                "data_source_api_status_success",
                endpoint="/api/v1/data_sources/{data_source_id}/status",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                data_source_name=status.name,
                assets_count=status.assets_count,
                fields_count=status.fields_count,
                connection_status=status.connection_status,
                http_status=200,
                success=True,
            )

            return status

        except NotFoundException as e:
            logger.error(
                "data_source_api_status_not_found",
                endpoint="/api/v1/data_sources/{data_source_id}/status",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            raise e

        except Exception as e:
            logger.error(
                "data_source_api_status_internal_error",
                endpoint="/api/v1/data_sources/{data_source_id}/status",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            raise InternalServerException(f"Failed to get data source status: {str(e)}") from e


@router.post("/{data_source_id}/run")
async def run_data_source_operation(
    data_source_id: UUID,
    data: RunDataSourceRequest,
    background_tasks: BackgroundTasks,
    user: List[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
):
    """Run metadata sync, profiling, or other operations on data source."""
    with tracer.start_as_current_span("data_source.api.run_data_source_operation"):
        logger.info(
            "data_source_api_run_start",
            endpoint="/api/v1/data_sources/{data_source_id}/run",
            method="POST",
            data_source_id=str(data_source_id),
            tenant_id=str(user[1]),
            run_type=data.run_type.value,
        )

        try:
            service = DataSourceService(db)
            run = await service.create_connector_run(data_source_id, user[1], data, user[0])

            # Capture current trace context before starting background task
            from opentelemetry import context as otel_context
            current_context = otel_context.get_current()
            
            # Start operation in background
            if data.run_type == RunType.METADATA:
                background_tasks.add_task(
                    start_metadata_sync, 
                    str(data_source_id), 
                    str(user[1]), 
                    str(run.id),
                    current_context
                )
            elif data.run_type == RunType.PROFILE:
                background_tasks.add_task(
                    start_profiling, 
                    str(data_source_id), 
                    str(user[1]), 
                    str(run.id),
                    current_context
                )

            logger.info(
                "data_source_api_run_success",
                endpoint="/api/v1/data_sources/{data_source_id}/run",
                method="POST",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                run_type=data.run_type.value,
                run_id=str(run.id),
                http_status=200,
                success=True,
            )

            return {"message": f"{data.run_type} operation started", "run_id": str(run.id)}

        except NotFoundException as e:
            logger.error(
                "data_source_api_run_not_found",
                endpoint="/api/v1/data_sources/{data_source_id}/run",
                method="POST",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                run_type=data.run_type.value,
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            raise e

        except Exception as e:
            logger.error(
                "data_source_api_run_internal_error",
                endpoint="/api/v1/data_sources/{data_source_id}/run",
                method="POST",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                run_type=data.run_type.value,
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            await db.rollback()
            raise InternalServerException(f"Failed to start operation: {str(e)}") from e


@router.get("/{data_source_id}/runs")
async def get_data_source_runs(
    data_source_id: UUID,
    user: List[UUID] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db_session),
    run_type: Optional[RunType] = Query(None, description="Filter by run type"),
    limit: int = Query(10, ge=1, le=100, description="Number of runs to return"),
):
    """Get connector runs for a data source."""
    with tracer.start_as_current_span("data_source.api.get_data_source_runs"):
        logger.info(
            "data_source_api_runs_start",
            endpoint="/api/v1/data_sources/{data_source_id}/runs",
            method="GET",
            data_source_id=str(data_source_id),
            tenant_id=str(user[1]),
            run_type=run_type.value if run_type else "all",
            limit=limit,
        )

        try:
            service = DataSourceService(db)
            runs = await service.get_data_source_runs(data_source_id, user[1], run_type, limit)

            response = {
                "data_source_id": str(data_source_id),
                "runs": [
                    {
                        "id": str(run.id),
                        "run_type": run.run_type,
                        "status": run.status,
                        "trigger": run.trigger,
                        "started_at": run.started_at.isoformat() if run.started_at else None,
                        "finished_at": run.finished_at.isoformat() if run.finished_at else None,
                        "error_message": run.error_message,
                        "metrics": run.metrics,
                        "params": run.params,
                    }
                    for run in runs
                ],
            }

            logger.info(
                "data_source_api_runs_success",
                endpoint="/api/v1/data_sources/{data_source_id}/runs",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                runs_count=len(runs),
                run_type=run_type.value if run_type else "all",
                http_status=200,
                success=True,
            )

            return response

        except NotFoundException as e:
            logger.error(
                "data_source_api_runs_not_found",
                endpoint="/api/v1/data_sources/{data_source_id}/runs",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            raise e

        except Exception as e:
            logger.error(
                "data_source_api_runs_internal_error",
                endpoint="/api/v1/data_sources/{data_source_id}/runs",
                method="GET",
                data_source_id=str(data_source_id),
                tenant_id=str(user[1]),
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            raise InternalServerException(f"Failed to get data source runs: {str(e)}") from e


# Background task functions
async def start_metadata_sync(data_source_id: str, tenant_id: str, run_id: str | None = None, parent_context=None):
    """Start metadata sync for data source using the extensible metadata extraction system."""
    from opentelemetry import context as otel_context
    
    # Set the parent context if provided
    if parent_context:
        token = otel_context.attach(parent_context)
    else:
        token = None
    
    try:
        with tracer.start_as_current_span("background_task.metadata_sync") as span:
            span.set_attribute("data_source_id", data_source_id)
            span.set_attribute("tenant_id", tenant_id)
            span.set_attribute("run_id", run_id or "none")

            logger.info(
                "metadata_sync_background_task_started",
                data_source_id=data_source_id,
                tenant_id=tenant_id,
                run_id=run_id,
                operation="background_metadata_sync",
            )

            try:
                # Import here to avoid circular imports
                from core.services.metadata_sync import run_metadata_sync

                # Run the metadata sync
                await run_metadata_sync(data_source_id, tenant_id, run_id)

                logger.info(
                    "metadata_sync_background_task_completed",
                    data_source_id=data_source_id,
                    tenant_id=tenant_id,
                    run_id=run_id,
                    operation="background_metadata_sync",
                    success=True,
                )

                span.set_attribute("success", True)

            except Exception as e:
                logger.error(
                    "metadata_sync_background_task_failed",
                    data_source_id=data_source_id,
                    tenant_id=tenant_id,
                    run_id=run_id,
                    operation="background_metadata_sync",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )

                span.set_attribute("success", False)
                span.record_exception(e)
                # Don't re-raise as this is a background task
    finally:
        # Detach the context when done
        if token is not None:
            otel_context.detach(token)


async def start_profiling(data_source_id: str, tenant_id: str, run_id: str | None = None, parent_context=None):
    """Start profiling for data source."""
    from opentelemetry import context as otel_context
    
    # Set the parent context if provided
    if parent_context:
        token = otel_context.attach(parent_context)
    else:
        token = None
    
    try:
        with tracer.start_as_current_span("background_task.profiling") as span:
            span.set_attribute("data_source_id", data_source_id)
            span.set_attribute("tenant_id", tenant_id)
            span.set_attribute("run_id", run_id or "none")

            logger.info(
                "profiling_background_task_started",
                data_source_id=data_source_id,
                tenant_id=tenant_id,
                run_id=run_id,
                operation="background_profiling",
            )

            try:
                # TODO: Implement actual profiling logic
                logger.info(
                    "profiling_background_task_completed",
                    data_source_id=data_source_id,
                    tenant_id=tenant_id,
                    run_id=run_id,
                    operation="background_profiling",
                    success=True,
                    message="Profiling not yet implemented",
                )

                span.set_attribute("success", True)
                span.set_attribute("message", "Not yet implemented")

            except Exception as e:
                logger.error(
                    "profiling_background_task_failed",
                    data_source_id=data_source_id,
                    tenant_id=tenant_id,
                    run_id=run_id,
                    operation="background_profiling",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )

                span.set_attribute("success", False)
                span.record_exception(e)
    finally:
        # Detach the context when done
        if token is not None:
            otel_context.detach(token)
