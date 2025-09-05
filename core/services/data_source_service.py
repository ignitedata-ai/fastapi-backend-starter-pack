"""Service layer for data source business logic."""

from typing import Dict, List, Optional, Set, Tuple
from uuid import UUID

from opentelemetry import trace
from sqlalchemy.ext.asyncio import AsyncSession

from core.api.v1.schemas.data_source import (
    DataSourceCreate,
    DataSourceStatus,
    DataSourceUpdate,
    RunDataSourceRequest,
)
from core.exceptions import BadRequestException, NotFoundException
from core.logging import get_logger
from core.models import ConnectorDefinition, ConnectorRun, DataSource
from core.models.enums import ConnectorKind, RunStatus, RunType
from core.repositories.connector_repository import ConnectorRepository
from core.repositories.data_source_repository import DataSourceRepository
from core.services.secrets_manager import secrets_manager

logger = get_logger(__name__)
tracer = trace.get_tracer(__name__)


class DataSourceService:
    """Service for data source business logic."""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.data_source_repository = DataSourceRepository(db)
        self.connector_repository = ConnectorRepository(db)

    def _get_credential_fields_from_connector(self, connector: ConnectorDefinition) -> Set[str]:
        """Extract credential field names from the connector's schema."""
        credential_fields = set()

        # First try secret_schema (legacy approach)
        if connector.secret_schema is not None:
            if isinstance(connector.secret_schema, dict):
                # Check for properties in JSON Schema
                if "properties" in connector.secret_schema:
                    credential_fields.update(connector.secret_schema["properties"].keys())
                # Also check top-level keys as field names
                else:
                    credential_fields.update(connector.secret_schema.keys())

        # Also check connection_schema for fields marked as credentials
        if connector.connection_schema is not None and isinstance(connector.connection_schema, dict):
            properties = connector.connection_schema.get("properties", {})
            for field_name, field_def in properties.items():
                if isinstance(field_def, dict) and field_def.get("credential", False):
                    credential_fields.add(field_name)

        logger.debug(
            "credential_fields_extracted",
            connector_key=connector.key,
            connector_version=connector.version,
            credential_fields=list(credential_fields),
        )

        return credential_fields

    def _separate_config_and_credentials(self, config_json: dict, connector: ConnectorDefinition) -> Tuple[Dict, Dict]:
        """Separate a combined config into public config and credentials based on connector schema."""
        # First decrypt all encrypted fields
        decrypted_config = secrets_manager.decrypt_config_credentials(config_json)

        # Get credential field names from connector definition
        credential_field_names = self._get_credential_fields_from_connector(connector)

        # Separate into public config and credentials
        public_config = {}
        credentials = {}

        for key, value in decrypted_config.items():
            if key in credential_field_names:
                credentials[key] = value
            else:
                public_config[key] = value

        return public_config, credentials

    async def create_data_source(self, data: DataSourceCreate, user_id: UUID, tenant_id: UUID) -> DataSource:
        """Create a new data source with business logic."""
        with tracer.start_as_current_span("data_source.service.create_data_source"):
            logger.info(
                "data_source_service_create_start",
                operation="create_data_source",
                data_source_name=data.name,
                connector_key=data.connector_key,
                tenant_id=str(tenant_id),
            )

            try:
                # Verify connector exists
                connector = await self.connector_repository.get_by_key_and_version(
                    key=data.connector_key, version=data.connector_version or "latest"
                )

                if not connector:
                    logger.error(
                        "data_source_service_connector_not_found",
                        operation="create_data_source",
                        connector_key=data.connector_key,
                        connector_version=data.connector_version,
                        success=False,
                    )
                    raise NotFoundException("Connector", data.connector_key)

                # Merge config and credentials into a single object
                config_json = data.config.copy() if data.config else {}

                # If credentials provided, encrypt them and add to config
                if data.credentials:
                    # Get list of credential field names from the credentials dict
                    credential_keys = list(data.credentials.keys())

                    # Merge credentials into config
                    config_json.update(data.credentials)

                    # Encrypt the credential fields within the config
                    config_json = secrets_manager.encrypt_config_credentials(config_json, credential_keys)

                    logger.info(
                        "credentials_encrypted_in_config",
                        data_source_slug=data.slug,
                        credential_fields=credential_keys,
                    )

                # Create data source with encrypted credentials in config
                data_source_data = {
                    "tenant_id": tenant_id,
                    "name": data.name,
                    "slug": data.slug,
                    "connector_key": data.connector_key,
                    "connector_version": data.connector_version or connector.version,
                    "kind": connector.kind,
                    "config_json": config_json,
                    "secret_uri": None,
                    "network_profile": data.network_profile,
                    "tags": data.tags,
                    "created_by": user_id,
                    "updated_by": user_id,
                }

                data_source = await self.data_source_repository.create(data_source_data)

                logger.info(
                    "data_source_service_create_success",
                    operation="create_data_source",
                    data_source_id=str(data_source.id),
                    data_source_name=data_source.name,
                    connector_key=data.connector_key,
                    tenant_id=str(tenant_id),
                    success=True,
                )

                return data_source

            except Exception as e:
                logger.error(
                    "data_source_service_create_error",
                    operation="create_data_source",
                    data_source_name=data.name,
                    connector_key=data.connector_key,
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def list_data_sources(
        self,
        tenant_id: UUID,
        page: int = 1,
        page_size: int = 20,
        search: Optional[str] = None,
        kind: Optional[ConnectorKind] = None,
        tags: Optional[List[str]] = None,
        sort_by: str = "created_at",
        sort_order: str = "desc",
    ) -> Tuple[List[DataSource], int]:
        """List data sources with pagination and filtering."""
        with tracer.start_as_current_span("data_source.service.list_data_sources"):
            logger.info(
                "data_source_service_list_start",
                operation="list_data_sources",
                tenant_id=str(tenant_id),
                page=page,
                page_size=page_size,
                search=search or "none",
                kind=kind.value if kind else "none",
            )

            try:
                skip = (page - 1) * page_size

                # Get data sources and total count in parallel
                data_sources = await self.data_source_repository.get_by_tenant_id(
                    tenant_id=tenant_id,
                    skip=skip,
                    limit=page_size,
                    search=search,
                    kind=kind,
                    tags=tags,
                    sort_by=sort_by,
                    sort_order=sort_order,
                )

                total_count = await self.data_source_repository.count_by_tenant_id(
                    tenant_id=tenant_id,
                    search=search,
                    kind=kind,
                    tags=tags,
                )

                logger.info(
                    "data_source_service_list_success",
                    operation="list_data_sources",
                    tenant_id=str(tenant_id),
                    total_count=total_count,
                    returned_count=len(data_sources),
                    page=page,
                    page_size=page_size,
                    success=True,
                )

                return list(data_sources), total_count

            except Exception as e:
                logger.error(
                    "data_source_service_list_error",
                    operation="list_data_sources",
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def get_data_source_with_credentials(
        self, data_source_id: UUID, tenant_id: UUID, include_credentials: bool = False
    ) -> Tuple[DataSource, Optional[Dict], Optional[Dict]]:
        """Get data source with separated config and credentials."""
        with tracer.start_as_current_span("data_source.service.get_data_source_with_credentials"):
            logger.info(
                "data_source_service_get_with_creds_start",
                operation="get_data_source_with_credentials",
                data_source_id=str(data_source_id),
                tenant_id=str(tenant_id),
                include_credentials=include_credentials,
            )

            try:
                # Get data source
                data_source = await self.data_source_repository.get_by_id_and_tenant(data_source_id, tenant_id)

                if not data_source:
                    logger.error(
                        "data_source_service_not_found",
                        operation="get_data_source_with_credentials",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=False,
                        success=False,
                    )
                    raise NotFoundException("Data Source", str(data_source_id))

                # Get connector definition for credential separation
                connector = await self.connector_repository.get_by_key_and_version(
                    key=data_source.connector_key, version=data_source.connector_version or "latest"
                )

                if not connector:
                    logger.error(
                        "data_source_service_connector_missing",
                        operation="get_data_source_with_credentials",
                        data_source_id=str(data_source_id),
                        connector_key=data_source.connector_key,
                        connector_version=data_source.connector_version,
                        success=False,
                    )
                    raise BadRequestException("Associated connector not found")

                # Separate config and credentials if config exists
                public_config, credentials = None, None
                if data_source.config_json:
                    public_config, credentials = self._separate_config_and_credentials(data_source.config_json, connector)

                logger.info(
                    "data_source_service_get_with_creds_success",
                    operation="get_data_source_with_credentials",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    config_fields=list(public_config.keys()) if public_config else [],
                    credential_fields=list(credentials.keys()) if credentials and include_credentials else ["<hidden>"],
                    include_credentials=include_credentials,
                    success=True,
                )

                return data_source, public_config, credentials if include_credentials else None

            except Exception as e:
                logger.error(
                    "data_source_service_get_with_creds_error",
                    operation="get_data_source_with_credentials",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def update_data_source(
        self, data_source_id: UUID, tenant_id: UUID, data: DataSourceUpdate, user_id: UUID
    ) -> DataSource:
        """Update data source with business logic."""
        with tracer.start_as_current_span("data_source.service.update_data_source"):
            logger.info(
                "data_source_service_update_start",
                operation="update_data_source",
                data_source_id=str(data_source_id),
                tenant_id=str(tenant_id),
            )

            try:
                # Get existing data source
                data_source = await self.data_source_repository.get_by_id_and_tenant(data_source_id, tenant_id)

                if not data_source:
                    logger.error(
                        "data_source_service_update_not_found",
                        operation="update_data_source",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=False,
                        success=False,
                    )
                    raise NotFoundException("Data Source", str(data_source_id))

                # Prepare update data
                update_data = data.dict(exclude_unset=True, exclude={"config", "credentials"})
                update_data["updated_by"] = user_id

                # Handle config and credentials specially
                existing_config = data_source.config_json or {}

                if data.config is not None:
                    # Merge new config with existing config
                    existing_config.update(data.config)

                if data.credentials is not None:
                    # Update encrypted credentials within config
                    credential_keys = list(data.credentials.keys())

                    # First decrypt existing config to avoid double encryption
                    decrypted_config = secrets_manager.decrypt_config_credentials(existing_config)

                    # Update with new credentials
                    decrypted_config.update(data.credentials)

                    # Re-encrypt the credential fields
                    existing_config = secrets_manager.encrypt_config_credentials(decrypted_config, credential_keys)

                    logger.info(
                        "credentials_updated_in_config",
                        data_source_id=str(data_source_id),
                        credential_fields=credential_keys,
                    )

                # Update the config_json if it was modified
                if data.config is not None or data.credentials is not None:
                    update_data["config_json"] = existing_config

                # Update the data source
                updated_data_source = await self.data_source_repository.update(data_source_id, update_data)

                if not updated_data_source:
                    raise BadRequestException("Failed to update data source")

                logger.info(
                    "data_source_service_update_success",
                    operation="update_data_source",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    updated_fields=list(update_data.keys()),
                    success=True,
                )

                return updated_data_source

            except Exception as e:
                logger.error(
                    "data_source_service_update_error",
                    operation="update_data_source",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def delete_data_source(self, data_source_id: UUID, tenant_id: UUID) -> bool:
        """Delete data source (soft delete)."""
        with tracer.start_as_current_span("data_source.service.delete_data_source"):
            logger.info(
                "data_source_service_delete_start",
                operation="delete_data_source",
                data_source_id=str(data_source_id),
                tenant_id=str(tenant_id),
            )

            try:
                # Get existing data source
                data_source = await self.data_source_repository.get_by_id_and_tenant(data_source_id, tenant_id)

                if not data_source:
                    logger.error(
                        "data_source_service_delete_not_found",
                        operation="delete_data_source",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=False,
                        success=False,
                    )
                    raise NotFoundException("Data Source", str(data_source_id))

                # Soft delete - update connection status
                update_data = {"connection_status": "deleted"}
                updated_data_source = await self.data_source_repository.update(data_source_id, update_data)

                success = updated_data_source is not None

                logger.info(
                    "data_source_service_delete_success",
                    operation="delete_data_source",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    success=success,
                )

                return success

            except Exception as e:
                logger.error(
                    "data_source_service_delete_error",
                    operation="delete_data_source",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def get_data_source_status(self, data_source_id: UUID, tenant_id: UUID) -> DataSourceStatus:
        """Get data source status including run statuses."""
        with tracer.start_as_current_span("data_source.service.get_data_source_status"):
            logger.info(
                "data_source_service_status_start",
                operation="get_data_source_status",
                data_source_id=str(data_source_id),
                tenant_id=str(tenant_id),
            )

            try:
                # Get data source
                data_source = await self.data_source_repository.get_by_id_and_tenant(data_source_id, tenant_id)

                if not data_source:
                    logger.error(
                        "data_source_service_status_not_found",
                        operation="get_data_source_status",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=False,
                        success=False,
                    )
                    raise NotFoundException("Data Source", str(data_source_id))

                # Get latest runs, asset counts in parallel
                metadata_run = await self.data_source_repository.get_latest_run(data_source_id, RunType.METADATA)
                profile_run = await self.data_source_repository.get_latest_run(data_source_id, RunType.PROFILE)
                assets_count = await self.data_source_repository.get_assets_count(data_source_id)
                fields_count = await self.data_source_repository.get_fields_count(data_source_id)

                status = DataSourceStatus(
                    id=data_source.id,
                    name=data_source.name,
                    connection_status=data_source.connection_status,
                    metadata_status=metadata_run.status if metadata_run else None,
                    profile_status=profile_run.status if profile_run else None,
                    last_metadata_run=metadata_run.started_at if metadata_run else None,
                    last_profile_run=profile_run.started_at if profile_run else None,
                    assets_count=assets_count,
                    fields_count=fields_count,
                )

                logger.info(
                    "data_source_service_status_success",
                    operation="get_data_source_status",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    assets_count=assets_count,
                    fields_count=fields_count,
                    metadata_status=metadata_run.status if metadata_run else "none",
                    profile_status=profile_run.status if profile_run else "none",
                    success=True,
                )

                return status

            except Exception as e:
                logger.error(
                    "data_source_service_status_error",
                    operation="get_data_source_status",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def create_connector_run(
        self,
        data_source_id: UUID,
        tenant_id: UUID,
        run_request: RunDataSourceRequest,
        user_id: UUID,
    ) -> ConnectorRun:
        """Create a new connector run for a data source operation."""
        with tracer.start_as_current_span("data_source.service.create_connector_run"):
            logger.info(
                "data_source_service_create_run_start",
                operation="create_connector_run",
                data_source_id=str(data_source_id),
                tenant_id=str(tenant_id),
                run_type=run_request.run_type.value,
            )

            try:
                # Verify data source exists
                data_source = await self.data_source_repository.get_by_id_and_tenant(data_source_id, tenant_id)

                if not data_source:
                    logger.error(
                        "data_source_service_create_run_not_found",
                        operation="create_connector_run",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=False,
                        success=False,
                    )
                    raise NotFoundException("Data Source", str(data_source_id))

                # Create run record
                run = ConnectorRun(
                    tenant_id=tenant_id,
                    data_source_id=data_source_id,
                    run_type=run_request.run_type,
                    trigger="manual",
                    params=run_request.params or {},
                    status=RunStatus.QUEUED,
                )

                self.db.add(run)
                await self.db.commit()
                await self.db.refresh(run)

                logger.info(
                    "data_source_service_create_run_success",
                    operation="create_connector_run",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    run_id=str(run.id),
                    run_type=run_request.run_type.value,
                    success=True,
                )

                return run

            except Exception as e:
                logger.error(
                    "data_source_service_create_run_error",
                    operation="create_connector_run",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    run_type=run_request.run_type.value,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def get_data_source_runs(
        self,
        data_source_id: UUID,
        tenant_id: UUID,
        run_type: Optional[RunType] = None,
        limit: int = 10,
    ) -> List[ConnectorRun]:
        """Get connector runs for a data source."""
        with tracer.start_as_current_span("data_source.service.get_data_source_runs"):
            logger.info(
                "data_source_service_get_runs_start",
                operation="get_data_source_runs",
                data_source_id=str(data_source_id),
                tenant_id=str(tenant_id),
                run_type=run_type.value if run_type else "all",
                limit=limit,
            )

            try:
                # Verify data source exists
                data_source = await self.data_source_repository.get_by_id_and_tenant(data_source_id, tenant_id)

                if not data_source:
                    logger.error(
                        "data_source_service_get_runs_not_found",
                        operation="get_data_source_runs",
                        data_source_id=str(data_source_id),
                        tenant_id=str(tenant_id),
                        found=False,
                        success=False,
                    )
                    raise NotFoundException("Data Source", str(data_source_id))

                # Get runs
                runs = await self.data_source_repository.get_runs(data_source_id, tenant_id, run_type, limit)

                logger.info(
                    "data_source_service_get_runs_success",
                    operation="get_data_source_runs",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    runs_count=len(runs),
                    run_type=run_type.value if run_type else "all",
                    success=True,
                )

                return runs

            except Exception as e:
                logger.error(
                    "data_source_service_get_runs_error",
                    operation="get_data_source_runs",
                    data_source_id=str(data_source_id),
                    tenant_id=str(tenant_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise
