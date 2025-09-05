"""Service layer for connector business logic"""

from typing import List, Optional
from uuid import UUID

from opentelemetry import trace
from sqlalchemy.ext.asyncio import AsyncSession

from core.api.v1.schemas.connector import TestConnectionRequest, TestConnectionResponse
from core.exceptions import BadRequestException, NotFoundException
from core.logging import get_logger
from core.models import ConnectorDefinition
from core.models.enums import ConnectorKind, DatabaseErrorPatterns
from core.repositories.connector_repository import ConnectorRepository
from core.services.connector_test import test_connector_connection

logger = get_logger(__name__)
tracer = trace.get_tracer(__name__)


class ConnectorService:
    """Service for connector business logic"""

    def __init__(self, db: AsyncSession):
        self.db = db
        self.repository = ConnectorRepository(db)

    async def list_supported_connectors(
        self, kind: Optional[str] = None, is_enabled: bool = True
    ) -> List[ConnectorDefinition]:
        """List all supported connectors with business logic"""
        with tracer.start_as_current_span("connector.service.list_supported_connectors"):
            logger.info(
                "connector_service_list_start",
                operation="list_supported_connectors",
                filter_kind=kind or "all",
                filter_enabled=is_enabled,
            )
            
            try:
                # Validate connector kind if provided
                if kind and not ConnectorKind.is_valid(kind):
                    logger.error(
                        "connector_service_validation_error",
                        operation="list_supported_connectors",
                        error_type="invalid_kind",
                        invalid_kind=kind,
                        valid_kinds=[k.value for k in ConnectorKind],
                    )
                    raise BadRequestException(
                        f"Invalid connector kind: '{kind}'",
                        details=DatabaseErrorPatterns.create_connector_kind_error_details(kind),
                    )

                connectors = await self.repository.get_all(kind=kind, is_enabled=is_enabled)
                
                # Group by kind for metrics
                kinds_count = {}
                for c in connectors:
                    kinds_count[c.kind.value] = kinds_count.get(c.kind.value, 0) + 1

                logger.info(
                    "connector_service_list_success",
                    operation="list_supported_connectors",
                    total_count=len(connectors),
                    filter_kind=kind,
                    filter_enabled=is_enabled,
                    connector_keys=[c.key for c in connectors],
                    kinds_breakdown=kinds_count,
                    success=True,
                )
                
                return connectors
                
            except Exception as e:
                logger.error(
                    "connector_service_list_error",
                    operation="list_supported_connectors",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    filter_kind=kind,
                    filter_enabled=is_enabled,
                    success=False,
                    exc_info=True,
                )
                raise

    async def get_connector_definition(self, connector_id: UUID) -> ConnectorDefinition:
        """Get a specific connector definition"""
        with tracer.start_as_current_span("connector.service.get_connector_definition"):
            logger.info(
                "connector_service_get_start",
                operation="get_connector_definition",
                connector_id=str(connector_id),
            )
            
            try:
                connector = await self.repository.get_by_id(connector_id)

                if not connector:
                    logger.error(
                        "connector_service_not_found",
                        operation="get_connector_definition",
                        connector_id=str(connector_id),
                        found=False,
                        success=False,
                    )
                    raise NotFoundException("Connector", str(connector_id))

                logger.info(
                    "connector_service_get_success",
                    operation="get_connector_definition",
                    connector_id=str(connector_id),
                    connector_key=connector.key,
                    connector_kind=connector.kind.value,
                    connector_version=connector.version,
                    connector_enabled=connector.is_enabled,
                    found=True,
                    success=True,
                )
                
                return connector
                
            except Exception as e:
                logger.error(
                    "connector_service_get_error",
                    operation="get_connector_definition",
                    connector_id=str(connector_id),
                    error_type=type(e).__name__,
                    error_message=str(e),
                    success=False,
                    exc_info=True,
                )
                raise

    async def test_connection(
        self, request: TestConnectionRequest
    ) -> TestConnectionResponse:
        """Test a connector connection"""
        with tracer.start_as_current_span("connector.service.test_connection"):
            # Log config metadata without sensitive data
            config_keys = list(request.config.keys()) if request.config else []
            
            logger.info(
                "connector_service_test_start",
                operation="test_connection",
                connector_key=request.connector_key,
                connector_version=request.connector_version or "latest",
                config_keys=config_keys,
                config_count=len(config_keys),
            )
            
            try:
                # Get connector definition
                connector = await self.repository.get_by_key_and_version(
                    key=request.connector_key,
                    version=request.connector_version or "latest"
                )

                if not connector:
                    logger.error(
                        "connector_service_test_not_found",
                        operation="test_connection",
                        connector_key=request.connector_key,
                        connector_version=request.connector_version,
                        found=False,
                        success=False,
                    )
                    raise NotFoundException(
                        "Connector", f"{request.connector_key}"
                    )

                logger.info(
                    "connector_service_test_connector_found",
                    operation="test_connection",
                    connector_key=request.connector_key,
                    connector_kind=connector.kind.value,
                    connector_actual_version=connector.version,
                    connector_enabled=connector.is_enabled,
                    found=True,
                )

                # Test the connection using the existing service
                result = await test_connector_connection(
                    connector=connector,
                    config=request.config,
                )
                # Unpack the result tuple
                success, message, details = result

                logger.info(
                    "connector_service_test_complete",
                    operation="test_connection",
                    connector_key=request.connector_key,
                    connector_kind=connector.kind.value,
                    test_success=success,
                    test_message=message,
                    test_details_keys=list(details.keys()) if details else [],
                    operation_success=True,
                )
                
                # Construct and return a TestConnectionResponse object
                return TestConnectionResponse(success=success, message=message, details=details)
                
            except Exception as e:
                logger.error(
                    "connector_service_test_error",
                    operation="test_connection",
                    connector_key=request.connector_key,
                    connector_version=request.connector_version,
                    error_type=type(e).__name__,
                    error_message=str(e),
                    operation_success=False,
                    exc_info=True,
                )
                raise
