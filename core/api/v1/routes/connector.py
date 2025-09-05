"""API routes for connector endpoints with exception handling"""

from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from opentelemetry import trace
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession

from core.api.v1.schemas.connector import (
    ConnectorDefinitionList,
    ConnectorDefinitionResponse,
    TestConnectionRequest,
    TestConnectionResponse,
)
from core.exceptions import (
    BadRequestException,
    InternalServerException,
    NotFoundException,
)
from core.logging import get_logger
from core.database.session import get_db_session
from core.services.connector_service import ConnectorService

router = APIRouter(prefix="/v1/connectors", tags=["connectors"])
logger = get_logger(__name__)
tracer = trace.get_tracer(__name__)


@router.get("/supported", response_model=ConnectorDefinitionList)
async def list_supported_connectors(
    kind: Optional[str] = Query(None, description="Filter by connector kind"),
    is_enabled: bool = Query(True, description="Filter by enabled status"),
    db: AsyncSession = Depends(get_db_session),
):
    """
    List all supported connectors

    - **kind**: Filter by connector kind (jdbc, warehouse, etc.)
    - **is_enabled**: Filter by enabled status

    Returns list of available connectors with their configurations.
    """
    with tracer.start_as_current_span("connector.api.list_supported_connectors"):
        logger.info(
            "connector_api_list_start",
            endpoint="/api/v1/connectors/supported",
            method="GET",
            filter_kind=kind or "none",
            filter_enabled=is_enabled,
        )
        
        try:
            service = ConnectorService(db)
            connectors = await service.list_supported_connectors(
                kind=kind, is_enabled=is_enabled
            )
            
            response = ConnectorDefinitionList(
                connectors=[
                    ConnectorDefinitionResponse.model_validate(c) for c in connectors
                ],
                total=len(connectors),
            )
            
            logger.info(
                "connector_api_list_success",
                endpoint="/api/v1/connectors/supported",
                method="GET",
                filter_kind=kind,
                filter_enabled=is_enabled,
                response_total=len(connectors),
                response_keys=[c.key for c in connectors],
                http_status=200,
                success=True,
            )
            
            return response

        except ValidationError as e:
            error_msg = str(e)
            logger.error(
                "connector_api_list_validation_error",
                endpoint="/api/v1/connectors/supported",
                method="GET",
                filter_kind=kind,
                filter_enabled=is_enabled,
                error_type="validation_error",
                error_message=error_msg,
                http_status=400,
                success=False,
                exc_info=True,
            )
            raise BadRequestException(f"Invalid request parameters: {str(e)}")

        except Exception as e:
            error_msg = str(e)
            logger.error(
                "connector_api_list_internal_error",
                endpoint="/api/v1/connectors/supported",
                method="GET",
                filter_kind=kind,
                filter_enabled=is_enabled,
                error_type=type(e).__name__,
                error_message=error_msg,
                http_status=500,
                success=False,
                exc_info=True,
            )
            raise InternalServerException(f"Failed to retrieve connectors: {error_msg}")


@router.get(
    "/{connector_id}",
    response_model=ConnectorDefinitionResponse,
    responses={
        404: {"description": "Connector not found"},
        500: {"description": "Internal server error"},
    },
)
async def get_connector_definition(
    connector_id: UUID, db: AsyncSession = Depends(get_db_session)
):
    """
    Get a specific connector definition by ID

    - **connector_id**: UUID of the connector
    """
    with tracer.start_as_current_span("connector.api.get_connector_definition"):
        logger.info(
            "connector_api_get_start",
            endpoint="/api/v1/connectors/{connector_id}",
            method="GET",
            connector_id=str(connector_id),
        )
        
        try:
            service = ConnectorService(db)
            connector = await service.get_connector_definition(connector_id)
            
            response = ConnectorDefinitionResponse.model_validate(connector)
            
            logger.info(
                "connector_api_get_success",
                endpoint="/api/v1/connectors/{connector_id}",
                method="GET",
                connector_id=str(connector_id),
                connector_key=connector.key,
                connector_kind=connector.kind.value,
                connector_version=connector.version,
                connector_enabled=connector.is_enabled,
                http_status=200,
                success=True,
            )
            
            return response

        except NotFoundException as e:
            logger.error(
                "connector_api_get_not_found",
                endpoint="/api/v1/connectors/{connector_id}",
                method="GET",
                connector_id=str(connector_id),
                error_type="not_found",
                error_message=str(e),
                http_status=404,
                success=False,
            )
            # Re-raise as is - already properly formatted
            raise e

        except ValueError as e:
            logger.error(
                "connector_api_get_validation_error",
                endpoint="/api/v1/connectors/{connector_id}",
                method="GET",
                connector_id=str(connector_id),
                error_type="validation_error",
                error_message=str(e),
                http_status=400,
                success=False,
                exc_info=True,
            )
            raise BadRequestException(f"Invalid connector ID format: {str(e)}")

        except Exception as e:
            logger.error(
                "connector_api_get_internal_error",
                endpoint="/api/v1/connectors/{connector_id}",
                method="GET",
                connector_id=str(connector_id),
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=500,
                success=False,
                exc_info=True,
            )
            raise InternalServerException(f"Failed to retrieve connector: {str(e)}")


@router.post(
    "/test-connection",
    response_model=TestConnectionResponse,
    responses={
        200: {"description": "Connection test result"},
        400: {"description": "Invalid connection parameters"},
        404: {"description": "Connector not found"},
        500: {"description": "Internal server error"},
    },
)
async def test_connection(
    request: TestConnectionRequest, db: AsyncSession = Depends(get_db_session)
):
    """
    Test a connection to a data source

    - **connector_key**: Key of the connector to test
    - **connector_version**: Version of the connector
    - **config**: Connection configuration
    - **credentials**: Connection credentials

    Returns success status and any error messages.
    """
    with tracer.start_as_current_span("connector.api.test_connection"):
        # Log config metadata without sensitive data
        config_keys = list(request.config.keys()) if request.config else []
        
        logger.info(
            "connector_api_test_start",
            endpoint="/api/v1/connectors/test-connection",
            method="POST",
            connector_key=request.connector_key,
            connector_version=request.connector_version or "latest",
            config_keys=config_keys,
            config_count=len(config_keys),
        )
        
        try:
            service = ConnectorService(db)
            result = await service.test_connection(request)
            
            logger.info(
                "connector_api_test_success",
                endpoint="/api/v1/connectors/test-connection",
                method="POST",
                connector_key=request.connector_key,
                connector_version=request.connector_version,
                test_success=result.success,
                test_message=result.message,
                test_details_keys=list(result.details.keys()) if result.details else [],
                http_status=200,
                api_success=True,
            )
            
            return result

        except NotFoundException as e:
            logger.error(
                "connector_api_test_not_found",
                endpoint="/api/v1/connectors/test-connection",
                method="POST",
                connector_key=request.connector_key,
                connector_version=request.connector_version,
                error_type="connector_not_found",
                error_message=str(e),
                http_status=404,
                api_success=False,
            )
            # Connector not found
            raise e

        except ValidationError as e:
            logger.error(
                "connector_api_test_validation_error",
                endpoint="/api/v1/connectors/test-connection",
                method="POST",
                connector_key=request.connector_key,
                connector_version=request.connector_version,
                error_type="validation_error",
                error_message=str(e),
                http_status=400,
                api_success=False,
                exc_info=True,
            )
            raise BadRequestException(f"Invalid connection parameters: {str(e)}")

        except TimeoutError as e:
            logger.error(
                "connector_api_test_timeout",
                endpoint="/api/v1/connectors/test-connection",
                method="POST",
                connector_key=request.connector_key,
                connector_version=request.connector_version,
                error_type="timeout_error",
                error_message=str(e),
                http_status=200,
                api_success=False,
            )
            return TestConnectionResponse(
                success=False,
                message="Connection timeout",
                details={"error": "Connection attempt timed out"},
            )

        except ConnectionError as e:
            logger.error(
                "connector_api_test_connection_error",
                endpoint="/api/v1/connectors/test-connection",
                method="POST",
                connector_key=request.connector_key,
                connector_version=request.connector_version,
                error_type="connection_error",
                error_message=str(e),
                http_status=200,
                api_success=False,
            )
            return TestConnectionResponse(
                success=False, message="Connection failed", details={"error": str(e)}
            )

        except Exception as e:
            logger.error(
                "connector_api_test_unexpected_error",
                endpoint="/api/v1/connectors/test-connection",
                method="POST",
                connector_key=request.connector_key,
                connector_version=request.connector_version,
                error_type=type(e).__name__,
                error_message=str(e),
                http_status=200,
                api_success=False,
                exc_info=True,
            )
            # Return error response instead of raising for connection tests
            return TestConnectionResponse(
                success=False, message="Connection test failed", details={"error": str(e)}
            )
