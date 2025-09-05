from .base import (
    AIpalBaseException,
    AuthenticationError,
    AuthorizationError,
    BadRequestException,
    BusinessLogicError,
    CacheError,
    ConfigurationError,
    DatabaseError,
    ExternalServiceError,
    InternalServerException,
    NotFoundError,
    NotFoundException,
    ValidationError,
)
from .handlers import (
    aipal_exception_handler,
    database_exception_handler,
    database_operational_exception_handler,
    generic_exception_handler,
    http_exception_handler,
    starlette_http_exception_handler,
    validation_exception_handler,
)

__all__ = [
    # Base exceptions
    "AIpalBaseException",
    "AuthenticationError",
    "AuthorizationError",
    "BadRequestException",
    "BusinessLogicError",
    "CacheError",
    "ConfigurationError",
    "DatabaseError",
    "ExternalServiceError",
    "InternalServerException",
    "NotFoundError",
    "NotFoundException",
    "ValidationError",
    # Exception handlers
    "aipal_exception_handler",
    "database_exception_handler",
    "database_operational_exception_handler",
    "generic_exception_handler",
    "http_exception_handler",
    "starlette_http_exception_handler",
    "validation_exception_handler",
]
