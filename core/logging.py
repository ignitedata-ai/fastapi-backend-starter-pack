import logging
import sys
import uuid
from contextvars import ContextVar
from typing import Any, Optional

import structlog
from opentelemetry import trace

from core.config import settings

# Context variable to store correlation ID across async requests
correlation_id_var: ContextVar[Optional[str]] = ContextVar("correlation_id", default=None)


def get_correlation_id() -> str:
    """Get or create a correlation ID for the current request context."""
    correlation_id = correlation_id_var.get()
    if correlation_id is None:
        correlation_id = str(uuid.uuid4())
        correlation_id_var.set(correlation_id)
    return correlation_id


def set_correlation_id(correlation_id: str) -> None:
    """Set the correlation ID for the current request context."""
    correlation_id_var.set(correlation_id)


def add_correlation_id(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Add correlation ID to log entries."""
    event_dict["correlation_id"] = get_correlation_id()
    return event_dict


def add_service_info(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Add service information to log entries."""
    event_dict["service"] = settings.OTEL_SERVICE_NAME
    event_dict["version"] = settings.OTEL_SERVICE_VERSION
    event_dict["environment"] = settings.ENVIRONMENT.value
    return event_dict


def get_trace_id() -> Optional[str]:
    """Get the current trace ID from OpenTelemetry context."""
    try:
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().trace_id != trace.INVALID_TRACE_ID:
            # Convert trace ID to hex string (32 characters, zero-padded)
            return f"{current_span.get_span_context().trace_id:032x}"
    except Exception:
        pass
    return None


def get_span_id() -> Optional[str]:
    """Get the current span ID from OpenTelemetry context."""
    try:
        current_span = trace.get_current_span()
        if current_span and current_span.get_span_context().span_id != trace.INVALID_SPAN_ID:
            # Convert span ID to hex string (16 characters, zero-padded)
            return f"{current_span.get_span_context().span_id:016x}"
    except Exception:
        pass
    return None


def add_trace_context(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Add trace and span IDs to log entries."""
    trace_id = get_trace_id()
    span_id = get_span_id()

    if trace_id:
        event_dict["trace_id"] = trace_id
    if span_id:
        event_dict["span_id"] = span_id

    return event_dict


def add_otel_logging(_logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    """Send log entries to OpenTelemetry logging."""
    # Import here to avoid circular imports
    from core.observability import get_logging_handler

    # Get the OpenTelemetry logging handler
    otel_handler = get_logging_handler()
    if not otel_handler:
        return event_dict

    try:
        # Convert structlog level to standard logging level
        level_map = {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "warn": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }

        # Get log level from event_dict or method_name
        log_level = event_dict.get("level", method_name)
        if isinstance(log_level, str):
            log_level = log_level.lower()

        numeric_level = level_map.get(log_level, logging.INFO)

        # Create a log record
        record = logging.LogRecord(
            name=event_dict.get("logger", "structlog"),
            level=numeric_level,
            pathname="",
            lineno=0,
            msg=event_dict.get("event", ""),
            args=(),
            exc_info=None,
        )

        # Add additional attributes to the record
        record.correlation_id = event_dict.get("correlation_id")
        record.service = event_dict.get("service")
        record.version = event_dict.get("version")
        record.environment = event_dict.get("environment")

        # Add any extra fields as attributes
        for key, value in event_dict.items():
            if key not in ["event", "level", "logger", "correlation_id", "service", "version", "environment", "timestamp"]:
                setattr(record, key, value)

        # Send to OpenTelemetry
        otel_handler.emit(record)

    except Exception:
        # Don't let OpenTelemetry errors break regular logging
        pass

    return event_dict


def configure_logging() -> None:
    """Configure structured logging with structlog."""
    # Configure stdlib logging
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, settings.LOG_LEVEL.value),
    )

    # Common processors for all loggers
    common_processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso"),
        add_correlation_id,
        add_trace_context,  # Add trace and span IDs to logs
        add_service_info,
        add_otel_logging,  # Send logs to OpenTelemetry
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.LOG_FORMAT == "json":
        # JSON formatter for production
        renderer = structlog.processors.JSONRenderer()
    else:
        # Human-readable formatter for development
        renderer = structlog.dev.ConsoleRenderer(colors=True if settings.ENVIRONMENT == "development" else False)

    # Configure structlog
    structlog.configure(
        processors=common_processors + [renderer],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        context_class=dict,
        cache_logger_on_first_use=True,
    )

    # Add OpenTelemetry handler to root logger if available
    # This ensures that direct Python logging calls also get sent to OpenTelemetry
    try:
        from core.observability import get_logging_handler

        otel_handler = get_logging_handler()
        if otel_handler:
            root_logger = logging.getLogger()
            root_logger.addHandler(otel_handler)
    except ImportError:
        # OpenTelemetry may not be fully initialized yet
        pass

    # Configure specific loggers
    # Silence noisy third-party loggers in production
    if settings.ENVIRONMENT == "production":
        logging.getLogger("uvicorn").setLevel(logging.WARNING)
        logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
        logging.getLogger("redis").setLevel(logging.WARNING)


def configure_otel_logging() -> None:
    """Configure OpenTelemetry logging integration after OTel is initialized."""
    try:
        from core.observability import get_logging_handler

        otel_handler = get_logging_handler()
        if otel_handler:
            root_logger = logging.getLogger()

            # Check if handler is already added to avoid duplicates
            if otel_handler not in root_logger.handlers:
                root_logger.addHandler(otel_handler)

            # Get the configured logger and log success
            logger = get_logger(__name__)
            logger.info("OpenTelemetry logging integration configured")
    except Exception as e:
        # Don't let OpenTelemetry configuration errors break the application
        logger = get_logger(__name__)
        logger.warning("Failed to configure OpenTelemetry logging integration", error=str(e))


def get_logger(name: str) -> structlog.stdlib.BoundLogger:
    """Get a configured logger instance."""
    return structlog.get_logger(name)
