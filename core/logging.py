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


class CentralizedLogger:
    """Centralized logger with OpenTelemetry integration for both structured logging and tracing"""

    def __init__(self, name: str = __name__):
        self.name = name
        self.logger = structlog.get_logger(name)
        self.tracer = trace.get_tracer(name)
        # Disable OTEL logging for Jaeger compatibility
        self._otel_logging_enabled = False

    def _log_with_trace(self, level: str, event: str, **kwargs):
        """Log with OpenTelemetry trace context and span attributes"""
        span = trace.get_current_span()

        # Add event and attributes to span if recording (for Jaeger UI visibility)
        if span and span.is_recording():
            # Create a comprehensive log event with timestamp
            import time
            event_attributes = {
                "level": level.upper(),
                "logger": self.name,
                "timestamp": int(time.time() * 1000),  # milliseconds
                "event_name": event,
            }
            
            # Add all kwargs as event attributes
            for key, value in kwargs.items():
                if key != "exc_info":  # Skip exc_info for events
                    if isinstance(value, (dict, list)):
                        # Convert complex types to strings for event attributes
                        event_attributes[f"{key}"] = str(value)
                    else:
                        event_attributes[f"{key}"] = value
            
            # Add as span event - this will appear in Jaeger UI logs section
            span.add_event(f"[{level.upper()}] {event}", attributes=event_attributes)
            
            # Also add key attributes to span for searchability in Jaeger UI
            # Use safe attribute names and handle large values
            safe_attributes = {}
            for key, value in kwargs.items():
                if key != "exc_info":
                    attr_key = f"log.{key}"
                    try:
                        # Handle different value types safely
                        if isinstance(value, (str, int, float, bool)):
                            # Check for overly large integers that might cause protobuf issues
                            if isinstance(value, int) and (value > 2**63 - 1 or value < -2**63):
                                safe_attributes[attr_key] = str(value)
                            else:
                                safe_attributes[attr_key] = value
                        elif isinstance(value, (dict, list)):
                            # Convert to JSON string for complex objects
                            import json
                            safe_attributes[attr_key] = json.dumps(value, default=str)[:500]  # Limit length
                        else:
                            safe_attributes[attr_key] = str(value)[:500]  # Limit string length
                    except Exception:
                        # Fallback to string representation if anything fails
                        safe_attributes[attr_key] = str(value)[:100]
            
            # Set span attributes in batches to avoid issues
            for key, value in safe_attributes.items():
                try:
                    span.set_attribute(key, value)
                except Exception:
                    # If individual attribute fails, skip it
                    pass

            # Set span status on error levels
            if level in ["error", "critical"]:
                try:
                    from opentelemetry.trace import Status, StatusCode
                    span.set_status(Status(StatusCode.ERROR, event))
                except Exception:
                    pass

        # Only try OTEL logging if it's enabled (which it's not for Jaeger compatibility)
        if hasattr(self, '_otel_logging_enabled') and self._otel_logging_enabled:
            self._send_to_otel_logging(level, event, **kwargs)

        # Log using structlog (for structured logging flexibility)
        try:
            log_method = getattr(self.logger, level)
            log_method(event, **kwargs)
        except Exception:
            # Fallback if structlog fails
            import logging
            std_logger = logging.getLogger(self.name)
            getattr(std_logger, level, std_logger.info)(f"{event}: {kwargs}")

    def _send_to_otel_logging(self, level: str, event: str, **kwargs):
        """Send log directly to OpenTelemetry logging handler with proper trace correlation"""
        try:
            from opentelemetry.sdk._logs import LogRecord, LoggerProvider
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.trace.status import StatusCode
            from opentelemetry.util.types import Attributes
            from core.observability import get_logging_handler
            from core.config import settings
            import time
            import logging as stdlib_logging
            
            # First try the handler approach
            otel_handler = get_logging_handler()
            if otel_handler:
                # Convert level to numeric
                level_map = {
                    "debug": stdlib_logging.DEBUG,
                    "info": stdlib_logging.INFO,
                    "warning": stdlib_logging.WARNING,
                    "warn": stdlib_logging.WARNING,
                    "error": stdlib_logging.ERROR,
                    "critical": stdlib_logging.CRITICAL,
                }
                
                numeric_level = level_map.get(level, stdlib_logging.INFO)
                
                # Create a log record
                record = stdlib_logging.LogRecord(
                    name=self.name,
                    level=numeric_level,
                    pathname="",
                    lineno=0,
                    msg=event,
                    args=(),
                    exc_info=kwargs.get("exc_info"),
                )
                
                # Add trace context for correlation
                span = trace.get_current_span()
                if span and span.is_recording():
                    span_context = span.get_span_context()
                    record.otelTraceID = format(span_context.trace_id, "032x")
                    record.otelSpanID = format(span_context.span_id, "016x")
                    
                    # Also set standard OpenTelemetry attributes
                    setattr(record, "trace_id", span_context.trace_id)
                    setattr(record, "span_id", span_context.span_id)
                    setattr(record, "trace_flags", span_context.trace_flags)
                
                # Add service information for correlation
                setattr(record, "service.name", settings.OTEL_SERVICE_NAME)
                setattr(record, "service.version", settings.OTEL_SERVICE_VERSION)
                setattr(record, "environment", settings.ENVIRONMENT.value)
                
                # Add all kwargs as record attributes
                for key, value in kwargs.items():
                    if key != "exc_info":  # exc_info already handled
                        try:
                            # Flatten nested structures for better visibility
                            if isinstance(value, (dict, list)):
                                setattr(record, key, str(value))
                            else:
                                setattr(record, key, value)
                        except (TypeError, ValueError):
                            setattr(record, key, str(value))
                
                # Send to OpenTelemetry handler
                otel_handler.emit(record)
            
            # Also try direct logger provider approach for better correlation
            try:
                from opentelemetry.sdk._logs import get_logger_provider
                
                logger_provider = get_logger_provider()
                if logger_provider and hasattr(logger_provider, "get_logger"):
                    otel_logger = logger_provider.get_logger(
                        name=self.name,
                        version=settings.OTEL_SERVICE_VERSION,
                    )
                    
                    # Create log record with proper trace correlation
                    span = trace.get_current_span()
                    trace_id = None
                    span_id = None
                    trace_flags = None
                    
                    if span and span.is_recording():
                        span_context = span.get_span_context()
                        trace_id = span_context.trace_id
                        span_id = span_context.span_id
                        trace_flags = span_context.trace_flags
                    
                    # Prepare attributes
                    attributes = {
                        "event": event,
                        "level": level,
                        "logger": self.name,
                        "service.name": settings.OTEL_SERVICE_NAME,
                        "service.version": settings.OTEL_SERVICE_VERSION,
                        "environment": settings.ENVIRONMENT.value,
                    }
                    
                    # Add kwargs as attributes
                    for key, value in kwargs.items():
                        if key != "exc_info":
                            if isinstance(value, (dict, list)):
                                attributes[key] = str(value)
                            else:
                                attributes[key] = value
                    
                    # Emit the log record
                    otel_logger.emit(
                        LogRecord(
                            timestamp=time.time_ns(),
                            trace_id=trace_id,
                            span_id=span_id,
                            trace_flags=trace_flags,
                            severity_text=level.upper(),
                            severity_number=stdlib_logging.getLevelName(level.upper()) if hasattr(stdlib_logging, 'getLevelName') else getattr(stdlib_logging, level.upper(), 20),
                            body=event,
                            resource=Resource.create({
                                "service.name": settings.OTEL_SERVICE_NAME,
                                "service.version": settings.OTEL_SERVICE_VERSION,
                                "environment": settings.ENVIRONMENT.value,
                            }),
                            attributes=attributes,
                        )
                    )
            except Exception:
                # Direct logger approach failed, continue with handler approach
                pass
            
        except Exception:
            # Don't let OpenTelemetry errors break regular logging
            pass

    def debug(self, event: str, **kwargs):
        """Log debug message with OpenTelemetry integration"""
        self._log_with_trace("debug", event, **kwargs)

    def info(self, event: str, **kwargs):
        """Log info message with OpenTelemetry integration"""
        self._log_with_trace("info", event, **kwargs)

    def warning(self, event: str, **kwargs):
        """Log warning message with OpenTelemetry integration"""
        self._log_with_trace("warning", event, **kwargs)

    def warn(self, event: str, **kwargs):
        """Alias for warning to match standard logging interface"""
        self.warning(event, **kwargs)

    def error(self, event: str, **kwargs):
        """Log error message with OpenTelemetry integration"""
        self._log_with_trace("error", event, **kwargs)

    def critical(self, event: str, **kwargs):
        """Log critical message with OpenTelemetry integration"""
        self._log_with_trace("critical", event, **kwargs)

    def exception(self, event: str, **kwargs):
        """Log exception with traceback and OpenTelemetry integration"""
        # Add exc_info=True to capture the exception traceback
        kwargs["exc_info"] = True
        self._log_with_trace("error", event, **kwargs)
        
        # Record the exception in the current span
        span = trace.get_current_span()
        if span and span.is_recording():
            import sys
            exc_type, exc_value, exc_traceback = sys.exc_info()
            if exc_value:
                span.record_exception(exc_value)

    def with_context(self, **kwargs):
        """Return a new logger instance with added context"""
        # Create a new bound logger with context
        bound_logger = self.logger.bind(**kwargs)
        
        # Create new CentralizedLogger instance with the bound logger
        new_logger = CentralizedLogger(self.name)
        new_logger.logger = bound_logger
        return new_logger

    def bind(self, **kwargs):
        """Alias for with_context to match structlog interface"""
        return self.with_context(**kwargs)


def get_logger(name: str) -> CentralizedLogger:
    """Get a configured centralized logger instance."""
    return CentralizedLogger(name)
