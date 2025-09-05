#!/usr/bin/env python3
"""Simple test script to verify centralized logging and OpenTelemetry integration"""

import asyncio
from opentelemetry import trace
from core.logging import get_logger, configure_logging
from core.observability import init_observability

async def test_logging():
    """Test the centralized logging functionality"""
    
    # Initialize observability
    print("Initializing observability...")
    init_observability()
    
    # Configure logging
    print("Configuring logging...")
    configure_logging()
    
    # Get logger
    logger = get_logger("test.connector")
    
    # Create a tracer and start a span
    tracer = trace.get_tracer("test.tracer")
    
    print("Starting span and testing logging...")
    with tracer.start_as_current_span("test_operation") as span:
        # Test different log levels
        logger.info(
            "connector_api_list_start",
            endpoint="/api/v1/connectors/supported",
            method="GET",
            filter_kind="none",
            filter_enabled=True,
        )
        
        logger.info(
            "test_operation_success", 
            operation="test_logging",
            total_count=5,
            connector_keys=["postgres", "mysql", "s3"],
            success=True
        )
        
        logger.warning(
            "test_warning",
            message="This is a test warning",
            component="test"
        )
        
        logger.error(
            "test_error",
            error_type="TestError",
            error_message="This is a test error",
            success=False
        )
    
    print("Logging test completed!")
    print("Check Jaeger UI for traces and logs")
    print("- Span events should appear in the 'Logs' section of the span")
    print("- Span attributes should include log.* prefixed attributes")

if __name__ == "__main__":
    asyncio.run(test_logging())