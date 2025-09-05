#!/usr/bin/env python3
"""Debug script to test OpenTelemetry logging integration with Jaeger"""

import asyncio
import logging
import time
from opentelemetry import trace
from opentelemetry.sdk._logs import get_logger_provider
from core.logging import get_logger, configure_logging
from core.observability import init_observability, get_logging_handler
from core.config import settings

async def debug_logging_setup():
    """Debug the complete logging setup chain"""
    
    print("=== OpenTelemetry Logging Debug ===")
    print(f"JAEGER_ENABLED: {settings.JAEGER_ENABLED}")
    print(f"JAEGER_LOGS_ENABLED: {settings.JAEGER_LOGS_ENABLED}")
    print(f"JAEGER_AGENT_HOST: {settings.JAEGER_AGENT_HOST}")
    print(f"ENVIRONMENT: {settings.ENVIRONMENT.value}")
    print()
    
    # Step 1: Initialize observability first
    print("1. Initializing observability...")
    try:
        init_observability()
        print("✓ Observability initialized")
    except Exception as e:
        print(f"✗ Observability failed: {e}")
        return
    
    # Step 2: Configure logging
    print("\n2. Configuring logging...")
    try:
        configure_logging()
        print("✓ Logging configured")
    except Exception as e:
        print(f"✗ Logging configuration failed: {e}")
        return
    
    # Step 3: Check OpenTelemetry logging handler
    print("\n3. Checking OpenTelemetry logging handler...")
    otel_handler = get_logging_handler()
    if otel_handler:
        print(f"✓ OpenTelemetry logging handler available: {type(otel_handler)}")
    else:
        print("✗ No OpenTelemetry logging handler found")
    
    # Step 4: Check logger provider
    print("\n4. Checking logger provider...")
    try:
        logger_provider = get_logger_provider()
        if logger_provider:
            print(f"✓ Logger provider available: {type(logger_provider)}")
        else:
            print("✗ No logger provider found")
    except Exception as e:
        print(f"✗ Logger provider check failed: {e}")
    
    # Step 5: Get centralized logger
    print("\n5. Testing centralized logger...")
    try:
        logger = get_logger("debug.test")
        print(f"✓ Centralized logger created: {type(logger)}")
        
        # Check if it has the OTEL methods
        if hasattr(logger, '_send_to_otel_logging'):
            print("✓ Logger has OTEL logging method")
        else:
            print("✗ Logger missing OTEL logging method")
    except Exception as e:
        print(f"✗ Centralized logger creation failed: {e}")
        return
    
    # Step 6: Test with tracing context
    print("\n6. Testing logs with tracing context...")
    tracer = trace.get_tracer("debug.tracer")
    
    try:
        with tracer.start_as_current_span("debug_logging_test") as span:
            print(f"✓ Span created: {span.get_span_context().trace_id:032x}")
            
            # Test different log levels
            logger.info(
                "debug_logging_test_info",
                operation="debug_test",
                test_level="info",
                timestamp=time.time(),
                success=True
            )
            
            logger.warning(
                "debug_logging_test_warning", 
                operation="debug_test",
                test_level="warning",
                message="This is a test warning"
            )
            
            logger.error(
                "debug_logging_test_error",
                operation="debug_test", 
                test_level="error",
                error_type="TestError",
                error_message="This is a test error"
            )
            
            print("✓ Test logs emitted within span context")
            
            # Also test span events directly
            span.add_event("debug_span_event", {
                "event_type": "direct_span_event",
                "test_data": "span event test"
            })
            
            print("✓ Direct span event added")
    except Exception as e:
        print(f"✗ Tracing context test failed: {e}")
        return
    
    # Step 7: Test without tracing context
    print("\n7. Testing logs without tracing context...")
    try:
        logger.info(
            "debug_logging_test_no_trace",
            operation="debug_test",
            context="no_trace",
            success=True
        )
        print("✓ Log emitted without tracing context")
    except Exception as e:
        print(f"✗ No-trace logging test failed: {e}")
    
    print("\n=== Debug Complete ===")
    print("If logs are properly configured, you should see them in:")
    print("1. Console output (structured logs)")
    print("2. Jaeger UI under the trace (if OTLP is working)")
    print("3. Span events in Jaeger trace details")
    print()
    print("Check Jaeger UI at: http://localhost:16686")
    print("Look for service:", settings.OTEL_SERVICE_NAME)
    print("Look for operation: debug_logging_test")

async def test_standard_logging():
    """Test standard Python logging integration"""
    print("\n=== Standard Logging Test ===")
    
    # Get standard Python logger
    std_logger = logging.getLogger("debug.standard")
    
    # Test within span
    tracer = trace.get_tracer("debug.standard.tracer")
    with tracer.start_as_current_span("standard_logging_test") as span:
        std_logger.info("Standard Python logging within span")
        std_logger.warning("Standard Python warning within span")
        std_logger.error("Standard Python error within span")
        
        print("✓ Standard logging calls made within span")

if __name__ == "__main__":
    asyncio.run(debug_logging_setup())
    asyncio.run(test_standard_logging())