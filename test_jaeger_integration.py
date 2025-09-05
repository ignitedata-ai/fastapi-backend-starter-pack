#!/usr/bin/env python3
"""Test Jaeger integration with span events instead of OTLP logs"""

import asyncio
import time
from opentelemetry import trace
from core.logging import get_logger, configure_logging
from core.observability import init_observability

async def test_jaeger_span_events():
    """Test logging with span events for Jaeger UI visibility"""
    
    print("=== Testing Jaeger Integration with Span Events ===")
    
    # Initialize observability and logging
    print("1. Initializing observability...")
    init_observability()
    configure_logging()
    
    # Get centralized logger
    logger = get_logger("test.jaeger.integration")
    tracer = trace.get_tracer("test.jaeger.integration")
    
    print("2. Testing logging within span context...")
    
    with tracer.start_as_current_span("jaeger_integration_test") as span:
        # Set span attributes for context
        span.set_attribute("test.type", "jaeger_integration")
        span.set_attribute("test.environment", "development")
        
        print(f"   Trace ID: {span.get_span_context().trace_id:032x}")
        print(f"   Span ID: {span.get_span_context().span_id:016x}")
        
        # Test various log levels - these should appear as span events in Jaeger
        logger.info(
            "test_jaeger_integration_start",
            operation="integration_test",
            test_phase="start",
            timestamp=time.time(),
            success=True
        )
        
        # Simulate some work with nested spans
        with tracer.start_as_current_span("nested_operation") as nested_span:
            logger.debug(
                "nested_operation_start",
                operation="nested_test",
                nested_level=1,
                parent_span=span.get_span_context().span_id
            )
            
            # Simulate processing
            await asyncio.sleep(0.1)
            
            logger.info(
                "nested_operation_success",
                operation="nested_test",
                nested_level=1,
                processing_time=0.1,
                success=True
            )
        
        # Test warning level
        logger.warning(
            "test_jaeger_integration_warning",
            operation="integration_test", 
            test_phase="warning_test",
            warning_type="test_warning",
            message="This is a test warning for Jaeger"
        )
        
        # Test error level (should set span status)
        logger.error(
            "test_jaeger_integration_error",
            operation="integration_test",
            test_phase="error_test",
            error_type="TestError",
            error_message="This is a test error for Jaeger",
            success=False
        )
        
        # Test with complex data
        complex_data = {
            "user_id": "test-user-123",
            "session_data": {
                "browser": "Chrome",
                "ip": "127.0.0.1",
                "features": ["feature1", "feature2"]
            },
            "metrics": {
                "response_time": 150,
                "memory_usage": 45.6
            }
        }
        
        logger.info(
            "test_complex_data_logging",
            operation="integration_test",
            test_phase="complex_data",
            user_data=complex_data,
            array_data=[1, 2, 3, 4, 5],
            success=True
        )
        
        logger.info(
            "test_jaeger_integration_complete",
            operation="integration_test",
            test_phase="complete",
            total_events=6,
            success=True
        )
    
    print("3. Test completed!")
    print("\n=== How to View in Jaeger UI ===")
    print("1. Open Jaeger UI: http://localhost:16686")
    print("2. Select Service: aipal-backend")  
    print("3. Look for Operation: jaeger_integration_test")
    print("4. Click on the trace to see details")
    print("5. In trace details, look for:")
    print("   - Span events with log messages (e.g., '[INFO] test_jaeger_integration_start')")
    print("   - Span attributes prefixed with 'log.*'")
    print("   - Nested spans showing the operation hierarchy")
    print("   - Error status on spans with error logs")
    print("\n✓ Logs should now appear as SPAN EVENTS in Jaeger UI")
    print("✓ This approach works with Jaeger all-in-one setup")

if __name__ == "__main__":
    asyncio.run(test_jaeger_span_events())