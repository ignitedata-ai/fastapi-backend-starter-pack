#!/usr/bin/env python3
"""Check available OpenTelemetry OTLP packages and their import paths"""

def check_otlp_packages():
    """Check which OTLP packages and import paths are available"""
    
    print("=== OpenTelemetry OTLP Package Check ===")
    
    # Check trace exporters
    print("\n--- Trace Exporters ---")
    trace_imports = [
        "opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter",
        "opentelemetry.exporter.otlp.proto.http.trace_exporter.OTLPSpanExporter",
        "opentelemetry.exporter.jaeger.thrift.JaegerExporter",
    ]
    
    for import_path in trace_imports:
        try:
            module_path, class_name = import_path.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            cls = getattr(module, class_name)
            print(f"✓ {import_path}")
        except ImportError as e:
            print(f"✗ {import_path} - ImportError: {e}")
        except Exception as e:
            print(f"? {import_path} - Error: {e}")
    
    # Check log exporters
    print("\n--- Log Exporters ---")
    log_imports = [
        "opentelemetry.exporter.otlp.proto.grpc._log_exporter.OTLPLogExporter",
        "opentelemetry.exporter.otlp.proto.grpc.logs_exporter.OTLPLogExporter",
        "opentelemetry.exporter.otlp.proto.http._log_exporter.OTLPLogExporter",
        "opentelemetry.exporter.otlp.proto.http.logs_exporter.OTLPLogExporter",
    ]
    
    for import_path in log_imports:
        try:
            module_path, class_name = import_path.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            cls = getattr(module, class_name)
            print(f"✓ {import_path}")
        except ImportError as e:
            print(f"✗ {import_path} - ImportError: {e}")
        except Exception as e:
            print(f"? {import_path} - Error: {e}")
    
    # Check core logging components
    print("\n--- Core Logging Components ---")
    core_imports = [
        "opentelemetry.sdk._logs.LoggerProvider",
        "opentelemetry.sdk._logs.LoggingHandler",
        "opentelemetry.sdk._logs.LogRecord",
        "opentelemetry.sdk._logs.export.BatchLogRecordProcessor",
        "opentelemetry._logs.set_logger_provider",
        "opentelemetry._logs.get_logger_provider",
    ]
    
    for import_path in core_imports:
        try:
            module_path, class_name = import_path.rsplit('.', 1)
            module = __import__(module_path, fromlist=[class_name])
            cls = getattr(module, class_name)
            print(f"✓ {import_path}")
        except ImportError as e:
            print(f"✗ {import_path} - ImportError: {e}")
        except Exception as e:
            print(f"? {import_path} - Error: {e}")
    
    # Check installed packages
    print("\n--- Installed OpenTelemetry Packages ---")
    try:
        import pkg_resources
        installed_packages = {pkg.project_name: pkg.version 
                            for pkg in pkg_resources.working_set 
                            if 'opentelemetry' in pkg.project_name.lower()}
        
        for pkg_name, version in sorted(installed_packages.items()):
            print(f"  {pkg_name}: {version}")
    except Exception as e:
        print(f"Could not check installed packages: {e}")
    
    print("\n=== Package Check Complete ===")

if __name__ == "__main__":
    check_otlp_packages()