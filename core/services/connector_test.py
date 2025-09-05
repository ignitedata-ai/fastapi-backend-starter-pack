from typing import Dict, Any, Optional, Tuple
import asyncpg
import aioboto3
from opentelemetry import trace
from core.models import ConnectorDefinition
from core.logging import get_logger
import aiomysql

logger = get_logger(__name__)
tracer = trace.get_tracer(__name__)


async def test_connector_connection(
    connector: ConnectorDefinition,
    config: Dict[str, Any],
    secrets: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Test connection to a data source based on connector type"""
    with tracer.start_as_current_span("connector.test.connection"):
        # Log config metadata without sensitive data
        config_keys = list(config.keys()) if config else []
        secrets_keys = list(secrets.keys()) if secrets else []
        
        logger.info(
            "connector_test_start",
            operation="test_connector_connection",
            connector_key=connector.key,
            connector_kind=connector.kind.value,
            connector_version=connector.version,
            config_keys=config_keys,
            config_count=len(config_keys),
            secrets_provided=bool(secrets),
            secrets_count=len(secrets_keys),
        )
        
        try:
            if connector.key == "postgres":
                result = await test_postgres_connection(config, secrets)
            elif connector.key == "mysql":
                result = await test_mysql_connection(config, secrets)
            elif connector.key == "s3":
                result = await test_s3_connection(config, secrets)
            elif connector.key == "snowflake":
                result = await test_snowflake_connection(config, secrets)
            else:
                logger.warning(
                    "connector_test_not_implemented",
                    operation="test_connector_connection",
                    connector_key=connector.key,
                    connector_kind=connector.kind.value,
                    test_implemented=False,
                )
                result = False, f"Connector {connector.key} testing not implemented", None
            
            success, message, details = result
            
            logger.info(
                "connector_test_complete",
                operation="test_connector_connection",
                connector_key=connector.key,
                connector_kind=connector.kind.value,
                test_success=success,
                test_message=message,
                test_details_keys=list(details.keys()) if details else [],
                operation_success=True,
            )
            
            return result
            
        except Exception as e:
            logger.error(
                "connector_test_error",
                operation="test_connector_connection",
                connector_key=connector.key,
                connector_kind=connector.kind.value,
                error_type=type(e).__name__,
                error_message=str(e),
                operation_success=False,
                exc_info=True,
            )
            return False, str(e), None


async def test_postgres_connection(
    config: Dict[str, Any],
    secrets: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Test PostgreSQL connection"""
    with tracer.start_as_current_span("connector.test.postgres"):
        logger.info(
            "postgres_connection_test_start",
            connector_type="postgres",
            host=config.get("host", "localhost"),
            port=config.get("port", 5432),
            database=config.get("database", "unknown"),
            user=config.get("user", "unknown"),
        )
        
        try:
            conn_params = {
                "host": config.get("host", "localhost"),
                "port": config.get("port", 5432),
                "database": config.get("database"),
                "user": config.get("user"),
            }
            
            if secrets and "password" in secrets:
                conn_params["password"] = secrets["password"]
            
            # Test connection
            conn = await asyncpg.connect(**conn_params)
            
            # Run simple query
            version = await conn.fetchval("SELECT version()")
            await conn.close()
            
            logger.info(
                "postgres_connection_test_success",
                connector_type="postgres",
                host=config.get("host"),
                port=config.get("port"),
                database=config.get("database"),
                postgres_version=version[:100] if version else "unknown",
                test_success=True,
            )
            
            return True, "Connection successful", {"version": version}
            
        except Exception as e:
            logger.error(
                "postgres_connection_test_error",
                connector_type="postgres",
                host=config.get("host"),
                port=config.get("port"),
                database=config.get("database"),
                error_type=type(e).__name__,
                error_message=str(e),
                test_success=False,
                exc_info=True,
            )
            return False, f"Connection failed: {str(e)}", None


async def test_mysql_connection(
    config: Dict[str, Any],
    secrets: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Test MySQL connection"""
    with tracer.start_as_current_span("connector.test.mysql"):
        logger.info(
            "mysql_connection_test_start",
            connector_type="mysql",
            host=config.get("host", "localhost"),
            port=config.get("port", 3306),
            database=config.get("database", "unknown"),
            user=config.get("user", "unknown"),
        )
        
        try:
            # Build connection parameters
            conn_params = {
                "host": config.get("host", "localhost"),
                "port": config.get("port", 3306),
                "user": config.get("user"),
                "db": config.get("database"),
                "charset": "utf8mb4",
                "autocommit": True
            }
            
            if secrets and "password" in secrets:
                conn_params["password"] = secrets["password"]
            
            # Test connection
            conn = await aiomysql.connect(**conn_params)
            
            try:
                async with conn.cursor() as cursor:
                    await cursor.execute("SELECT VERSION()")
                    result = await cursor.fetchone()
                    version = result[0] if result else "Unknown"
                
                logger.info(
                    "mysql_connection_test_success",
                    connector_type="mysql",
                    host=config.get("host"),
                    port=config.get("port"),
                    database=config.get("database"),
                    mysql_version=version[:100] if version else "unknown",
                    test_success=True,
                )
                
                return True, "Connection successful", {"version": version}
            finally:
                await conn.ensure_closed()
            
        except ImportError as e:
            logger.error(
                "mysql_connection_test_dependency_error",
                connector_type="mysql",
                host=config.get("host"),
                port=config.get("port"),
                database=config.get("database"),
                error_type="dependency_missing",
                error_message=str(e),
                test_success=False,
            )
            return False, "aiomysql dependency not available", None
        except Exception as e:
            logger.error(
                "mysql_connection_test_error",
                connector_type="mysql",
                host=config.get("host"),
                port=config.get("port"),
                database=config.get("database"),
                error_type=type(e).__name__,
                error_message=str(e),
                test_success=False,
                exc_info=True,
            )
            return False, f"Connection failed: {str(e)}", None


async def test_s3_connection(
    config: Dict[str, Any],
    secrets: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Test S3 connection"""
    with tracer.start_as_current_span("connector.test.s3"):
        logger.info(
            "s3_connection_test_start",
            connector_type="s3",
            region=config.get("region", "us-east-1"),
            bucket=config.get("bucket", "none"),
            test_specific_bucket="bucket" in config,
        )
        
        try:
            session_params = {
                "region_name": config.get("region", "us-east-1")
            }
            
            if secrets:
                if "access_key_id" in secrets:
                    session_params["aws_access_key_id"] = secrets["access_key_id"]
                if "secret_access_key" in secrets:
                    session_params["aws_secret_access_key"] = secrets["secret_access_key"]
            
            session = aioboto3.Session(**session_params)
            
            async with session.client('s3') as s3:
                # Test by listing buckets or checking specific bucket
                if "bucket" in config:
                    response = await s3.head_bucket(Bucket=config["bucket"])
                    
                    logger.info(
                        "s3_connection_test_success_bucket",
                        connector_type="s3",
                        region=config.get("region"),
                        target_bucket=config["bucket"],
                        test_success=True,
                    )
                    
                    return True, "Connection successful", {"bucket": config["bucket"]}
                else:
                    response = await s3.list_buckets()
                    buckets = [b['Name'] for b in response.get('Buckets', [])]
                    
                    logger.info(
                        "s3_connection_test_success_list",
                        connector_type="s3",
                        region=config.get("region"),
                        buckets_count=len(buckets),
                        sample_buckets=buckets[:3],
                        test_success=True,
                    )
                    
                    return True, "Connection successful", {"buckets": buckets[:5]}
            
        except Exception as e:
            logger.error(
                "s3_connection_test_error",
                connector_type="s3",
                region=config.get("region"),
                bucket=config.get("bucket"),
                error_type=type(e).__name__,
                error_message=str(e),
                test_success=False,
                exc_info=True,
            )
            return False, f"Connection failed: {str(e)}", None


async def test_snowflake_connection(
    config: Dict[str, Any],
    secrets: Optional[Dict[str, Any]] = None
) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
    """Test Snowflake connection"""
    with tracer.start_as_current_span("connector.test.snowflake"):
        logger.warning(
            "snowflake_connection_test_not_implemented",
            connector_type="snowflake",
            test_implemented=False,
            account=config.get("account", "unknown"),
            warehouse=config.get("warehouse", "unknown"),
            database=config.get("database", "unknown"),
            test_success=False,
        )
        
        # Placeholder for Snowflake connection testing
        return False, "Snowflake connector testing not implemented", None