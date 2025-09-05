"""
MySQL metadata extractor
"""
import asyncio
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple

from core.models.enums import AssetType
from core.logging import CentralizedLogger
from .base import (
    BaseMetadataExtractor,
    ExtractionResult,
    DatabaseMetadata,
    SchemaMetadata,
    TableMetadata,
    ColumnMetadata,
    MetadataExtractionStatus
)

logger = CentralizedLogger(__name__)

# Optional import - allows the module to load even if aiomysql is not installed
try:
    import aiomysql
    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False
    logger.warning("aiomysql not available - MySQL metadata extraction will not work")


class MySQLMetadataExtractor(BaseMetadataExtractor):
    """MySQL/MariaDB metadata extractor"""
    
    def _convert_decimals(self, obj):
        """Recursively convert Decimal objects to int/float for JSON serialization"""
        if isinstance(obj, Decimal):
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        elif isinstance(obj, dict):
            return {key: self._convert_decimals(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_decimals(item) for item in obj]
        else:
            return obj
    
    def __init__(self, data_source_id, tenant_id, config: Dict[str, Any], credentials: Dict[str, Any]):
        super().__init__(data_source_id, tenant_id, config, credentials)
        
        # Double-check aiomysql availability at runtime
        if not AIOMYSQL_AVAILABLE:
            # Try one more time to import in case there was a temporary issue
            try:
                import aiomysql
                logger.warning(
                    "mysql_extractor_aiomysql_recovered",
                    message="aiomysql import succeeded at runtime despite module-level failure"
                )
            except ImportError:
                import sys
                raise ImportError(
                    f"aiomysql is required for MySQL metadata extraction. "
                    f"Install it with: pip install aiomysql==0.2.0. "
                    f"Current Python executable: {sys.executable}. "
                    f"AIOMYSQL_AVAILABLE flag was set to False during module import."
                )
        
        # Default MySQL port
        self.host = config.get("host", "localhost")
        self.port = int(config.get("port", 3306))
        self.database = config.get("database")
        
        # Credentials
        self.username = credentials.get("username")
        self.password = credentials.get("password")
        
        # Connection options
        self.ssl_enabled = config.get("ssl_enabled", False)
        self.connection_timeout = int(config.get("connection_timeout", 30))
        self.read_timeout = int(config.get("read_timeout", 30))
    
    def get_supported_asset_types(self) -> List[AssetType]:
        return [
            AssetType.DATABASE,
            AssetType.SCHEMA,  # MySQL doesn't have schemas but we'll use this for databases
            AssetType.TABLE,
            AssetType.VIEW,
            AssetType.COLUMN
        ]
    
    async def test_connection(self) -> Tuple[bool, Optional[str]]:
        """Test connection to MySQL"""
        try:
            connection = await self._create_connection()
            if connection:
                await connection.ensure_closed()
                return True, None
        except Exception as e:
            logger.error(
                "mysql_connection_test_failed",
                data_source_id=str(self.data_source_id),
                error=str(e)
            )
            return False, str(e)
        
        return False, "Failed to establish connection"
    
    async def extract_metadata(self) -> ExtractionResult:
        """Extract metadata from MySQL"""
        databases = []
        schemas = []
        tables = []
        columns = []
        errors = []
        warnings = []
        
        try:
            logger.info(
                "mysql_metadata_extraction_started",
                data_source_id=str(self.data_source_id),
                host=self.host,
                database=self.database
            )
            
            connection = await self._create_connection()
            if not connection:
                errors.append("Failed to connect to MySQL")
                return ExtractionResult(
                    status=MetadataExtractionStatus.FAILED,
                    databases=databases,
                    schemas=schemas,
                    tables=tables,
                    columns=columns,
                    errors=errors,
                    warnings=warnings
                )
            
            try:
                # Only extract the configured database
                if not self.database:
                    errors.append("No database specified in configuration")
                    return ExtractionResult(
                        status=MetadataExtractionStatus.FAILED,
                        databases=databases,
                        schemas=schemas,
                        tables=tables,
                        columns=columns,
                        errors=errors,
                        warnings=warnings
                    )
                
                # Extract information for the configured database only
                db = await self._extract_single_database(connection, self.database)
                if db:
                    databases.append(db)
                    
                    # Use the specific database
                    await self._use_database(connection, db.name)
                    
                    # Extract tables for this database
                    db_tables = await self._extract_tables(connection, db.name)
                    tables.extend(db_tables)
                    
                    # Extract columns for each table
                    for table in db_tables:
                        table_columns = await self._extract_columns(connection, db.name, table.name)
                        columns.extend(table_columns)
                    
                    # Create a schema entry for the database (MySQL paradigm)
                    schema = SchemaMetadata(
                        name=db.name,
                        qualified_name=self._build_qualified_name("mysql", self.host, db.name, "schema"),
                        database_name=db.name,
                        properties=db.properties,
                        comment=db.comment
                    )
                    schemas.append(schema)
                else:
                    errors.append(f"Database '{self.database}' not found or not accessible")
                
            finally:
                await connection.ensure_closed()
            
            status = MetadataExtractionStatus.SUCCESS
            if errors:
                status = MetadataExtractionStatus.PARTIAL if (databases or tables) else MetadataExtractionStatus.FAILED
            
            logger.info(
                "mysql_metadata_extraction_completed",
                data_source_id=str(self.data_source_id),
                databases_found=len(databases),
                tables_found=len(tables),
                columns_found=len(columns),
                errors=len(errors),
                warnings=len(warnings)
            )
            
            return ExtractionResult(
                status=status,
                databases=databases,
                schemas=schemas,
                tables=tables,
                columns=columns,
                errors=errors,
                warnings=warnings
            )
        
        except Exception as e:
            error_msg = f"MySQL metadata extraction failed: {str(e)}"
            logger.error(
                "mysql_metadata_extraction_error",
                data_source_id=str(self.data_source_id),
                error=error_msg
            )
            errors.append(error_msg)
            
            return ExtractionResult(
                status=MetadataExtractionStatus.FAILED,
                databases=databases,
                schemas=schemas,
                tables=tables,
                columns=columns,
                errors=errors,
                warnings=warnings
            )
    
    async def _create_connection(self):
        """Create async MySQL connection"""
        try:
            connection = await aiomysql.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                db=self.database if self.database else None,
                charset='utf8mb4',
                connect_timeout=self.connection_timeout,
                autocommit=True,
                cursorclass=aiomysql.DictCursor
            )
            return connection
        except Exception as e:
            logger.error(
                "mysql_connection_failed",
                data_source_id=str(self.data_source_id),
                host=self.host,
                port=self.port,
                database=self.database,
                error=str(e)
            )
            raise
    
    async def _use_database(self, connection, database_name: str):
        """Switch to a specific database"""
        try:
            async with connection.cursor() as cursor:
                await cursor.execute(f"USE `{database_name}`")
        except Exception as e:
            logger.warning(
                "mysql_use_database_failed",
                database=database_name,
                error=str(e)
            )
            raise
    
    async def _extract_single_database(self, connection, database_name: str) -> Optional[DatabaseMetadata]:
        """Extract information for a specific database"""
        try:
            async with connection.cursor() as cursor:
                # Get specific database info
                await cursor.execute("""
                    SELECT 
                        SCHEMA_NAME as database_name,
                        SCHEMA_NAME as schema_name,
                        DEFAULT_CHARACTER_SET_NAME as charset,
                        DEFAULT_COLLATION_NAME as collation
                    FROM information_schema.SCHEMATA
                    WHERE SCHEMA_NAME = %s
                """, (database_name,))
                
                row = await cursor.fetchone()
                if not row:
                    logger.warning(
                        "database_not_found",
                        database_name=database_name,
                        data_source_id=str(self.data_source_id)
                    )
                    return None
                
                db_name = row['database_name']
                
                # Get database size and table count
                await cursor.execute("""
                    SELECT 
                        IFNULL(SUM(DATA_LENGTH + INDEX_LENGTH), 0) as size_bytes,
                        COUNT(*) as table_count
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                """, (db_name,))
                
                stats = await cursor.fetchone()
                
                properties = {
                    "host": self.host,
                    "port": self.port,
                    "charset": row.get('charset'),
                    "collation": row.get('collation'),
                    "size_bytes": stats.get('size_bytes', 0),
                    "table_count": stats.get('table_count', 0),
                    "connector_type": "mysql"
                }
                
                database = DatabaseMetadata(
                    name=self._sanitize_identifier(db_name),
                    qualified_name=self._build_qualified_name("mysql", self.host, db_name),
                    properties=self._convert_decimals(properties)
                )
                
                return database
        
        except Exception as e:
            logger.error(
                "mysql_extract_single_database_failed",
                data_source_id=str(self.data_source_id),
                database_name=database_name,
                error=str(e)
            )
            return None

    async def _extract_databases(self, connection) -> List[DatabaseMetadata]:
        """Extract database information"""
        databases = []
        
        try:
            async with connection.cursor() as cursor:
                # Get all databases, excluding system databases
                await cursor.execute("""
                    SELECT 
                        SCHEMA_NAME as database_name,
                        SCHEMA_NAME as schema_name,
                        DEFAULT_CHARACTER_SET_NAME as charset,
                        DEFAULT_COLLATION_NAME as collation
                    FROM information_schema.SCHEMATA
                    WHERE SCHEMA_NAME NOT IN ('information_schema', 'mysql', 'performance_schema', 'sys')
                    ORDER BY SCHEMA_NAME
                """)
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    db_name = row['database_name']
                    
                    # Get database size and table count
                    await cursor.execute("""
                        SELECT 
                            IFNULL(SUM(DATA_LENGTH + INDEX_LENGTH), 0) as size_bytes,
                            COUNT(*) as table_count
                        FROM information_schema.TABLES
                        WHERE TABLE_SCHEMA = %s
                    """, (db_name,))
                    
                    stats = await cursor.fetchone()
                    
                    properties = {
                        "host": self.host,
                        "port": self.port,
                        "charset": row.get('charset'),
                        "collation": row.get('collation'),
                        "size_bytes": stats.get('size_bytes', 0),
                        "table_count": stats.get('table_count', 0),
                        "connector_type": "mysql"
                    }
                    
                    database = DatabaseMetadata(
                        name=self._sanitize_identifier(db_name),
                        qualified_name=self._build_qualified_name("mysql", self.host, db_name),
                        properties=self._convert_decimals(properties)
                    )
                    
                    databases.append(database)
        
        except Exception as e:
            logger.error(
                "mysql_extract_databases_failed",
                data_source_id=str(self.data_source_id),
                error=str(e)
            )
            raise
        
        return databases
    
    async def _extract_tables(self, connection, database_name: str) -> List[TableMetadata]:
        """Extract table information for a specific database"""
        tables = []
        
        try:
            async with connection.cursor() as cursor:
                # Get table information
                await cursor.execute("""
                    SELECT 
                        TABLE_NAME,
                        TABLE_TYPE,
                        TABLE_COMMENT,
                        TABLE_ROWS,
                        DATA_LENGTH + INDEX_LENGTH as size_bytes,
                        ENGINE,
                        TABLE_COLLATION,
                        CREATE_TIME,
                        UPDATE_TIME
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = %s
                    ORDER BY TABLE_NAME
                """, (database_name,))
                
                rows = await cursor.fetchall()
                
                for row in rows:
                    table_name = self._sanitize_identifier(row['TABLE_NAME'])
                    table_type = row['TABLE_TYPE']
                    
                    # Map MySQL table types to our asset types
                    if table_type == 'VIEW':
                        asset_type = 'VIEW'
                    else:
                        asset_type = 'TABLE'
                    
                    properties = {
                        "engine": row.get('ENGINE'),
                        "collation": row.get('TABLE_COLLATION'),
                        "created_at": row.get('CREATE_TIME').isoformat() if row.get('CREATE_TIME') else None,
                        "updated_at": row.get('UPDATE_TIME').isoformat() if row.get('UPDATE_TIME') else None,
                        "connector_type": "mysql"
                    }
                    
                    table = TableMetadata(
                        name=table_name,
                        qualified_name=self._build_qualified_name("mysql", self.host, database_name, table_name),
                        schema_name=database_name,
                        database_name=database_name,
                        table_type=asset_type,
                        properties=self._convert_decimals(properties),
                        comment=row.get('TABLE_COMMENT') if row.get('TABLE_COMMENT') else None,
                        row_count=self._convert_decimals(row.get('TABLE_ROWS')) if row.get('TABLE_ROWS') is not None else None,
                        size_bytes=self._convert_decimals(row.get('size_bytes')) if row.get('size_bytes') is not None else None
                    )
                    
                    tables.append(table)
        
        except Exception as e:
            logger.error(
                "mysql_extract_tables_failed",
                data_source_id=str(self.data_source_id),
                database=database_name,
                error=str(e)
            )
            raise
        
        return tables
    
    async def _extract_columns(self, connection, database_name: str, table_name: str) -> List[ColumnMetadata]:
        """Extract column information for a specific table"""
        columns = []
        
        try:
            async with connection.cursor() as cursor:
                # Get column information
                await cursor.execute("""
                    SELECT 
                        COLUMN_NAME,
                        ORDINAL_POSITION,
                        COLUMN_DEFAULT,
                        IS_NULLABLE,
                        DATA_TYPE,
                        COLUMN_TYPE,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE,
                        COLUMN_COMMENT,
                        COLUMN_KEY,
                        EXTRA
                    FROM information_schema.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                """, (database_name, table_name))
                
                rows = await cursor.fetchall()
                
                # Get primary key information
                await cursor.execute("""
                    SELECT COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_NAME = %s 
                    AND CONSTRAINT_NAME = 'PRIMARY'
                """, (database_name, table_name))
                
                primary_key_rows = await cursor.fetchall()
                primary_keys = {row['COLUMN_NAME'] for row in primary_key_rows}
                
                # Get foreign key information
                await cursor.execute("""
                    SELECT 
                        COLUMN_NAME,
                        REFERENCED_TABLE_SCHEMA,
                        REFERENCED_TABLE_NAME,
                        REFERENCED_COLUMN_NAME
                    FROM information_schema.KEY_COLUMN_USAGE
                    WHERE TABLE_SCHEMA = %s 
                    AND TABLE_NAME = %s 
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """, (database_name, table_name))
                
                foreign_key_rows = await cursor.fetchall()
                foreign_keys = {}
                for fk_row in foreign_key_rows:
                    col_name = fk_row['COLUMN_NAME']
                    ref_table = fk_row['REFERENCED_TABLE_NAME']
                    ref_schema = fk_row['REFERENCED_TABLE_SCHEMA']
                    ref_column = fk_row['REFERENCED_COLUMN_NAME']
                    foreign_keys[col_name] = f"{ref_schema}.{ref_table}.{ref_column}"
                
                # Process each column
                for row in rows:
                    column_name = self._sanitize_identifier(row['COLUMN_NAME'])
                    data_type = self._normalize_data_type(row['COLUMN_TYPE'])
                    is_nullable = row['IS_NULLABLE'] == 'YES'
                    is_primary_key = column_name in primary_keys
                    is_foreign_key = column_name in foreign_keys
                    
                    properties = {
                        "raw_data_type": row.get('DATA_TYPE'),
                        "column_type": row.get('COLUMN_TYPE'),
                        "character_maximum_length": row.get('CHARACTER_MAXIMUM_LENGTH'),
                        "numeric_precision": row.get('NUMERIC_PRECISION'),
                        "numeric_scale": row.get('NUMERIC_SCALE'),
                        "column_key": row.get('COLUMN_KEY'),
                        "extra": row.get('EXTRA'),
                        "connector_type": "mysql"
                    }
                    
                    column = ColumnMetadata(
                        name=column_name,
                        table_qualified_name=self._build_qualified_name("mysql", self.host, database_name, table_name),
                        ordinal_position=self._convert_decimals(row['ORDINAL_POSITION']),
                        data_type=data_type,
                        is_nullable=is_nullable,
                        default_value=row.get('COLUMN_DEFAULT'),
                        comment=row.get('COLUMN_COMMENT') if row.get('COLUMN_COMMENT') else None,
                        is_primary_key=is_primary_key,
                        is_foreign_key=is_foreign_key,
                        foreign_key_reference=foreign_keys.get(column_name),
                        properties=self._convert_decimals(properties)
                    )
                    
                    columns.append(column)
        
        except Exception as e:
            logger.error(
                "mysql_extract_columns_failed",
                data_source_id=str(self.data_source_id),
                database=database_name,
                table=table_name,
                error=str(e)
            )
            raise
        
        return columns