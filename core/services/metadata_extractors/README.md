# Metadata Extractors

Extensible metadata extraction system for various data source connectors.

## Architecture

### Base Classes
- **`BaseMetadataExtractor`**: Abstract base class defining the contract for all extractors
- **`MetadataExtractorFactory`**: Factory pattern for creating extractors based on connector type
- **`MetadataPersistenceService`**: Handles persisting extracted metadata to the database

### Data Models
- **`DatabaseMetadata`**: Database/catalog information
- **`SchemaMetadata`**: Schema/namespace information  
- **`TableMetadata`**: Table/view details with statistics
- **`ColumnMetadata`**: Column definitions with types and constraints
- **`ExtractionResult`**: Complete result with status and errors

## Supported Connectors

### ✅ MySQL/MariaDB (`mysql_extractor.py`)
- **Features**: Databases, tables, views, columns, constraints, statistics
- **Requirements**: `PyMySQL==1.1.1`
- **Configuration**:
  ```json
  {
    "host": "localhost",
    "port": 3306,
    "database": "mydb",
    "ssl_enabled": false,
    "connection_timeout": 30
  }
  ```
- **Credentials**:
  ```json
  {
    "username": "user",
    "password": "password"
  }
  ```

### ✅ Databricks (`databricks_extractor.py`)
- **Features**: Unity Catalog support (catalogs, schemas, tables, columns)
- **Requirements**: `httpx` (already included)
- **Configuration**:
  ```json
  {
    "workspace_url": "https://dbc-abc123-def4.cloud.databricks.com",
    "catalog": "main",
    "http_path": "/sql/1.0/warehouses/abc123def456"
  }
  ```
- **Credentials**:
  ```json
  {
    "access_token": "dapi123456789abcdef"
  }
  ```

## Adding New Connectors

### 1. Create Extractor Class
```python
class MyConnectorExtractor(BaseMetadataExtractor):
    def get_supported_asset_types(self) -> List[AssetType]:
        return [AssetType.DATABASE, AssetType.TABLE, AssetType.COLUMN]
    
    async def test_connection(self) -> Tuple[bool, Optional[str]]:
        # Test connectivity
        pass
    
    async def extract_metadata(self) -> ExtractionResult:
        # Extract and return metadata
        pass
```

### 2. Register in Factory
```python
# In factory.py
from .my_connector_extractor import MyConnectorExtractor

_extractors = {
    # existing extractors...
    "my_connector": MyConnectorExtractor,
}
```

### 3. Add Dependencies
```toml
# In pyproject.toml
dependencies = [
    # existing dependencies...
    "my-connector-driver==1.0.0",
]
```

## Usage

### Automatic (via API)
Metadata sync is automatically triggered when creating data sources or manually via:
```bash
POST /api/v1/data_sources/{id}/run
{
    "run_type": "metadata"
}
```

### Programmatic
```python
from app.services.metadata_sync import run_metadata_sync

await run_metadata_sync(
    data_source_id="uuid-string",
    tenant_id="uuid-string", 
    run_id="uuid-string"  # optional
)
```

## Error Handling

- **Connection Failures**: Reported in `ConnectorRun` status
- **Partial Extraction**: Some metadata extracted, errors logged
- **Complete Failures**: No metadata extracted, run marked as failed
- **Detailed Logging**: All operations logged with structured data

## Database Schema

Extracted metadata is stored in:
- **`assets`**: Databases, schemas, tables, views
- **`asset_fields`**: Columns/fields with types and constraints
- **`connector_runs`**: Extraction job status and results

## Future Connectors

Ready for implementation:
- PostgreSQL
- Snowflake  
- BigQuery
- S3/Object Storage
- Kafka/Event Streams
- MongoDB