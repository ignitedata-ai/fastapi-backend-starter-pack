"""
Seed data for connector definitions
"""
from typing import List, Dict, Any
from core.models.enums import ConnectorKind

def get_connector_definitions() -> List[Dict[str, Any]]:
    """Returns a list of connector definitions to seed the database"""
    return [
        {
            "key": "postgresql",
            "version": "1.0.0",
            "kind": ConnectorKind.JDBC,
            "display_name": "PostgreSQL",
            "capabilities": {
                "catalog_extraction": True,
                "data_profiling": True,
                "lineage_extraction": True,
                "sample_data": True,
                "test_connection": True,
                "incremental_extraction": True,
                "sql_queries": True,
                "schema_evolution": True
            },
            "connection_schema": {
                "type": "object",
                "required": ["host", "port", "database"],
                "properties": {
                    "host": {
                        "type": "string",
                        "title": "Host",
                        "description": "PostgreSQL server hostname or IP address"
                    },
                    "port": {
                        "type": "integer",
                        "title": "Port",
                        "default": 5432,
                        "description": "PostgreSQL server port"
                    },
                    "database": {
                        "type": "string",
                        "title": "Database",
                        "description": "Database name to connect to"
                    },
                    "schema": {
                        "type": "string",
                        "title": "Schema",
                        "default": "public",
                        "description": "Schema name (default: public)"
                    },
                    "ssl_mode": {
                        "type": "string",
                        "title": "SSL Mode",
                        "enum": ["disable", "require", "verify-ca", "verify-full"],
                        "default": "prefer",
                        "description": "SSL connection mode"
                    },
                    "connect_timeout": {
                        "type": "integer",
                        "title": "Connection Timeout",
                        "default": 10,
                        "description": "Connection timeout in seconds"
                    }
                }
            },
            "secret_schema": {
                "type": "object",
                "required": ["username", "password"],
                "properties": {
                    "username": {
                        "type": "string",
                        "title": "Username",
                        "description": "PostgreSQL username"
                    },
                    "password": {
                        "type": "string",
                        "title": "Password",
                        "format": "password",
                        "description": "PostgreSQL password"
                    }
                }
            },
            "image_ref": "ignite/connector-postgresql:1.0.0",
            "docs_url": "https://docs.ignite.com/connectors/postgresql",
            "is_enabled": True
        },
        {
            "key": "mysql",
            "version": "1.0.0",
            "kind": ConnectorKind.JDBC,
            "display_name": "MySQL",
            "capabilities": {
                "catalog_extraction": True,
                "data_profiling": True,
                "lineage_extraction": True,
                "sample_data": True,
                "test_connection": True,
                "incremental_extraction": True,
                "sql_queries": True,
                "schema_evolution": True
            },
            "connection_schema": {
                "type": "object",
                "required": ["host", "port", "database"],
                "properties": {
                    "host": {
                        "type": "string",
                        "title": "Host",
                        "description": "MySQL server hostname or IP address"
                    },
                    "port": {
                        "type": "integer",
                        "title": "Port",
                        "default": 3306,
                        "description": "MySQL server port"
                    },
                    "database": {
                        "type": "string",
                        "title": "Database",
                        "description": "Database name to connect to"
                    },
                    "charset": {
                        "type": "string",
                        "title": "Character Set",
                        "default": "utf8mb4",
                        "description": "Character set for the connection"
                    },
                    "use_ssl": {
                        "type": "boolean",
                        "title": "Use SSL",
                        "default": False,
                        "description": "Use SSL for connection"
                    },
                    "connect_timeout": {
                        "type": "integer",
                        "title": "Connection Timeout",
                        "default": 10,
                        "description": "Connection timeout in seconds"
                    }
                }
            },
            "secret_schema": {
                "type": "object",
                "required": ["username", "password"],
                "properties": {
                    "username": {
                        "type": "string",
                        "title": "Username",
                        "description": "MySQL username"
                    },
                    "password": {
                        "type": "string",
                        "title": "Password",
                        "format": "password",
                        "description": "MySQL password"
                    }
                }
            },
            "image_ref": "ignite/connector-mysql:1.0.0",
            "docs_url": "https://docs.ignite.com/connectors/mysql",
            "is_enabled": True
        },
        {
            "key": "databricks",
            "version": "1.0.0",
            "kind": ConnectorKind.WAREHOUSE,
            "display_name": "Databricks",
            "capabilities": {
                "catalog_extraction": True,
                "data_profiling": True,
                "lineage_extraction": True,
                "sample_data": True,
                "test_connection": True,
                "incremental_extraction": True,
                "sql_queries": True,
                "unity_catalog": True,
                "delta_tables": True,
                "notebook_lineage": True
            },
            "connection_schema": {
                "type": "object",
                "required": ["workspace_url", "http_path"],
                "properties": {
                    "workspace_url": {
                        "type": "string",
                        "title": "Workspace URL",
                        "description": "Databricks workspace URL (e.g., https://xxx.cloud.databricks.com)",
                        "pattern": "^https://.*\\.databricks\\.com$"
                    },
                    "http_path": {
                        "type": "string",
                        "title": "HTTP Path",
                        "description": "SQL warehouse HTTP path"
                    },
                    "catalog": {
                        "type": "string",
                        "title": "Catalog",
                        "default": "main",
                        "description": "Unity Catalog name (default: main)"
                    },
                    "schema": {
                        "type": "string",
                        "title": "Schema",
                        "default": "default",
                        "description": "Schema name within the catalog"
                    },
                    "use_unity_catalog": {
                        "type": "boolean",
                        "title": "Use Unity Catalog",
                        "default": True,
                        "description": "Enable Unity Catalog features"
                    },
                    "extract_notebooks": {
                        "type": "boolean",
                        "title": "Extract Notebooks",
                        "default": False,
                        "description": "Extract notebook metadata and lineage"
                    },
                    "extract_jobs": {
                        "type": "boolean",
                        "title": "Extract Jobs",
                        "default": True,
                        "description": "Extract Databricks jobs metadata"
                    }
                }
            },
            "secret_schema": {
                "type": "object",
                "required": ["access_token"],
                "properties": {
                    "access_token": {
                        "type": "string",
                        "title": "Access Token",
                        "format": "password",
                        "description": "Databricks personal access token"
                    }
                }
            },
            "image_ref": "ignite/connector-databricks:1.0.0",
            "docs_url": "https://docs.ignite.com/connectors/databricks",
            "is_enabled": True
        },
        {
            "key": "sqlite",
            "version": "1.0.0",
            "kind": ConnectorKind.JDBC,
            "display_name": "SQLite",
            "capabilities": {
                "catalog_extraction": True,
                "data_profiling": True,
                "sample_data": True,
                "test_connection": True,
                "sql_queries": True
            },
            "connection_schema": {
                "type": "object",
                "required": ["database_path"],
                "properties": {
                    "database_path": {
                        "type": "string",
                        "title": "Database Path",
                        "description": "Path to SQLite database file"
                    },
                    "read_only": {
                        "type": "boolean",
                        "title": "Read Only",
                        "default": True,
                        "description": "Open database in read-only mode"
                    },
                    "timeout": {
                        "type": "number",
                        "title": "Timeout",
                        "default": 5.0,
                        "description": "Database lock timeout in seconds"
                    }
                }
            },
            "secret_schema": {
                "type": "object",
                "properties": {}
            },
            "image_ref": "ignite/connector-sqlite:1.0.0",
            "docs_url": "https://docs.ignite.com/connectors/sqlite",
            "is_enabled": True
        },
        {
            "key": "snowflake",
            "version": "1.0.0",
            "kind": ConnectorKind.WAREHOUSE,
            "display_name": "Snowflake",
            "capabilities": {
                "catalog_extraction": True,
                "data_profiling": True,
                "lineage_extraction": True,
                "sample_data": True,
                "test_connection": True,
                "incremental_extraction": True,
                "sql_queries": True,
                "time_travel": True,
                "data_sharing": True,
                "schema_evolution": True
            },
            "connection_schema": {
                "type": "object",
                "required": ["account", "warehouse", "database"],
                "properties": {
                    "account": {
                        "type": "string",
                        "title": "Account",
                        "description": "Snowflake account identifier (e.g., xy12345.us-east-1)"
                    },
                    "warehouse": {
                        "type": "string",
                        "title": "Warehouse",
                        "description": "Snowflake warehouse name"
                    },
                    "database": {
                        "type": "string",
                        "title": "Database",
                        "description": "Database name to connect to"
                    },
                    "schema": {
                        "type": "string",
                        "title": "Schema",
                        "default": "PUBLIC",
                        "description": "Schema name (default: PUBLIC)"
                    },
                    "role": {
                        "type": "string",
                        "title": "Role",
                        "description": "Snowflake role to use"
                    },
                    "query_tag": {
                        "type": "string",
                        "title": "Query Tag",
                        "description": "Tag to add to queries for tracking"
                    }
                }
            },
            "secret_schema": {
                "type": "object",
                "required": ["username", "password"],
                "properties": {
                    "username": {
                        "type": "string",
                        "title": "Username",
                        "description": "Snowflake username"
                    },
                    "password": {
                        "type": "string",
                        "title": "Password",
                        "format": "password",
                        "description": "Snowflake password"
                    },
                    "private_key": {
                        "type": "string",
                        "title": "Private Key",
                        "format": "textarea",
                        "description": "Private key for key-pair authentication (optional)"
                    },
                    "private_key_passphrase": {
                        "type": "string",
                        "title": "Private Key Passphrase",
                        "format": "password",
                        "description": "Passphrase for the private key (if encrypted)"
                    }
                }
            },
            "image_ref": "ignite/connector-snowflake:1.0.0",
            "docs_url": "https://docs.ignite.com/connectors/snowflake",
            "is_enabled": True
        },
        {
            "key": "bigquery",
            "version": "1.0.0",
            "kind": ConnectorKind.WAREHOUSE,
            "display_name": "BigQuery",
            "capabilities": {
                "catalog_extraction": True,
                "data_profiling": True,
                "lineage_extraction": True,
                "sample_data": True,
                "test_connection": True,
                "incremental_extraction": True,
                "sql_queries": True,
                "partitioned_tables": True,
                "materialized_views": True
            },
            "connection_schema": {
                "type": "object",
                "required": ["project_id"],
                "properties": {
                    "project_id": {
                        "type": "string",
                        "title": "Project ID",
                        "description": "Google Cloud project ID"
                    },
                    "dataset_id": {
                        "type": "string",
                        "title": "Dataset ID",
                        "description": "Default dataset ID (optional)"
                    },
                    "location": {
                        "type": "string",
                        "title": "Location",
                        "default": "US",
                        "description": "BigQuery dataset location"
                    },
                    "use_legacy_sql": {
                        "type": "boolean",
                        "title": "Use Legacy SQL",
                        "default": False,
                        "description": "Use legacy SQL syntax"
                    }
                }
            },
            "secret_schema": {
                "type": "object",
                "required": ["service_account_json"],
                "properties": {
                    "service_account_json": {
                        "type": "string",
                        "title": "Service Account JSON",
                        "format": "textarea",
                        "description": "Google Cloud service account JSON key"
                    }
                }
            },
            "image_ref": "ignite/connector-bigquery:1.0.0",
            "docs_url": "https://docs.ignite.com/connectors/bigquery",
            "is_enabled": True
        },
        {
            "key": "redshift",
            "version": "1.0.0",
            "kind": ConnectorKind.WAREHOUSE,
            "display_name": "Amazon Redshift",
            "capabilities": {
                "catalog_extraction": True,
                "data_profiling": True,
                "lineage_extraction": True,
                "sample_data": True,
                "test_connection": True,
                "incremental_extraction": True,
                "sql_queries": True,
                "spectrum_tables": True
            },
            "connection_schema": {
                "type": "object",
                "required": ["host", "port", "database"],
                "properties": {
                    "host": {
                        "type": "string",
                        "title": "Host",
                        "description": "Redshift cluster endpoint"
                    },
                    "port": {
                        "type": "integer",
                        "title": "Port",
                        "default": 5439,
                        "description": "Redshift port"
                    },
                    "database": {
                        "type": "string",
                        "title": "Database",
                        "description": "Database name"
                    },
                    "schema": {
                        "type": "string",
                        "title": "Schema",
                        "default": "public",
                        "description": "Schema name (default: public)"
                    },
                    "ssl_mode": {
                        "type": "string",
                        "title": "SSL Mode",
                        "enum": ["disable", "require", "verify-ca", "verify-full"],
                        "default": "require",
                        "description": "SSL connection mode"
                    }
                }
            },
            "secret_schema": {
                "type": "object",
                "required": ["username", "password"],
                "properties": {
                    "username": {
                        "type": "string",
                        "title": "Username",
                        "description": "Redshift username"
                    },
                    "password": {
                        "type": "string",
                        "title": "Password",
                        "format": "password",
                        "description": "Redshift password"
                    }
                }
            },
            "image_ref": "ignite/connector-redshift:1.0.0",
            "docs_url": "https://docs.ignite.com/connectors/redshift",
            "is_enabled": True
        }
    ]