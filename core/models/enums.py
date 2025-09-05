import enum
import re
from typing import Optional


class ConnectorKind(str, enum.Enum):
    JDBC = "jdbc"
    WAREHOUSE = "warehouse"
    OBJECT_STORE = "object_store"
    UNSTRUCTURED = "unstructured"
    API = "api"
    FILE = "file"  # Add missing FILE enum value

    @classmethod
    def values(cls) -> list[str]:
        """Get all enum values as a list"""
        return [item.value for item in cls]

    @classmethod
    def is_valid(cls, value: str) -> bool:
        """Check if a value is a valid ConnectorKind"""
        return value in cls.values()

    @classmethod
    def get_invalid_value_error_message(cls, invalid_value: str) -> str:
        """Get formatted error message for invalid connector kind"""
        return f"Invalid connector kind: '{invalid_value}'. Supported values: {cls.values()}"


class RunType(str, enum.Enum):
    METADATA = "metadata"
    PROFILE = "profile"
    SAMPLE = "sample"
    LINEAGE = "lineage"
    SYNC = "sync"


class RunStatus(str, enum.Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    COMPLETED = "completed"  # Add COMPLETED which tests expect
    FAILED = "failed"
    CANCELLED = "cancelled"
    PARTIAL = "partial"


class ConnectionStatus(str, enum.Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PENDING = "pending"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ERROR = "error"  # Add ERROR which tests expect
    TESTING = "testing"  # Add TESTING which tests expect


class AssetType(str, enum.Enum):
    DATABASE = "database"
    SCHEMA = "schema"
    TABLE = "table"
    VIEW = "view"
    COLUMN = "column"
    BUCKET = "bucket"
    PREFIX = "prefix"
    OBJECT = "object"
    DOCUMENT_SET = "document_set"
    DOCUMENT = "document"


class LineageOp(str, enum.Enum):
    READ = "read"
    WRITE = "write"
    TRANSFORM = "transform"
    COPY = "copy"
    LOAD = "load"
    EXTRACT = "extract"
    JOIN = "join"
    AGGREGATE = "aggregate"
    DELETE = "delete"


# Database Error Constants and Utilities
class DatabaseErrorPatterns:
    """Constants for database error pattern matching"""

    # PostgreSQL/AsyncPG enum validation error patterns
    INVALID_ENUM_PATTERN = r'invalid input value for enum (\w+): "([^"]+)"'
    CONNECTORKIND_ENUM_PATTERN = r'invalid input value for enum connectorkind: "([^"]+)"'

    @staticmethod
    def extract_invalid_enum_value(error_message: str, enum_name: str = "connectorkind") -> Optional[str]:
        """Extract invalid enum value from database error message"""
        pattern = rf'invalid input value for enum {enum_name}: "([^"]+)"'
        match = re.search(pattern, error_message)
        return match.group(1) if match else None

    @staticmethod
    def is_enum_validation_error(error_message: str, enum_name: str = "connectorkind") -> bool:
        """Check if error message is an enum validation error"""
        print(f"Checking if error message is enum validation error: {error_message}")
        print(f"Using enum_name: {enum_name}")
        pattern = f"invalid input value for enum {enum_name}"
        return pattern in error_message.lower()

    @staticmethod
    def create_connector_kind_error_details(invalid_value: str) -> dict:
        """Create structured error details for invalid ConnectorKind"""
        return {"invalid_value": invalid_value, "field": "kind", "supported_values": ConnectorKind.values()}
