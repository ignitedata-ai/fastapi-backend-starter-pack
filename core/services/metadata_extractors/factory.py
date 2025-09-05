"""
Metadata extractor factory for different connector types
"""
from typing import Dict, Any, Optional
from uuid import UUID

from core.logging import CentralizedLogger
from .base import BaseMetadataExtractor

logger = CentralizedLogger(__name__)

# Import extractors with error handling
_extractors = {}

# MySQL/MariaDB extractor
try:
    from .mysql_extractor import MySQLMetadataExtractor
    _extractors.update({
        "mysql": MySQLMetadataExtractor,
        "mariadb": MySQLMetadataExtractor,  # MariaDB uses same extractor as MySQL
    })
except ImportError as e:
    logger.warning(f"MySQL extractor not available: {e}")

# Databricks extractor  
try:
    from .databricks_extractor import DatabricksMetadataExtractor
    _extractors["databricks"] = DatabricksMetadataExtractor
except ImportError as e:
    logger.warning(f"Databricks extractor not available: {e}")


class MetadataExtractorFactory:
    """Factory for creating metadata extractors based on connector type"""
    
    @classmethod
    def create_extractor(
        cls,
        connector_key: str,
        data_source_id: UUID,
        tenant_id: UUID,
        config: Dict[str, Any],
        credentials: Dict[str, Any]
    ) -> Optional[BaseMetadataExtractor]:
        """
        Create a metadata extractor for the specified connector
        
        Args:
            connector_key: Type of connector (mysql, postgresql, etc.)
            data_source_id: UUID of the data source
            tenant_id: UUID of the tenant
            config: Non-sensitive configuration
            credentials: Sensitive credentials
            
        Returns:
            Metadata extractor instance or None if not supported
        """
        extractor_class = _extractors.get(connector_key.lower())
        
        if not extractor_class:
            logger.warning(
                "metadata_extractor_not_found",
                connector_key=connector_key,
                supported_connectors=list(_extractors.keys())
            )
            return None
        
        try:
            logger.debug(
                "creating_extractor_instance",
                connector_key=connector_key,
                extractor_class=extractor_class.__name__,
                data_source_id=str(data_source_id)
            )
            
            return extractor_class(
                data_source_id=data_source_id,
                tenant_id=tenant_id,
                config=config,
                credentials=credentials
            )
        except ImportError as e:
            # Handle dependency issues specifically
            logger.error(
                "metadata_extractor_dependency_missing",
                connector_key=connector_key,
                data_source_id=str(data_source_id),
                extractor_class=extractor_class.__name__,
                dependency_error=str(e),
                python_executable=__import__('sys').executable
            )
            return None
        except Exception as e:
            logger.error(
                "metadata_extractor_creation_failed",
                connector_key=connector_key,
                data_source_id=str(data_source_id),
                extractor_class=extractor_class.__name__,
                error=str(e),
                error_type=type(e).__name__
            )
            return None
    
    @classmethod
    def get_supported_connectors(cls) -> list[str]:
        """Get list of supported connector types"""
        return list(_extractors.keys())
    
    @classmethod
    def register_extractor(cls, connector_key: str, extractor_class: type):
        """
        Register a new metadata extractor
        
        Args:
            connector_key: Connector type identifier
            extractor_class: Extractor class that inherits from BaseMetadataExtractor
        """
        if not issubclass(extractor_class, BaseMetadataExtractor):
            raise ValueError("Extractor class must inherit from BaseMetadataExtractor")
        
        _extractors[connector_key.lower()] = extractor_class
        logger.info(
            "metadata_extractor_registered",
            connector_key=connector_key,
            extractor_class=extractor_class.__name__
        )