"""
Main script to seed the database with initial data
"""
import asyncio
import logging
from typing import Optional
import sys
from pathlib import Path

# Add the project root to Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from core.database.session import initialize_database, get_session_manager
from core.models.connector import ConnectorDefinition
from scripts.connector_definitions import get_connector_definitions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def seed_connector_definitions(session: AsyncSession, force: bool = False) -> None:
    """
    Seed connector definitions to the database
    
    Args:
        session: Database session
        force: If True, will update existing definitions
    """
    definitions = get_connector_definitions()
    
    for definition_data in definitions:
        # Check if connector already exists
        stmt = select(ConnectorDefinition).where(
            ConnectorDefinition.key == definition_data["key"],
            ConnectorDefinition.version == definition_data["version"]
        )
        existing = await session.execute(stmt)
        existing_connector = existing.scalar_one_or_none()
        
        if existing_connector:
            if force:
                # Update existing connector
                for key, value in definition_data.items():
                    setattr(existing_connector, key, value)
                logger.info(f"Updated connector: {definition_data['key']} v{definition_data['version']}")
            else:
                logger.info(f"Skipping existing connector: {definition_data['key']} v{definition_data['version']}")
        else:
            # Create new connector
            connector = ConnectorDefinition(**definition_data)
            session.add(connector)
            logger.info(f"Added connector: {definition_data['key']} v{definition_data['version']}")
    
    await session.commit()
    logger.info("Connector definitions seeding completed")


async def run_all_seeds(force: bool = False) -> None:
    """
    Run all seed scripts
    
    Args:
        force: If True, will update existing data
    """
    session_manager = get_session_manager()
    async with session_manager.get_session() as session:
        try:
            logger.info("Starting database seeding...")
            
            # Seed connector definitions
            await seed_connector_definitions(session, force=force)
            
            # Add more seed functions here as needed
            # await seed_sample_data_sources(session, force=force)
            # await seed_sample_assets(session, force=force)
            
            logger.info("Database seeding completed successfully!")
            
        except Exception as e:
            logger.error(f"Error during seeding: {e}")
            await session.rollback()
            raise


async def main():
    """Main entry point for the seed script"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Seed the database with initial data")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force update existing data"
    )
    parser.add_argument(
        "--connectors-only",
        action="store_true",
        help="Only seed connector definitions"
    )
    
    args = parser.parse_args()
    
    # Initialize the database session manager
    session_manager = initialize_database()
    await session_manager.initialize()
    
    try:
        if args.connectors_only:
            async with session_manager.get_session() as session:
                await seed_connector_definitions(session, force=args.force)
        else:
            await run_all_seeds(force=args.force)
    finally:
        await session_manager.close()


if __name__ == "__main__":
    asyncio.run(main())