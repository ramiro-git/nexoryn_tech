"""
Integration utilities for AsyncDatabase and Flet UI.

Provides helpers to initialize and manage async database operations
within a Flet application context.
"""

import logging
from typing import Optional

from desktop_app.database import Database
from desktop_app.database_async import AsyncDatabase

logger = logging.getLogger(__name__)


class AsyncDatabaseManager:
    """
    Manager for async database operations in Flet app.
    
    Handles initialization and cleanup of AsyncDatabase alongside
    the existing synchronous Database instance.
    """
    
    _instance: Optional["AsyncDatabaseManager"] = None
    
    def __init__(self, db: Database):
        """Initialize manager with existing Database instance."""
        self.db = db
        self.db_async = AsyncDatabase(db)
        logger.info("AsyncDatabaseManager initialized")
    
    @classmethod
    def initialize(cls, db: Database) -> "AsyncDatabaseManager":
        """Initialize the manager (singleton pattern)."""
        if cls._instance is None:
            cls._instance = cls(db)
        return cls._instance
    
    @classmethod
    def get_instance(cls) -> Optional["AsyncDatabaseManager"]:
        """Get the current manager instance."""
        return cls._instance
    
    def set_context(self, user_id: Optional[int], ip: Optional[str] = None) -> None:
        """Set user context for both sync and async DB."""
        self.db.set_context(user_id, ip)
        self.db_async.set_context(user_id, ip)
    
    async def cleanup(self) -> None:
        """Cleanup async resources (call on app shutdown)."""
        try:
            await self.db_async.close_async()
            logger.info("AsyncDatabaseManager cleanup completed")
        except Exception as e:
            logger.error(f"Error during AsyncDatabaseManager cleanup: {e}")
    
    def get_async_db(self) -> AsyncDatabase:
        """Get the async database instance."""
        return self.db_async


# Convenience function for creating loaders inline
def create_async_loader(fetch_method, label_extractor=None):
    """
    Create a simple async loader function.
    
    Args:
        fetch_method: Async method from AsyncDatabase (e.g., db_async.fetch_entities)
        label_extractor: Optional function to extract label from item (default: lambda item: str(item.get('nombre', '')))
    
    Returns:
        Async loader function compatible with AsyncSelect
    """
    if label_extractor is None:
        label_extractor = lambda item: str(item.get("nombre", ""))
    
    async def loader(query: str, offset: int, limit: int):
        try:
            items = await fetch_method(
                search=query or None,
                limit=limit + 1,  # +1 to detect if there's more
                offset=offset,
            )
            
            has_more = len(items) > limit
            if has_more:
                items = items[:limit]
            
            options = [
                {
                    "value": item["id"],
                    "label": label_extractor(item),
                }
                for item in items
            ]
            
            return options, has_more
        except Exception as e:
            logger.error(f"Error in async loader: {e}")
            return [], False
    
    return loader
