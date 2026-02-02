"""
Async loaders for AsyncSelect component.

Provides factory functions to create async loaders that work with AsyncDatabase.
These loaders are designed to be passed to AsyncSelect's loader parameter.
"""

from typing import Any, Dict, List, Optional, Sequence, Tuple
from desktop_app.database_async import AsyncDatabase


class AsyncSelectLoaders:
    """Factory for creating async loaders compatible with AsyncSelect."""
    
    @staticmethod
    async def entities_loader(
        db_async: AsyncDatabase,
        search: str = "",
        offset: int = 0,
        page_size: int = 50,
        advanced: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Async loader for entities (clientes/proveedores).
        Returns (items, has_more) tuple.
        """
        try:
            items = await db_async.fetch_entities(
                search=search or None,
                advanced=advanced,
                limit=page_size + 1,  # +1 to detect if there's more
                offset=offset,
            )
            
            # Check if there are more items
            has_more = len(items) > page_size
            if has_more:
                items = items[:page_size]
            
            # Convert to option format: {value, label}
            options = [
                {"value": item["id"], "label": item.get("nombre_completo", "")}
                for item in items
            ]
            
            return options, has_more
        except Exception as e:
            import logging
            logging.error(f"Error loading entities: {e}")
            return [], False
    
    @staticmethod
    async def documentos_loader(
        db_async: AsyncDatabase,
        search: str = "",
        offset: int = 0,
        page_size: int = 50,
        advanced: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Async loader for documentos (invoices/remitos).
        Returns (items, has_more) tuple.
        """
        try:
            items = await db_async.fetch_documentos_resumen(
                search=search or None,
                advanced=advanced,
                limit=page_size + 1,
                offset=offset,
            )
            
            has_more = len(items) > page_size
            if has_more:
                items = items[:page_size]
            
            options = [
                {
                    "value": item["id"],
                    "label": f"{item.get('numero', '')} - {item.get('entidad', '')}"
                }
                for item in items
            ]
            
            return options, has_more
        except Exception as e:
            import logging
            logging.error(f"Error loading documentos: {e}")
            return [], False
    
    @staticmethod
    async def pagos_loader(
        db_async: AsyncDatabase,
        search: str = "",
        offset: int = 0,
        page_size: int = 50,
        advanced: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Async loader for pagos (payments).
        Returns (items, has_more) tuple.
        """
        try:
            items = await db_async.fetch_pagos(
                search=search or None,
                advanced=advanced,
                limit=page_size + 1,
                offset=offset,
            )
            
            has_more = len(items) > page_size
            if has_more:
                items = items[:page_size]
            
            options = [
                {
                    "value": item["id"],
                    "label": f"{item.get('monto', '')} - {item.get('forma', '')}"
                }
                for item in items
            ]
            
            return options, has_more
        except Exception as e:
            import logging
            logging.error(f"Error loading pagos: {e}")
            return [], False
    
    @staticmethod
    async def articulos_loader(
        db_async: AsyncDatabase,
        search: str = "",
        offset: int = 0,
        page_size: int = 50,
        advanced: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Tuple[List[Dict[str, Any]], bool]:
        """
        Async loader for articulos (inventory items).
        Returns (items, has_more) tuple.
        """
        try:
            items = await db_async.fetch_articulos(
                search=search or None,
                advanced=advanced,
                limit=page_size + 1,
                offset=offset,
            )
            
            has_more = len(items) > page_size
            if has_more:
                items = items[:page_size]
            
            options = [
                {
                    "value": item["id"],
                    "label": f"{item.get('codigo', '')} - {item.get('descripcion', '')}"
                }
                for item in items
            ]
            
            return options, has_more
        except Exception as e:
            import logging
            logging.error(f"Error loading articulos: {e}")
            return [], False
    
    @staticmethod
    def create_generic_loader(
        db_async: AsyncDatabase,
        fetch_method_name: str,
        label_fields: Optional[List[str]] = None,
        value_field: str = "id",
    ):
        """
        Create a generic async loader for custom table operations.
        
        Args:
            db_async: AsyncDatabase instance
            fetch_method_name: Name of the async method in AsyncDatabase to call
            label_fields: Fields to concatenate for label (default: ["nombre"])
            value_field: Field to use as value (default: "id")
        
        Returns:
            Async loader function compatible with AsyncSelect
        """
        if label_fields is None:
            label_fields = ["nombre"]
        
        async def generic_loader(
            search: str = "",
            offset: int = 0,
            page_size: int = 50,
            **kwargs
        ) -> Tuple[List[Dict[str, Any]], bool]:
            try:
                fetch_method = getattr(db_async, fetch_method_name, None)
                if not fetch_method:
                    raise AttributeError(
                        f"AsyncDatabase has no method {fetch_method_name}"
                    )
                
                items = await fetch_method(
                    search=search or None,
                    limit=page_size + 1,
                    offset=offset,
                )
                
                has_more = len(items) > page_size
                if has_more:
                    items = items[:page_size]
                
                options = []
                for item in items:
                    label_parts = [
                        str(item.get(field, ""))
                        for field in label_fields
                        if field in item
                    ]
                    label = " - ".join(filter(None, label_parts)) or str(item.get(value_field, ""))
                    
                    options.append({
                        "value": item[value_field],
                        "label": label,
                    })
                
                return options, has_more
            except Exception as e:
                import logging
                logging.error(f"Error loading {fetch_method_name}: {e}")
                return [], False
        
        return generic_loader
