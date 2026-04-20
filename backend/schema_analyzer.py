"""
Schema Analyzer - Cache and retrieve database metadata without LLM.
Stores table structures, columns, relationships, and types for fast access.
"""

import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

class SchemaCache:
    """In-memory cache for database schemas."""
    
    def __init__(self):
        self.cache: Dict[str, Any] = {}
        self.last_updated: Dict[str, datetime] = {}
        self.ttl_seconds = 3600  # 1 hour cache
    
    def get_schema(self, database: str) -> Optional[Dict]:
        """Get cached schema for a database."""
        if database in self.cache:
            return self.cache[database]
        return None
    
    def set_schema(self, database: str, schema: Dict):
        """Cache schema for a database."""
        self.cache[database] = schema
        self.last_updated[database] = datetime.now()
        logger.info(f"Cached schema for {database}")
    
    def clear_schema(self, database: str = None):
        """Clear cached schema."""
        if database:
            self.cache.pop(database, None)
            self.last_updated.pop(database, None)
        else:
            self.cache.clear()
            self.last_updated.clear()


class SchemaAnalyzer:
    """Analyze schemas and extract metadata for query generation."""
    
    def __init__(self):
        self.cache = SchemaCache()
    
    def parse_schema_info(self, raw_schema: str) -> Dict[str, Any]:
        """
        Parse raw schema info from get_database_info (JSON format) and organize by database.
        
        Returns:
        {
            'postgres': {
                'tables': {
                    'customers': {
                        'columns': [
                            {'name': 'customer_id', 'type': 'INTEGER', 'key': 'PRIMARY'},
                            {'name': 'firstname', 'type': 'VARCHAR', 'key': None},
                        ],
                        'sample': [row1, row2, ...],
                    },
                    ...
                }
            },
            'mongodb': {...},
            'sqlserver': {...},
        }
        """
        import json
        
        parsed = {
            'postgres': {'tables': {}, 'database': 'postgres'},
            'sqlserver': {'tables': {}, 'database': 'sqlserver'},
            'mongodb': {'tables': {}, 'database': 'mongodb'},
        }
        
        try:
            # Parse JSON schema from mcp_server
            schema_json = json.loads(raw_schema)
        except json.JSONDecodeError:
            logger.warning("Failed to parse schema as JSON, attempting text parsing")
            return parsed
        
        # Map database identifiers to normalized names
        db_mapping = {
            'InventoryDB_SQL_Server': 'sqlserver',
            'SalesDB_PostgreSQL': 'postgres',
            'CustomerDB_MongoDB': 'mongodb',
        }
        
        # Process each database in the schema
        for db_identifier, db_data in schema_json.items():
            if db_identifier not in db_mapping:
                continue
            
            db_name = db_mapping[db_identifier]
            schema_info = db_data.get('schema', {})
            
            print(f"\n[SCHEMA PARSE] Processing {db_identifier}: found {len(schema_info)} tables")
            logger.info(f"Processing {db_identifier}: found {len(schema_info)} tables")
            
            # Process tables/collections
            for table_name, table_info in schema_info.items():
                # Handle both SQL (columns) and MongoDB (fields) formats
                if 'columns' in table_info:
                    columns_raw = table_info.get('columns', [])
                    columns = []
                    
                    for col_def in columns_raw:
                        # Column format: "Column_Name (type)" or "Column_Name (PK) (type)"
                        # Example: "Product_ID (PK) (int)"
                        col_name = col_def.split('(')[0].strip()
                        
                        # Check if it's a primary key
                        is_pk = 'PK' in col_def
                        is_fk = 'FK' in col_def
                        
                        # Extract type
                        col_type = 'VARCHAR'
                        if '(' in col_def:
                            parts = col_def.split('(')
                            for part in parts:
                                cleaned = part.replace(')', '').strip()
                                if cleaned and cleaned not in ['PK', 'FK'] and not cleaned.startswith('Column'):
                                    col_type = cleaned
                                    break
                        
                        columns.append({
                            'name': col_name,
                            'type': col_type,
                            'key': 'PRIMARY' if is_pk else ('FOREIGN' if is_fk else None),
                        })
                
                elif 'fields' in table_info:
                    # MongoDB format: 'fields' is a dict of field_name -> type
                    fields_raw = table_info.get('fields', {})
                    columns = []
                    
                    for field_name, field_type in fields_raw.items():
                        columns.append({
                            'name': field_name,
                            'type': str(field_type) if field_type else 'str',
                            'key': None,
                        })
                
                else:
                    columns = []
                
                # Store parsed table info
                table_key = table_name.lower()
                parsed[db_name]['tables'][table_key] = {
                    'columns': columns,
                    'sample': db_data.get('samples', {}).get(table_name, []),
                    'record_count': table_info.get('record_count', 0),
                }
                
                print(f"  - {table_name} -> {table_key}: {len(columns)} columns")
                logger.info(f"  - {table_name} -> {table_key}: {len(columns)} columns")
                if columns:
                    col_names = [c['name'] for c in columns[:3]]
                    print(f"    First 3 columns: {col_names}")
                    logger.info(f"    Columns: {col_names}...")
        
        return parsed
    
    def get_table_columns(self, database: str, table_name: str) -> List[Dict]:
        """Get columns for a specific table."""
        schema = self.cache.get_schema(database)
        if schema and table_name in schema['tables']:
            return schema['tables'][table_name]['columns']
        return []
    
    def get_tables(self, database: str) -> List[str]:
        """Get all table names for a database."""
        schema = self.cache.get_schema(database)
        if schema:
            return list(schema['tables'].keys())
        return []
    
    def find_primary_key(self, database: str, table_name: str) -> Optional[str]:
        """Find primary key column for a table."""
        columns = self.get_table_columns(database, table_name)
        for col in columns:
            if col['key'] == 'PRIMARY':
                return col['name']
        return None
    
    def find_foreign_keys(self, database: str, table_name: str) -> List[Dict]:
        """Find foreign key columns for a table."""
        columns = self.get_table_columns(database, table_name)
        return [col for col in columns if col['key'] == 'FOREIGN']
    
    def get_schema_summary(self, database: str) -> str:
        """Get a concise summary of database schema for quick reference."""
        schema = self.cache.get_schema(database)
        if not schema:
            return f"No cached schema for {database}"
        
        summary = f"\n{database.upper()} SCHEMA:\n"
        summary += "=" * 50 + "\n"
        
        for table_name, table_info in schema['tables'].items():
            cols = ', '.join([f"{c['name']}({c['type']})" for c in table_info['columns']])
            summary += f"  • {table_name}: {cols}\n"
        
        return summary


# Global instance
schema_analyzer = SchemaAnalyzer()
