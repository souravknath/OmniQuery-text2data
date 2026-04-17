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
        Parse raw schema info from get_database_info and organize by database.
        
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
        parsed = {
            'postgres': {'tables': {}, 'database': 'postgres'},
            'sqlserver': {'tables': {}, 'database': 'sqlserver'},
            'mongodb': {'tables': {}, 'database': 'mongodb'},
        }
        
        # Split by database sections
        sections = raw_schema.split('\n---\n')
        
        for section in sections:
            if not section.strip():
                continue
            
            lines = section.strip().split('\n')
            current_db = None
            current_table = None
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Detect database
                if 'PostgreSQL' in line or 'Sales' in line:
                    current_db = 'postgres'
                elif 'SQL Server' in line or 'Inventory' in line:
                    current_db = 'sqlserver'
                elif 'MongoDB' in line or 'Customers' in line:
                    current_db = 'mongodb'
                
                # Detect table
                if line.startswith('Table:') or line.startswith('Collection:'):
                    table_name = line.split(':')[1].strip()
                    if current_db:
                        current_table = table_name
                        parsed[current_db]['tables'][table_name] = {
                            'columns': [],
                            'sample': [],
                        }
                
                # Parse columns
                if current_db and current_table and '(' in line and ':' in line:
                    try:
                        col_name = line.split('(')[0].strip()
                        col_type = line.split('(')[1].split(')')[0].strip() if '(' in line else 'VARCHAR'
                        is_key = 'PRIMARY' in line or 'FOREIGN' in line
                        
                        parsed[current_db]['tables'][current_table]['columns'].append({
                            'name': col_name,
                            'type': col_type,
                            'key': 'PRIMARY' if 'PRIMARY' in line else ('FOREIGN' if 'FOREIGN' in line else None),
                        })
                    except Exception as e:
                        logger.debug(f"Could not parse column line: {line}, error: {e}")
        
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
