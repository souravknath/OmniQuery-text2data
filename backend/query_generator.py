"""
Query Generator - Generate SQL queries from metadata without LLM.
Uses schema information to build deterministic, fast SQL queries.
"""

import logging
from typing import Dict, List, Optional, Tuple
from schema_analyzer import schema_analyzer

logger = logging.getLogger(__name__)

class QueryGenerator:
    """Generate SQL queries from schema metadata."""
    
    # Common database-specific joins
    JOINS = {
        'postgres': {
            'customers_to_orders': ('customers.customer_id', 'orders.customer_id'),
            'orders_to_products': ('orders.order_id', 'order_items.order_id', 'order_items.product_id', 'products.product_id'),
        },
        'sqlserver': {
            'inventory_to_sales': ('[Inventory].product_id', '[Sales].product_id'),
        },
        'mongodb': {
            'customers_by_id': 'customer_id',
        }
    }
    
    def __init__(self):
        self.analyzer = schema_analyzer
    
    def generate_simple_select(
        self,
        database: str,
        table: str,
        columns: Optional[List[str]] = None,
        where_clause: Optional[Dict] = None,
        limit: int = 100
    ) -> str:
        """
        Generate a simple SELECT query.
        
        Args:
            database: 'postgres', 'sqlserver', 'mongodb'
            table: table name
            columns: list of column names (None = all)
            where_clause: dict like {'column': 'value', 'operator': '='}
            limit: max rows to return
        
        Returns:
            SQL query string
        """
        # Get available columns
        all_columns = self.analyzer.get_table_columns(database, table)
        col_names = [c['name'] for c in all_columns]
        
        if columns:
            select_cols = ', '.join(columns)
        else:
            select_cols = ', '.join(col_names[:10])  # Limit to 10 cols if not specified
        
        # Build query
        query = f"SELECT {select_cols} FROM {table}"
        
        # Add WHERE clause
        if where_clause:
            conditions = []
            for key, value in where_clause.items():
                if key == 'operator':
                    continue
                operator = where_clause.get('operator', '=')
                if isinstance(value, str):
                    conditions.append(f"{key} {operator} '{value}'")
                else:
                    conditions.append(f"{key} {operator} {value}")
            
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
        
        query += f" LIMIT {limit};"
        
        logger.info(f"Generated simple query:\n{query}")
        return query
    
    def generate_join_query(
        self,
        database: str,
        primary_table: str,
        join_tables: List[str],
        columns: Optional[List[str]] = None,
        where_clause: Optional[Dict] = None,
        limit: int = 100
    ) -> str:
        """
        Generate a JOIN query across multiple tables.
        
        Args:
            database: 'postgres', 'sqlserver', 'mongodb'
            primary_table: main table to query
            join_tables: tables to join with
            columns: list of columns (can be 'table.column')
            where_clause: filters
            limit: max rows
        
        Returns:
            SQL query string
        """
        if not columns:
            columns = [f"{primary_table}.*"]
        
        select_cols = ', '.join(columns)
        query = f"SELECT {select_cols} FROM {primary_table}"
        
        # Add joins (simple heuristic)
        for join_table in join_tables:
            # Try to find a common ID column
            pk = self.analyzer.find_primary_key(database, primary_table)
            fk_candidates = self.analyzer.find_foreign_keys(database, join_table)
            
            if pk and fk_candidates:
                fk = fk_candidates[0]['name']
                query += f"\nJOIN {join_table} ON {primary_table}.{pk} = {join_table}.{fk}"
            else:
                # Fallback: guess common column name
                query += f"\nJOIN {join_table} ON {primary_table}.id = {join_table}.{primary_table}_id"
        
        # Add WHERE
        if where_clause:
            conditions = []
            for key, value in where_clause.items():
                if key == 'operator':
                    continue
                operator = where_clause.get('operator', '=')
                if isinstance(value, str):
                    conditions.append(f"{key} {operator} '{value}'")
                else:
                    conditions.append(f"{key} {operator} {value}")
            
            if conditions:
                query += f" WHERE {' AND '.join(conditions)}"
        
        query += f"\nLIMIT {limit};"
        
        logger.info(f"Generated join query:\n{query}")
        return query
    
    def parse_user_intent(self, user_question: str) -> Dict:
        """
        Parse user question to extract query parameters (without LLM).
        Uses keyword matching to infer intent.
        
        Returns:
            {
                'action': 'select' | 'join' | 'search',
                'database': 'postgres' | 'sqlserver' | 'mongodb',
                'table': table_name,
                'join_tables': [...],
                'filters': {...},
                'columns': [...],
            }
        """
        intent = {
            'action': 'select',
            'database': None,
            'table': None,
            'join_tables': [],
            'filters': {},
            'columns': None,
        }
        
        q = user_question.lower()
        
        # Detect database
        if 'order' in q or 'sales' in q or 'customer' in q:
            intent['database'] = 'postgres'
        elif 'inventory' in q or 'product' in q and intent['database'] != 'postgres':
            intent['database'] = 'sqlserver'
        elif 'mongo' in q or 'document' in q:
            intent['database'] = 'mongodb'
        else:
            intent['database'] = 'postgres'  # default
        
        # Detect action
        if 'join' in q or ('product' in q and 'order' in q):
            intent['action'] = 'join'
        
        # Detect tables
        if 'order' in q:
            intent['table'] = 'orders'
        elif 'customer' in q:
            intent['table'] = 'customers'
        elif 'product' in q:
            intent['table'] = 'products'
        elif 'inventory' in q:
            intent['table'] = 'inventory'
        
        # Detect filters
        if 'lastname' in q:
            # Extract lastname value (simple regex)
            import re
            match = re.search(r"lastname['\"]?\s*=?\s*['\"]?([^'\";\s]+)", q)
            if match:
                intent['filters']['lastname'] = match.group(1)
        
        if 'firstname' in q:
            import re
            match = re.search(r"firstname['\"]?\s*=?\s*['\"]?([^'\";\s]+)", q)
            if match:
                intent['filters']['firstname'] = match.group(1)
        
        if 'customer_id' in q or 'customerid' in q:
            import re
            match = re.search(r"(customer_id|customerid)\s*=?\s*(\d+)", q)
            if match:
                intent['filters']['customer_id'] = int(match.group(2))
        
        # Detect joins
        if intent['action'] == 'join':
            if 'order' in q and 'product' in q:
                intent['join_tables'] = ['order_items', 'products']
            if 'customer' in q and 'order' in q:
                if 'products' not in intent['join_tables']:
                    intent['join_tables'].insert(0, 'customers')
        
        logger.info(f"\nParsed user intent:\n{intent}\n")
        return intent
    
    def generate_from_intent(self, intent: Dict) -> str:
        """Generate SQL query from parsed intent."""
        database = intent['database']
        action = intent['action']
        table = intent['table']
        
        if not table:
            logger.error("Could not determine target table from question")
            return None
        
        try:
            if action == 'join':
                query = self.generate_join_query(
                    database=database,
                    primary_table=table,
                    join_tables=intent['join_tables'],
                    columns=intent['columns'],
                    where_clause=intent['filters'],
                    limit=100
                )
            else:
                query = self.generate_simple_select(
                    database=database,
                    table=table,
                    columns=intent['columns'],
                    where_clause=intent['filters'],
                    limit=100
                )
            
            # Print final generated query prominently
            print("\n" + "="*80)
            print(f"✅ FINAL QUERY GENERATED (Auto-Generated - 0 LLM Tokens Used)")
            print("="*80)
            print(f"Database: {database}")
            print(f"Action: {action.upper()}")
            print(f"Query:\n{query}")
            print("="*80 + "\n")
            
            return query
        except Exception as e:
            logger.error(f"Error generating query: {e}")
            print(f"❌ Error generating query: {e}\n")
            return None


# Global instance
query_generator = QueryGenerator()
