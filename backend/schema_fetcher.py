import os
import pyodbc
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient
import json
import sys
import logging
from dotenv import load_dotenv

load_dotenv()

# Setup logging instead of using sys.stderr
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
logger = logging.getLogger(__name__)
handler = logging.FileHandler(os.path.join(log_dir, "schema_fetcher.log"))
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.ERROR)

def fetch_sql_server_metadata(conn_str):
    """Fetches schema, relationships, and samples from SQL Server."""
    if not conn_str or conn_str.startswith("sqlite"):
        return {"schema": {}, "relationships": [], "samples": {}}
    
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        
        # 1. Fetch Schema
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        """)
        pks = {(row[0], row[1]) for row in cursor.fetchall()}

        schema = {}
        for table in tables:
            # Quote table name for SQL Server
            cursor.execute(f"SELECT COUNT(*) FROM [{table}]")
            count = cursor.fetchone()[0]
            
            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
            cols = []
            for row in cursor.fetchall():
                col_name, data_type = row[0], row[1]
                is_pk = " (PK)" if (table, col_name) in pks else ""
                cols.append(f"{col_name}{is_pk} ({data_type})")
            schema[table] = {"columns": cols, "record_count": count}
            
        # 2. Fetch Relationships
        relationships = []
        cursor.execute("""
            SELECT 
                tp.name AS Table_Name, cp.name AS Column_Name,
                tr.name AS Referenced_Table, cr.name AS Referenced_Column
            FROM sys.foreign_keys AS fk
            INNER JOIN sys.tables AS tp ON fk.parent_object_id = tp.object_id
            INNER JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
            INNER JOIN sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
            INNER JOIN sys.tables AS tr ON fk.referenced_object_id = tr.object_id
            INNER JOIN sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
        """)
        for row in cursor.fetchall():
            relationships.append(f"{row[0]}.{row[1]} -> {row[2]}.{row[3]}")
            
        # 3. Fetch Samples
        samples = {}
        for table in tables:
            cursor.execute(f"SELECT TOP 1 * FROM [{table}]")
            row = cursor.fetchone()
            if row:
                columns = [column[0] for column in cursor.description]
                samples[table] = [dict(zip(columns, row))]
                
        conn.close()
        return {"schema": schema, "relationships": relationships, "samples": samples}
    except Exception as e:
        # Use logger instead of stderr
        logger.error(f"SQL Server Metadata Error: {e}")
        return {"schema": {}, "relationships": [], "samples": {}}

def fetch_postgres_metadata(conn_str):
    """Fetches schema, relationships, and samples from PostgreSQL."""
    if not conn_str:
        return {"schema": {}, "relationships": [], "samples": {}}
    
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        
        # 1. Fetch Schema
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cursor.fetchall()]

        cursor.execute("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
        """)
        pks = {(row[0], row[1]) for row in cursor.fetchall()}
        
        schema = {}
        for table in tables:
            # Quote table name for Postgres
            cursor.execute(f'SELECT COUNT(*) FROM "{table}"')
            count = cursor.fetchone()[0]

            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = 'public'")
            cols = []
            for row in cursor.fetchall():
                col_name, data_type = row[0], row[1]
                is_pk = " (PK)" if (table, col_name) in pks else ""
                cols.append(f"{col_name}{is_pk} ({data_type})")
            schema[table] = {"columns": cols, "record_count": count}
            
        # 2. Fetch Relationships
        relationships = []
        cursor.execute("""
            SELECT tc.table_name, kcu.column_name, ccu.table_name AS foreign_table, ccu.column_name AS foreign_column
            FROM information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';
        """)
        for row in cursor.fetchall():
            relationships.append(f"{row[0]}.{row[1]} -> {row[2]}.{row[3]}")
            
        # 3. Fetch Samples
        samples = {}
        from psycopg2.extras import RealDictCursor
        dict_cursor = conn.cursor(cursor_factory=RealDictCursor)
        for table in tables:
            dict_cursor.execute(f'SELECT * FROM "{table}" LIMIT 1')
            row = dict_cursor.fetchone()
            if row: samples[table] = [row]
            
        conn.close()
        return {"schema": schema, "relationships": relationships, "samples": samples}
    except Exception as e:
        logger.error(f"Postgres Metadata Error: {e}")
        return {"schema": {}, "relationships": [], "samples": {}}

def fetch_mongo_metadata(mongo_uri, db_name):
    """Fetches schema and samples from MongoDB."""
    if not mongo_uri:
        return {"schema": {}, "samples": {}}
    
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collections = db.list_collection_names()
        
        schema = {}
        samples = {}
        for coll_name in collections:
            coll = db[coll_name]
            sample = coll.find_one({}, {'_id': 0})
            count = coll.count_documents({})
            
            def infer_type(v):
                if isinstance(v, dict): return {sk: infer_type(sv) for sk, sv in v.items()}
                elif isinstance(v, list): return f"LIST of {type(v[0]).__name__}" if v else "LIST"
                else: return type(v).__name__

            if sample:
                schema[coll_name] = {"fields": {k: infer_type(v) for k, v in sample.items()}, "record_count": count}
                samples[coll_name] = [sample]
            else:
                schema[coll_name] = {"fields": {}, "record_count": 0}
                
        client.close()
        return {"schema": schema, "samples": samples}
    except Exception as e:
        logger.error(f"Mongo Metadata Error: {e}")
        return {"schema": {}, "samples": {}}
