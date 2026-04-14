import os
import pyodbc
import psycopg2
from pymongo import MongoClient
import json
from dotenv import load_dotenv

# Ensure environment variables are loaded
load_dotenv()

def fetch_sql_server_schema(conn_str):
    """Fetches schema information from SQL Server (T-SQL)."""
    if not conn_str:
        return {}
    
    try:
        # Check if it's a sqlite string and skip if so (as pyodbc won't handle it easily here)
        if conn_str.startswith("sqlite"):
            print(f"Skipping schema fetch for SQLite-like string: {conn_str}")
            return {}

        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Get Primary Keys
        cursor.execute("""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
        """)
        pks = {(row[0], row[1]) for row in cursor.fetchall()}

        schema = {}
        for table in tables:
            cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table}'")
            cols = []
            for row in cursor.fetchall():
                col_name, data_type = row[0], row[1]
                is_pk = " (PK)" if (table, col_name) in pks else ""
                cols.append(f"{col_name}{is_pk} ({data_type})")
            schema[table] = {"columns": cols}
        
        conn.close()
        return schema
    except Exception as e:
        print(f"Error fetching SQL Server schema from {conn_str}: {e}")
        return {}

def fetch_postgres_schema(conn_str):
    """Fetches schema information from PostgreSQL."""
    if not conn_str:
        return {}
    
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor()
        
        # Get all tables in public schema
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        tables = [row[0] for row in cursor.fetchall()]

        # Get Primary Keys
        cursor.execute("""
            SELECT tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = 'public'
        """)
        pks = {(row[0], row[1]) for row in cursor.fetchall()}
        
        schema = {}
        for table in tables:
            cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' AND table_schema = 'public'")
            cols = []
            for row in cursor.fetchall():
                col_name, data_type = row[0], row[1]
                is_pk = " (PK)" if (table, col_name) in pks else ""
                cols.append(f"{col_name}{is_pk} ({data_type})")
            schema[table] = {"columns": cols}
        
        conn.close()
        return schema
    except Exception as e:
        print(f"Error fetching Postgres schema: {e}")
        return {}

def fetch_mongo_schema(mongo_uri, db_name):
    """Fetches schema information from MongoDB by sampling collections."""
    if not mongo_uri:
        return {}
    
    try:
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collections = db.list_collection_names()
        
        schema = {}
        for coll_name in collections:
            coll = db[coll_name]
            sample = coll.find_one()
            if sample:
                # Remove _id from schema definition
                if '_id' in sample: del sample['_id']
                
                def infer_type(v):
                    if isinstance(v, dict):
                        return {sk: infer_type(sv) for sk, sv in v.items()}
                    elif isinstance(v, list):
                        return "LIST of " + (type(v[0]).__name__ if v else "items")
                    else:
                        return type(v).__name__

                schema[coll_name] = {
                    "description": f"Collection {coll_name}",
                    "fields": {k: infer_type(v) for k, v in sample.items()}
                }
            else:
                schema[coll_name] = {"description": f"Collection {coll_name} (Empty)", "fields": {}}
        
        client.close()
        return schema
    except Exception as e:
        print(f"Error fetching Mongo schema from {mongo_uri}: {e}")
        return {}

def fetch_relationships(db_type, conn_str):
    """Fetches Foreign Key relationships from SQL Server or PostgreSQL."""
    if not conn_str or conn_str.startswith("sqlite"):
        return []
        
    try:
        relationships = []
        if db_type == "sql_server":
            conn = pyodbc.connect(conn_str, timeout=5)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    tp.name AS Table_Name,
                    cp.name AS Column_Name,
                    tr.name AS Referenced_Table,
                    cr.name AS Referenced_Column
                FROM sys.foreign_keys AS fk
                INNER JOIN sys.tables AS tp ON fk.parent_object_id = tp.object_id
                INNER JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
                INNER JOIN sys.columns AS cp ON fkc.parent_object_id = cp.object_id AND fkc.parent_column_id = cp.column_id
                INNER JOIN sys.tables AS tr ON fk.referenced_object_id = tr.object_id
                INNER JOIN sys.columns AS cr ON fkc.referenced_object_id = cr.object_id AND fkc.referenced_column_id = cr.column_id
            """)
            for row in cursor.fetchall():
                relationships.append(f"{row[0]}.{row[1]} -> {row[2]}.{row[3]}")
            conn.close()
            
        elif db_type == "postgres":
            conn = psycopg2.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT
                    tc.table_name, 
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name 
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = 'public';
            """)
            for row in cursor.fetchall():
                relationships.append(f"{row[0]}.{row[1]} -> {row[2]}.{row[3]}")
            conn.close()
            
        return relationships
    except Exception as e:
        print(f"Error fetching relationships for {db_type}: {e}")
        return []

def fetch_samples(db_type, conn_str, schema, db_name=None):
    """Fetches a sample record for each table/collection."""
    samples = {}
    
    try:
        if db_type == "sql_server":
            if not conn_str or conn_str.startswith("sqlite"): return {}
            conn = pyodbc.connect(conn_str, timeout=5)
            cursor = conn.cursor()
            for table in schema.keys():
                cursor.execute(f"SELECT TOP 1 * FROM {table}")
                row = cursor.fetchone()
                if row:
                    columns = [column[0] for column in cursor.description]
                    samples[table] = [dict(zip(columns, row))]
            conn.close()
            
        elif db_type == "postgres":
            if not conn_str: return {}
            conn = psycopg2.connect(conn_str)
            cursor = conn.cursor()
            for table in schema.keys():
                cursor.execute(f"SELECT * FROM {table} LIMIT 1")
                row = cursor.fetchone()
                if row:
                    from psycopg2.extras import RealDictCursor
                    # Re-run with RealDictCursor for easier dict conversion
                    cursor_dict = conn.cursor(cursor_factory=RealDictCursor)
                    cursor_dict.execute(f"SELECT * FROM {table} LIMIT 1")
                    samples[table] = [cursor_dict.fetchone()]
            conn.close()
            
        elif db_type == "mongo":
            mongo_uri = conn_str # Reusing conn_str parameter for mongo_uri
            if not mongo_uri: return {}
            client = MongoClient(mongo_uri)
            db = client[db_name]
            for coll_name in schema.keys():
                coll = db[coll_name]
                sample = coll.find_one({}, {'_id': 0})
                if sample:
                    samples[coll_name] = [sample]
            client.close()
            
    except Exception as e:
        print(f"Error fetching samples for {db_type}: {e}")
        
    return samples
