import sys
import os
import json
import pyodbc
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

# Load environment variables from 'env' file in the same directory
env_file = os.path.join(os.path.dirname(__file__), "env")
load_dotenv(env_file, override=True)
# Initialize the MCP Server
mcp = FastMCP("OmniQuery Retail & Sales Engine")

# MongoDB Layer
_mongo_client = None
_metadata_cache = None

def get_mongo_client():
    """Lazy initialization of MongoDB client."""
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"), serverSelectionTimeoutMS=5000)
    return _mongo_client

def get_metadata():
    """Lazy-load metadata only when needed."""
    global _metadata_cache
    if _metadata_cache is None:
        from schema_fetcher import (
            fetch_sql_server_metadata,
            fetch_postgres_metadata,
            fetch_mongo_metadata
        )
        
        sql_conn = os.getenv("SQL_DB_CONN") or r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=ALIPL6375\SQLEXPRESS;DATABASE=InventoryDB;Trusted_Connection=yes;"
        pg_conn = os.getenv("PG_DB_CONN") or "dbname=customer_db user=postgres password=root host=localhost port=5432"
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        mongo_db = os.getenv("CUSTOMER_DB", "CustomerDB")
        
        inventory_meta = fetch_sql_server_metadata(sql_conn)
        sales_meta = fetch_postgres_metadata(pg_conn)
        customer_meta = fetch_mongo_metadata(mongo_uri, mongo_db)
        
        _metadata_cache = {
            "InventoryDB_SQL_Server": {
                "schema": inventory_meta["schema"],
                "relationships": inventory_meta.get("relationships", [])
            },
            "SalesDB_PostgreSQL": {
                "schema": sales_meta["schema"],
                "relationships": sales_meta.get("relationships", [])
            },
            "CustomerDB_MongoDB": {
                "schema": customer_meta["schema"]
            }
        }
    return _metadata_cache

@mcp.tool()
def get_database_info() -> str:
    """Returns the schema and relationships for all available databases (SQL, Postgres, Mongo). Call this FIRST to understand the data structure."""
    metadata = get_metadata()
    return json.dumps(metadata, indent=2, default=str)


def execute_nosql(db_name: str, collection_name: str, query_type: str, query_payload: str):
    try:
        client = get_mongo_client()
        payload = json.loads(query_payload)
        db = client[db_name]
        collection = db[collection_name]
        
        if query_type == "find":
            results = list(collection.find(payload, {'_id': 0}).limit(50))
        elif query_type == "aggregate":
            results = [{k: v for k, v in doc.items() if k != '_id'} for doc in list(collection.aggregate(payload))][:50]
        else:
            return "Error: query_type must be 'find' or 'aggregate'"
            
        return json.dumps(results, default=str)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
def query_customer_db(collection_name: str, query_payload: str, query_type: str = "find") -> str:
    """Query MongoDB for customer profiles and loyalty data. Use find or aggregate types."""
    return execute_nosql(os.getenv("CUSTOMER_DB", "CustomerDB"), collection_name, query_type.lower(), query_payload)

@mcp.tool()
def query_inventory_db(sql_query: str) -> str:
    """Query SQL Server InventoryDB for products and stock. Use T-SQL and quote [keywords]."""
    conn_str = os.getenv("SQL_DB_CONN") or r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\MSSQLLocalDB;DATABASE=InventoryDB;Trusted_Connection=yes;"
    conn = None
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps(results[:50], default=str)
    except Exception as e:
        return f"SQL Error: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

@mcp.tool()
def query_sales_db(sql_query: str) -> str:
    """Query PostgreSQL SalesDB for orders and revenue. Use Standard SQL and quote \"keywords\"."""
    conn_str = os.getenv("PG_DB_CONN")
    conn = None
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql_query)
        results = cursor.fetchall()
        return json.dumps([dict(r) for r in results[:50]], default=str)
    except Exception as e:
        return f"Postgres Error: {e}"
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    mcp.run()
