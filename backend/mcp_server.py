import sys
import os
import json
import pyodbc
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

# We will now Lazy-Load the metadata within a tool to ensure fast startup and avoid pipe corruption.
from database_schema import (
    INVENTORY_DB_SCHEMA, INVENTORY_DB_SAMPLES,
    SALES_DB_SCHEMA, SALES_DB_SAMPLES,
    CUSTOMER_DB_SCHEMA, CUSTOMER_DB_SAMPLES
)

load_dotenv()

# Initialize the MCP Server
mcp = FastMCP("OmniQuery Retail & Sales Engine")

# MongoDB Layer
_mongo_client = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))

@mcp.tool()
def get_database_info() -> str:
    """Returns the schema, sample data, and relationships for all available databases (SQL, Postgres, Mongo). Call this FIRST to understand the data structure."""
    info = {
        "InventoryDB_SQL_Server": {
            "schema": INVENTORY_DB_SCHEMA,
            "samples": INVENTORY_DB_SAMPLES
        },
        "SalesDB_PostgreSQL": {
            "schema": SALES_DB_SCHEMA,
            "samples": SALES_DB_SAMPLES
        },
        "CustomerDB_MongoDB": {
            "schema": CUSTOMER_DB_SCHEMA,
            "samples": CUSTOMER_DB_SAMPLES
        }
    }
    return json.dumps(info, indent=2, default=str)

def execute_nosql(db_name: str, collection_name: str, query_type: str, query_payload: str):
    try:
        payload = json.loads(query_payload)
        db = _mongo_client[db_name]
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
    conn_str = os.getenv("SQL_DB_CONN") or "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=InventoryDB;Trusted_Connection=yes;"
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps(results[:50], default=str)
    except Exception as e:
        return f"SQL Error: {e}"

@mcp.tool()
def query_sales_db(sql_query: str) -> str:
    """Query PostgreSQL SalesDB for orders and revenue. Use Standard SQL and quote \"keywords\"."""
    conn_str = os.getenv("PG_DB_CONN")
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql_query)
        results = cursor.fetchall()
        return json.dumps(results[:50], default=str)
    except Exception as e:
        return f"Postgres Error: {e}"
    finally:
        if 'conn' in locals(): conn.close()

if __name__ == "__main__":
    mcp.run()
