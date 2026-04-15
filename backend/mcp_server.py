import warnings
warnings.filterwarnings("ignore")
import json
import os
import pyodbc
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

# Import Schema Metadata
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

def validate_nosql_query(query_dict: dict):
    query_str = json.dumps(query_dict).lower()
    blocked = ["$delete", "$update", "$set", "$unset", "$drop"]
    if any(op in query_str for op in blocked):
        raise ValueError("Unsafe NoSQL query detected.")

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
    return execute_nosql(os.getenv("CUSTOMER_DB", "CustomerDB"), collection_name, query_type.lower(), query_payload)

query_customer_db.__doc__ = f"""
Query the MongoDB CustomerDB for customer profiles and loyalty data.
SCHEMA: {json.dumps(CUSTOMER_DB_SCHEMA, indent=2)}
SAMPLES: {json.dumps(CUSTOMER_DB_SAMPLES, indent=2)}
"""

@mcp.tool()
def query_inventory_db(sql_query: str) -> str:
    """Query SQL Server InventoryDB for products, stock, and suppliers."""
    conn_str = os.getenv("HR_DB_CONN") or "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=InventoryDB;Trusted_Connection=yes;"
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return json.dumps(results[:50], default=str)
    except Exception as e:
        return f"SQL Error: {e}"

query_inventory_db.__doc__ = f"""
Query SQL Server InventoryDB for products and stock.
SCHEMA: {json.dumps(INVENTORY_DB_SCHEMA, indent=2)}
SAMPLES: {json.dumps(INVENTORY_DB_SAMPLES, indent=2)}
"""

@mcp.tool()
def query_sales_db(sql_query: str) -> str:
    """Query PostgreSQL SalesDB for orders, transactions, and revenue."""
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

query_sales_db.__doc__ = f"""
Query PostgreSQL SalesDB for orders and transactions.
SCHEMA: {json.dumps(SALES_DB_SCHEMA, indent=2)}
SAMPLES: {json.dumps(SALES_DB_SAMPLES, indent=2)}
"""

if __name__ == "__main__":
    mcp.run()
