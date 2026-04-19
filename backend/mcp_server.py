import sys
import os
import json
import logging
import pyodbc
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

load_dotenv()

# Initialize logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
                "samples": inventory_meta["samples"],
                "relationships": inventory_meta.get("relationships", [])
            },
            "SalesDB_PostgreSQL": {
                "schema": sales_meta["schema"],
                "samples": sales_meta["samples"],
                "relationships": sales_meta.get("relationships", [])
            },
            "CustomerDB_MongoDB": {
                "schema": customer_meta["schema"],
                "samples": customer_meta["samples"]
            }
        }
    return _metadata_cache

@mcp.tool()
def get_database_info() -> str:
    """Returns the schema, sample data, and relationships for all available databases (SQL, Postgres, Mongo). Call this FIRST to understand the data structure."""
    metadata = get_metadata()
    return json.dumps(metadata, indent=2, default=str)


def execute_nosql(db_name: str, collection_name: str, query_type: str, query_payload: str):
    try:
        client = get_mongo_client()
        payload = json.loads(query_payload)
        db = client[db_name]
        collection = db[collection_name]
        
        # Print final query
        print("\n" + "="*80)
        print(f"📊 FINAL MONGODB QUERY EXECUTING")
        print("="*80)
        print(f"Database: {db_name}")
        print(f"Collection: {collection_name}")
        print(f"Query Type: {query_type.upper()}")
        print(f"Query Payload:\n{json.dumps(payload, indent=2, default=str)}")
        print("="*80 + "\n")
        logger.info(f"Executing MongoDB {query_type} on {db_name}.{collection_name}: {json.dumps(payload)}")
        
        if query_type == "find":
            results = list(collection.find(payload, {'_id': 0}).limit(50))
        elif query_type == "aggregate":
            results = [{k: v for k, v in doc.items() if k != '_id'} for doc in list(collection.aggregate(payload))][:50]
        else:
            return "Error: query_type must be 'find' or 'aggregate'"
            
        return json.dumps(results, default=str)
    except Exception as e:
        error_msg = f"Error: {e}"
        print(f"❌ MongoDB Query Error: {error_msg}\n")
        logger.error(f"MongoDB error: {error_msg}")
        return error_msg


@mcp.tool()
def query_customer_db(collection_name: str, query_payload: str, query_type: str = "find") -> str:
    """Query MongoDB for customer profiles and loyalty data. Use find or aggregate types."""
    return execute_nosql(os.getenv("CUSTOMER_DB", "CustomerDB"), collection_name, query_type.lower(), query_payload)

@mcp.tool()
def query_inventory_db(sql_query: str) -> str:
    """Query SQL Server InventoryDB for products and stock. Use T-SQL and quote [keywords]."""
    # Print final query
    print("\n" + "="*80)
    print(f"📊 FINAL SQL QUERY EXECUTING - SQL Server (Inventory DB)")
    print("="*80)
    print(f"SQL Query:\n{sql_query}")
    print("="*80 + "\n")
    logger.info(f"Executing SQL on InventoryDB: {sql_query}")
    
    conn_str = os.getenv("SQL_DB_CONN") or r"DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\MSSQLLocalDB;DATABASE=InventoryDB;Trusted_Connection=yes;"
    conn = None
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        print(f"✅ Query executed successfully. Rows returned: {len(results)}\n")
        return json.dumps(results[:50], default=str)
    except Exception as e:
        error_msg = f"SQL Error: {e}"
        print(f"❌ SQL Query Error: {error_msg}\n")
        logger.error(f"SQL Server error: {error_msg}")
        return error_msg
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

@mcp.tool()
def query_sales_db(sql_query: str) -> str:
    """Query PostgreSQL SalesDB for orders and revenue. Use Standard SQL and quote \"keywords\"."""
    # Print final query
    print("\n" + "="*80)
    print(f"📊 FINAL SQL QUERY EXECUTING - PostgreSQL (Sales DB)")
    print("="*80)
    print(f"SQL Query:\n{sql_query}")
    print("="*80 + "\n")
    logger.info(f"Executing SQL on SalesDB: {sql_query}")
    
    conn_str = os.getenv("PG_DB_CONN")
    conn = None
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql_query)
        results = cursor.fetchall()
        print(f"✅ Query executed successfully. Rows returned: {len(results)}\n")
        return json.dumps([dict(r) for r in results[:50]], default=str)
    except Exception as e:
        error_msg = f"Postgres Error: {e}"
        print(f"❌ PostgreSQL Query Error: {error_msg}\n")
        logger.error(f"PostgreSQL error: {error_msg}")
        return error_msg
    finally:
        if conn:
            try:
                conn.close()
            except:
                pass

if __name__ == "__main__":
    mcp.run()
