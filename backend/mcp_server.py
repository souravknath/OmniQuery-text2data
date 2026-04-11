import warnings
warnings.filterwarnings("ignore")
import json
import os
import pyodbc
from dotenv import load_dotenv
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

# Import Schema Metadata
from database_schema import (
    MONGODB_CUSTOMER_DB_SCHEMA, MONGODB_CUSTOMER_DB_SAMPLES,
    SQL_USERS_ORDERS_DB_SCHEMA, SQL_USERS_ORDERS_DB_SAMPLES,
    SQL_LOCATIONS_DB_SCHEMA, SQL_LOCATIONS_DB_SAMPLES
)

load_dotenv()

# Initialize the MCP Server
mcp = FastMCP("OmniQuery Customer Registry")

# NoSQL Layer (CustomerDB only)
mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
_mongo_client = MongoClient(mongo_uri)

def validate_nosql_query(query_dict: dict):
    """Ensures only read operations are allowed."""
    query_str = json.dumps(query_dict).lower()
    blocked = ["$delete", "$update", "$set", "$unset", "$drop"]
    if any(op in query_str for op in blocked):
        raise ValueError("Unsafe NoSQL query detected. Only read operations are allowed.")

def execute_nosql(db_name: str, collection_name: str, query_type: str, query_payload: str):
    """Executes a NoSQL query securely."""
    try:
        payload = json.loads(query_payload)
        
        if isinstance(payload, dict):
            validate_nosql_query(payload)
        elif isinstance(payload, list):
            for stage in payload:
                validate_nosql_query(stage)

        db = _mongo_client[db_name]
        collection = db[collection_name]
        
        if query_type == "find":
            cursor = collection.find(payload, {'_id': 0}).limit(50)
            results = list(cursor)
        elif query_type == "aggregate":
            cursor = collection.aggregate(payload)
            results = [{k: v for k, v in doc.items() if k != '_id'} for doc in list(cursor)][:50]
        else:
            return "Error: query_type must be 'find' or 'aggregate'"
            
        if not results:
            return "No records found in CustomerDB."
            
        json_output = json.dumps(results, default=str)
        if len(results) == 50:
            return json_output + "\n\n(Warning: Results limited to 50 records to prevent memory crash. Use precise filters or aggregates for full data analysis)."
        return json_output
    except json.JSONDecodeError:
        return "Error: query_payload must be valid JSON."
    except Exception as e:
        return f"Error executing NoSQL query: {str(e)}"


@mcp.tool()
def query_customer_db(collection_name: str, query_payload: str, query_type: str = "find") -> str:
    return execute_nosql("CustomerDB", collection_name, query_type.lower(), query_payload)

# Dynamically set tool description from schema
query_customer_db.__doc__ = f"""
Query the MongoDB CustomerDB for profiles, activities, and support tickets.

SCHEMA:
- customers: {MONGODB_CUSTOMER_DB_SCHEMA['customers']}
- activities: {MONGODB_CUSTOMER_DB_SCHEMA['activities']}
- support_tickets: {MONGODB_CUSTOMER_DB_SCHEMA['support_tickets']}

SAMPLES:
{json.dumps(MONGODB_CUSTOMER_DB_SAMPLES, indent=2)}

RELATIONSHIP: 
- customers.location_id -> SQL Location.dbo.Locations.LocationId

IMPORTANT: Use query_type 'find' or 'aggregate'. Provide 'query_payload' as a valid JSON string.
"""

@mcp.tool()
def query_users_orders_db(sql_query: str) -> str:
    query_lower = sql_query.lower()
    if any(blocked in query_lower for blocked in ["insert ", "update ", "delete ", "drop ", "truncate ", "alter "]):
        return "Error: Only read-only SELECT queries are allowed."
        
    conn_str = os.getenv("HR_DB_CONN", "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=Users;Trusted_Connection=yes;")
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        if not cursor.description:
            return "Query executed successfully, but returned no data."
            
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        json_output = json.dumps(results, default=str)
        if len(results) > 50:
            return json.dumps(results[:50], default=str) + "\n\n(Warning: Results limited to 50 records to prevent memory crash. Use precise WHERE clauses or aggregations)."
        return json_output
    except Exception as e:
        return f"SQL Error: {str(e)}"

# Dynamically set tool description from schema
query_users_orders_db.__doc__ = f"""
Executes a Microsoft SQL Server (T-SQL) query on Users and Orders Database.

SCHEMA:
- Users: {SQL_USERS_ORDERS_DB_SCHEMA['Users']}
- Orders: {SQL_USERS_ORDERS_DB_SCHEMA['Orders']}
- User_Orders: {SQL_USERS_ORDERS_DB_SCHEMA['User_Orders']}

SAMPLES:
{json.dumps(SQL_USERS_ORDERS_DB_SAMPLES, indent=2)}

CROSS-DATABASE JOIN:
Users.dbo.Users.LocationId = Location.dbo.Locations.LocationId

IMPORTANT: Use 'TOP 50' in your SELECT statements to avoid returning too much data.
"""

@mcp.tool()
def query_locations_db(sql_query: str) -> str:
    query_lower = sql_query.lower()
    if any(blocked in query_lower for blocked in ["insert ", "update ", "delete ", "drop ", "truncate ", "alter "]):
        return "Error: Only read-only SELECT queries are allowed."
        
    conn_str = os.getenv("SALES_DB_CONN", "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=Location;Trusted_Connection=yes;")
    try:
        conn = pyodbc.connect(conn_str, timeout=5)
        cursor = conn.cursor()
        cursor.execute(sql_query)
        
        if not cursor.description:
            return "Query executed successfully, but returned no data."
            
        columns = [column[0] for column in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        
        json_output = json.dumps(results, default=str)
        if len(results) > 50:
            return json.dumps(results[:50], default=str) + "\n\n(Warning: Results limited to 50 records to prevent memory crash. Use precise WHERE clauses or aggregations)."
        return json_output
    except Exception as e:
        return f"SQL Error: {str(e)}"

# Dynamically set tool description from schema
query_locations_db.__doc__ = f"""
Executes a Microsoft SQL Server (T-SQL) query on the Locations Database.

SCHEMA:
- Locations: {SQL_LOCATIONS_DB_SCHEMA['Locations']}

SAMPLES:
{json.dumps(SQL_LOCATIONS_DB_SAMPLES, indent=2)}

IMPORTANT: Use 'TOP 50' in your SELECT statements to avoid returning too much data.
"""

if __name__ == "__main__":
    mcp.run()


