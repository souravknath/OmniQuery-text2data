import warnings
warnings.filterwarnings("ignore")

import asyncio
import json
import os
import pyodbc
from dotenv import load_dotenv
from pymongo import MongoClient
from mcp.server.fastmcp import FastMCP

load_dotenv()

# Initialize the MCP Server
mcp = FastMCP("Talk2Data Customer Registry")

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
    """
    Use for customer profiles, activities, and support tickets (MongoDB CustomerDB).
    Collections: 
    - customers: profile (name, age, gender), location_id (INT - links to SQL Locations database), segments (list), financial (total_spent, avg_order_value), is_active
    - activities: customer_id, activity_type, product_category, amount, timestamp
    - support_tickets: customer_id, issue_type, status, priority
    
    RELATIONSHIP: 
    - customers.location_id maps to Location.dbo.Locations.LocationId in the SQL Server database.
    - To find a customer's physical address, fetch the customer from Mongo first, then query the SQL database using their location_id.
    
    IMPORTANT: Provide the 'query_payload' as a valid JSON string. Use query_type 'find' or 'aggregate'.
    Example: {"location_id": 5}
    """
    return execute_nosql("CustomerDB", collection_name, query_type.lower(), query_payload)

@mcp.tool()
def query_users_orders_db(sql_query: str) -> str:
    """
    Executes a Microsoft SQL Server (T-SQL) query on the Users and Orders Database.
    Use this for any user or order-related questions.
    
    Tables available:
    1. Users.dbo.Users (UserId, FirstName, LastName, EmailId, UserName, LocationId)
    2. Users.dbo.Orders (OrderId, OrderName, Amount, OrderDate)
    3. Users.dbo.User_Orders (Id, UserId, OrderId)
    
    CROSS-DATABASE RELATIONSHIP: 
    - To get State, City, or Address for a User, you MUST join with the Location database.
    - JOIN CLUES: Users.dbo.Users.LocationId = Location.dbo.Locations.LocationId
    
    MANDATORY SYNTAX FOR LOCATION QUERIES:
    SELECT u.FirstName, l.State, l.City 
    FROM Users.dbo.Users u 
    JOIN Location.dbo.Locations l ON u.LocationId = l.LocationId 
    WHERE l.State = 'StateName'
    
    IMPORTANT: Provide the 'sql_query' as a valid T-SQL SELECT statement. 
    Use 'TOP 50' in your queries to avoid returning too much data.
    """
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




@mcp.tool()
def query_locations_db(sql_query: str) -> str:
    """
    Executes a Microsoft SQL Server (T-SQL) query on the Locations Database.
    Use this for any address, city, state, or country-related lookups.
    
    Tables available:
    1. Locations (LocationId int, Address nvarchar, City nvarchar, State nvarchar, Country nvarchar, ZipCode nvarchar)
    
    IMPORTANT: Provide the 'sql_query' as a valid T-SQL SELECT statement. 
    Use 'TOP 50' in your queries to avoid returning too much data.
    """
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

if __name__ == "__main__":
    mcp.run()


