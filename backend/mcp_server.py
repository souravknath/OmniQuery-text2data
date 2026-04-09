import warnings
warnings.filterwarnings("ignore")

import asyncio
import json
import os
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
    - customers: profile (name, age, gender), location (city, state, country), segments (list), financial (total_spent, avg_order_value), is_active
    - activities: customer_id, activity_type, product_category, amount, timestamp
    - support_tickets: customer_id, issue_type, status, priority
    IMPORTANT: Provide the 'query_payload' as a valid JSON string. Use query_type 'find' or 'aggregate'.
    Example: {"location.country": "USA"}
    """
    return execute_nosql("CustomerDB", collection_name, query_type.lower(), query_payload)




if __name__ == "__main__":
    mcp.run()


