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
    MONGODB_HEALTHCARE_DB_SCHEMA, MONGODB_HEALTHCARE_DB_SAMPLES,
    SQL_HOSPITAL_DB_SCHEMA, SQL_HOSPITAL_DB_SAMPLES,
    SQL_FACILITIES_DB_SCHEMA, SQL_FACILITIES_DB_SAMPLES,
    POSTGRES_PHARMACY_DB_SCHEMA, POSTGRES_PHARMACY_DB_SAMPLES
)

load_dotenv()

# Initialize the MCP Server
mcp = FastMCP("OmniQuery Healthcare Registry")

# NoSQL Layer (HealthcareDB only)
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
            return f"No records found in {db_name}."
            
        json_output = json.dumps(results, default=str)
        if len(results) == 50:
            return json_output + "\n\n(Warning: Results limited to 50 records to prevent memory crash. Use precise filters or aggregates for full data analysis)."
        return json_output
    except json.JSONDecodeError:
        return "Error: query_payload must be valid JSON."
    except Exception as e:
        return f"Error executing NoSQL query: {str(e)}"

@mcp.tool()
def query_healthcare_db(collection_name: str, query_payload: str, query_type: str = "find") -> str:
    return execute_nosql("HealthcareDB", collection_name, query_type.lower(), query_payload)

# Dynamically set tool description from schema
query_healthcare_db.__doc__ = f"""
Query the MongoDB HealthcareDB for patient profiles and medical encounters.

SCHEMA:
{json.dumps(MONGODB_HEALTHCARE_DB_SCHEMA, indent=2)}

SAMPLES:
{json.dumps(MONGODB_HEALTHCARE_DB_SAMPLES, indent=2)}

RELATIONSHIP: 
- patients.patient_id matches HospitalDB.dbo.Appointments.PatientId
- patients.facility_id -> SQL FacilityDB.dbo.Facilities.FacilityId

IMPORTANT: Use query_type 'find' or 'aggregate'. Provide 'query_payload' as a valid JSON string.
"""

@mcp.tool()
def query_hospital_db(sql_query: str) -> str:
    query_lower = sql_query.lower()
    if any(blocked in query_lower for blocked in ["insert ", "update ", "delete ", "drop ", "truncate ", "alter "]):
        return "Error: Only read-only SELECT queries are allowed."
        
    conn_str = os.getenv("HR_DB_CONN")
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
query_hospital_db.__doc__ = f"""
Executes a Microsoft SQL Server (T-SQL) query on Hospital Database (Doctors & Appointments).

SCHEMA:
{json.dumps(SQL_HOSPITAL_DB_SCHEMA, indent=2)}

SAMPLES:
{json.dumps(SQL_HOSPITAL_DB_SAMPLES, indent=2)}

CROSS-DATABASE JOIN:
- HospitalDB.dbo.Appointments.PatientId matches HealthcareDB.patients.patient_id
- Doctors.FacilityId = FacilityDB.dbo.Facilities.FacilityId

IMPORTANT: Use 'TOP 50' in your SELECT statements to avoid returning too much data.
"""

@mcp.tool()
def query_facility_db(sql_query: str) -> str:
    query_lower = sql_query.lower()
    if any(blocked in query_lower for blocked in ["insert ", "update ", "delete ", "drop ", "truncate ", "alter "]):
        return "Error: Only read-only SELECT queries are allowed."
        
    conn_str = os.getenv("SALES_DB_CONN")
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
query_facility_db.__doc__ = f"""
Executes a Microsoft SQL Server (T-SQL) query on the Facilities Database.

SCHEMA:
{json.dumps(SQL_FACILITIES_DB_SCHEMA, indent=2)}

SAMPLES:
{json.dumps(SQL_FACILITIES_DB_SAMPLES, indent=2)}

IMPORTANT: Use 'TOP 50' in your SELECT statements to avoid returning too much data.
"""

@mcp.tool()
def query_pharmacy_db(sql_query: str) -> str:
    """Executes a PostgreSQL query on the Pharmacy Database."""
    query_lower = sql_query.lower()
    if any(blocked in query_lower for blocked in ["insert ", "update ", "delete ", "drop ", "truncate ", "alter "]):
        return "Error: Only read-only SELECT queries are allowed."
        
    conn_str = os.getenv("PG_DB_CONN")
    try:
        conn = psycopg2.connect(conn_str)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute(sql_query)
        
        results = cursor.fetchall()
        
        json_output = json.dumps(results, default=str)
        if len(results) > 50:
            return json.dumps(results[:50], default=str) + "\n\n(Warning: Results limited to 50 records to prevent memory crash.)"
        return json_output
    except Exception as e:
        return f"PostgreSQL Error: {str(e)}"
    finally:
        if 'conn' in locals():
            conn.close()

# Dynamically set tool description from schema
query_pharmacy_db.__doc__ = f"""
Executes a PostgreSQL query on the Pharmacy Inventory and Prescriptions Database.

SCHEMA:
{json.dumps(POSTGRES_PHARMACY_DB_SCHEMA, indent=2)}

SAMPLES:
{json.dumps(POSTGRES_PHARMACY_DB_SAMPLES, indent=2)}

IMPORTANT: Provide a valid PostgreSQL SELECT statement. Use 'LIMIT 50' to avoid large data returns.
"""

if __name__ == "__main__":
    mcp.run()


