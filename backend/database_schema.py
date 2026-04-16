import os
import sys
from dotenv import load_dotenv
from schema_fetcher import (
    fetch_sql_server_metadata,
    fetch_postgres_metadata,
    fetch_mongo_metadata
)

load_dotenv()

# --- Connection Strings ---
SQL_DB_CONN = os.getenv("SQL_DB_CONN") or "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=InventoryDB;Trusted_Connection=yes;"
PG_DB_CONN = os.getenv("PG_DB_CONN") or "dbname=SalesDB user=postgres password=postgres host=localhost port=5432"
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = os.getenv("CUSTOMER_DB", "CustomerDB")

# --- Fetch Metadata (Database-First) ---
# Silent initialization to protect MCP stdout protocol

# 1. SQL Server - Inventory Metadata
inventory_meta = fetch_sql_server_metadata(SQL_DB_CONN)
INVENTORY_DB_SCHEMA = inventory_meta["schema"]
INVENTORY_DB_SAMPLES = inventory_meta["samples"]
if inventory_meta["relationships"]:
    INVENTORY_DB_SCHEMA["_relationships"] = inventory_meta["relationships"]

# 2. Postgres - Sales Metadata
sales_meta = fetch_postgres_metadata(PG_DB_CONN)
SALES_DB_SCHEMA = sales_meta["schema"]
SALES_DB_SAMPLES = sales_meta["samples"]
if sales_meta["relationships"]:
    SALES_DB_SCHEMA["_relationships"] = sales_meta["relationships"]

# 3. MongoDB - Customer Metadata
customer_meta = fetch_mongo_metadata(MONGO_URI, MONGO_DB_NAME)
CUSTOMER_DB_SCHEMA = customer_meta["schema"]
CUSTOMER_DB_SAMPLES = customer_meta["samples"]
