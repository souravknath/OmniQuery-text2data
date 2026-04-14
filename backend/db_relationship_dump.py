import os
import json
from dotenv import load_dotenv
from schema_fetcher import (
    fetch_sql_server_schema,
    fetch_postgres_schema,
    fetch_mongo_schema,
    fetch_relationships
)

load_dotenv()

def dump_db_metadata():
    metadata = {
        "sql_server_hospital": {
            "schema": fetch_sql_server_schema(os.getenv("HR_DB_CONN") or "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=HospitalDB;Trusted_Connection=yes;"),
            "relationships": fetch_relationships("sql_server", os.getenv("HR_DB_CONN") or "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=HospitalDB;Trusted_Connection=yes;")
        },
        "sql_server_facilities": {
            "schema": fetch_sql_server_schema(os.getenv("SALES_DB_CONN") or "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=FacilityDB;Trusted_Connection=yes;"),
            "relationships": fetch_relationships("sql_server", os.getenv("SALES_DB_CONN") or "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=FacilityDB;Trusted_Connection=yes;")
        },
        "postgres_pharmacy": {
            "schema": fetch_postgres_schema(os.getenv("PG_DB_CONN")),
            "relationships": fetch_relationships("postgres", os.getenv("PG_DB_CONN"))
        },
        "mongodb_healthcare": {
            "schema": fetch_mongo_schema(os.getenv("MONGO_URI", "mongodb://localhost:27017/"), "HealthcareDB")
        }
    }

    # Save to file
    output_file = "db_metadata_dump.json"
    with open(output_file, "w") as f:
        json.dump(metadata, f, indent=4)
    
    print(f"Database metadata (schemas and relationships) successfully dumped to {output_file}")
    
    # Print summary
    print("\n--- Relationship Summary ---")
    for db, data in metadata.items():
        rels = data.get("relationships", [])
        print(f"{db}: {len(rels)} relationships found.")
        for r in rels:
            print(f"  - {r}")

if __name__ == "__main__":
    dump_db_metadata()
