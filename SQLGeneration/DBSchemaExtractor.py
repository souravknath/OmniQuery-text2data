import os
import json
import pymongo
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

def extract_schema(connection_string, output_file):
    engine = create_engine(connection_string)
    inspector = inspect(engine)

    schema = {
        "tables": {},
        "relationships": []
    }

    for table_name in inspector.get_table_names():
        table_info = {
            "columns": [],
            "primary_key": [],
            "foreign_keys": [],
            "indexes": []
        }

        # Columns
        columns = inspector.get_columns(table_name)
        for col in columns:
            table_info["columns"].append({
                "name": col["name"],
                "type": str(col["type"]),
                "nullable": col["nullable"],
                "default": str(col["default"]) if col["default"] else None
            })

        # Primary Keys
        pk = inspector.get_pk_constraint(table_name)
        table_info["primary_key"] = pk.get("constrained_columns", [])

        # Foreign Keys
        fks = inspector.get_foreign_keys(table_name)
        for fk in fks:
            fk_info = {
                "column": fk["constrained_columns"],
                "referred_table": fk["referred_table"],
                "referred_columns": fk["referred_columns"]
            }
            table_info["foreign_keys"].append(fk_info)

            # Add to global relationships
            schema["relationships"].append({
                "from_table": table_name,
                "from_column": fk["constrained_columns"],
                "to_table": fk["referred_table"],
                "to_column": fk["referred_columns"]
            })

        # Indexes
        indexes = inspector.get_indexes(table_name)
        for idx in indexes:
            table_info["indexes"].append({
                "name": idx["name"],
                "columns": idx["column_names"],
                "unique": idx["unique"]
            })

        schema["tables"][table_name] = table_info

    # Save to JSON
    with open(output_file, "w") as f:
        json.dump(schema, f, indent=4)

    print(f"Schema extracted successfully to {output_file}")

def extract_mongo_schema(uri, db_name, output_file):
    client = pymongo.MongoClient(uri)
    db = client[db_name]
    
    schema = {
        "collections": {}
    }
    
    for collection_name in db.list_collection_names():
        collection = db[collection_name]
        sample_doc = collection.find_one()
        fields = []
        if sample_doc:
            for key, value in sample_doc.items():
                fields.append({
                    "name": key,
                    "type": type(value).__name__
                })
        
        schema["collections"][collection_name] = {
            "fields": fields
        }
        
    with open(output_file, "w") as f:
        json.dump(schema, f, indent=4)
        
    print(f"Mongo schema extracted successfully to {output_file}")


if __name__ == "__main__":
    load_dotenv()

    # Ensure DBSchemas directory exists
    os.makedirs("DBSchemas", exist_ok=True)

    # PostgreSQL
    import urllib.parse
    pg_user = os.getenv("POSTGRES_USER")
    pg_pass = os.getenv("POSTGRES_PASSWORD")
    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = os.getenv("POSTGRES_PORT")
    pg_db = os.getenv("POSTGRES_DB")

    if pg_user and pg_pass and pg_host and pg_port and pg_db:
        password = urllib.parse.quote_plus(pg_pass)
        conn_str = f"postgresql://{pg_user}:{password}@{pg_host}:{pg_port}/{pg_db}"
        extract_schema(conn_str, output_file="DBSchemas/Postgres_Sales_DB_Schema.json")

    # SQL Server
    sql_conn_str = os.getenv("SQLSERVER_CONNECTION_STRING")
    if sql_conn_str:
        params = urllib.parse.quote_plus(sql_conn_str)
        conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
        extract_schema(conn_str, output_file="DBSchemas/SQL_Inventory_DB_Schema.json")

    # MongoDB
    mongo_uri = os.getenv("MONGO_URI")
    mongo_db_name = os.getenv("MONGO_DB")
    if mongo_uri and mongo_db_name:
        extract_mongo_schema(mongo_uri, mongo_db_name, "DBSchemas/Mongo_Customer_DB_Schema.json")