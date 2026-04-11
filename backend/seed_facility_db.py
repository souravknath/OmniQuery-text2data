
import pyodbc
from faker import Faker
import random

fake = Faker()

master_conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=master;Trusted_Connection=yes;"
db_name = "FacilityDB"
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=(localdb)\\MSSQLLocalDB;DATABASE={db_name};Trusted_Connection=yes;"

def create_db():
    conn = pyodbc.connect(master_conn_str, autocommit=True)
    cursor = conn.cursor()
    cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{db_name}') CREATE DATABASE {db_name}")
    conn.close()

def seed_facility_db():
    create_db()
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Create Tables
        print("Ensuring Facilities table exists...")
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Facilities')
            BEGIN
                CREATE TABLE Facilities (
                    FacilityId INT PRIMARY KEY,
                    Name NVARCHAR(200),
                    Address NVARCHAR(255),
                    City NVARCHAR(100),
                    State NVARCHAR(100),
                    Country NVARCHAR(100),
                    ZipCode NVARCHAR(20)
                )
            END
        """)
        
        # Clear existing data
        print("Clearing existing Facility data...")
        cursor.execute("DELETE FROM Facilities")
        
        # Insert Facilities
        print("Inserting 10 facilities...")
        suffixes = ["General Hospital", "Medical Center", "Clinic", "Healthcare", "Specialty Care"]
        for i in range(10):
            name = f"{fake.city()} {random.choice(suffixes)}"
            cursor.execute(
                "INSERT INTO Facilities (FacilityId, Name, Address, City, State, Country, ZipCode) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (i + 1, name, fake.street_address(), fake.city(), fake.state(), "India", fake.postcode())
            )
            
        conn.commit()
        print(f"SUCCESS: {db_name} seeded successfully!")
        
    except Exception as e:
        print(f"ERROR seeding Facility DB: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    seed_facility_db()
