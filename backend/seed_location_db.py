import pyodbc
from faker import Faker
import random

fake = Faker()

# Connection string for Location database
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=Location;Trusted_Connection=yes;"

def seed_location_db():
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Create table if not exists
        print("Ensuring Locations table exists...")
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Locations')
            BEGIN
                CREATE TABLE Locations (
                    LocationId INT PRIMARY KEY,
                    Address NVARCHAR(255),
                    City NVARCHAR(100),
                    State NVARCHAR(100),
                    Country NVARCHAR(100),
                    ZipCode NVARCHAR(20)
                )
            END
        """)
        
        # Clear existing data
        print("Clearing existing Location data...")
        cursor.execute("DELETE FROM Locations")
        
        # Insert Locations
        print("Inserting 50 locations...")
        for i in range(50):
            cursor.execute(
                "INSERT INTO Locations (LocationId, Address, City, State, Country, ZipCode) VALUES (?, ?, ?, ?, ?, ?)",
                (i + 1, fake.street_address(), fake.city(), fake.state(), "India", fake.postcode())
            )
            
        conn.commit()
        print("SUCCESS: Location database seeded successfully!")
        
    except Exception as e:
        print(f"ERROR seeding Location DB: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    seed_location_db()
