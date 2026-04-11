
import psycopg2
from psycopg2 import sql
import uuid
import random
from faker import Faker
from datetime import date

fake = Faker()

# Connection to default postgres DB to create PharmacyDB
conn_params = "dbname=postgres user=postgres password=postgres host=localhost port=5432"
target_db = "pharmacy_db" # Changed to underscore to be safe with PG naming

def create_database():
    try:
        conn = psycopg2.connect(conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{target_db}'")
        exists = cursor.fetchone()
        if not exists:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(target_db)))
            print(f"Database {target_db} created.")
        else:
            print(f"Database {target_db} already exists.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Note: Could not create database (may already exist or permission denied): {e}")

def seed_pharmacy_db():
    create_database()
    
    # Connect to target DB
    target_conn_params = f"dbname={target_db} user=postgres password=postgres host=localhost port=5432"
    try:
        conn = psycopg2.connect(target_conn_params)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create Tables
        print("Creating tables...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS medicines (
                medicine_id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                manufacturer VARCHAR(200),
                price DECIMAL(10, 2),
                inventory_count INT,
                category VARCHAR(100)
            );
            
            CREATE TABLE IF NOT EXISTS prescriptions (
                prescription_id UUID PRIMARY KEY,
                patient_id VARCHAR(100),
                doctor_id UUID,
                medicine_id INT REFERENCES medicines(medicine_id),
                dosage VARCHAR(200),
                issued_date DATE
            );
        """)
        
        # Clear data
        cursor.execute("TRUNCATE TABLE prescriptions CASCADE; TRUNCATE TABLE medicines CASCADE;")
        
        # Insert Medicines
        print("Seeding medicines...")
        med_names = [
            ("Amoxicillin", "Antibiotic"), ("Metformin", "Diabetes"), ("Lisinopril", "Hypertension"),
            ("Atorvastatin", "Cholesterol"), ("Albuterol", "Asthma"), ("Omeprazole", "Gastro"),
            ("Sertraline", "Antidepressant"), ("Gabapentin", "Nerve Pain"), ("Levothyroxine", "Thyroid"),
            ("Amlodipine", "Blood Pressure")
        ]
        
        for name, cat in med_names:
            cursor.execute(
                "INSERT INTO medicines (name, manufacturer, price, inventory_count, category) VALUES (%s, %s, %s, %s, %s)",
                (name, fake.company(), round(random.uniform(10.0, 500.0), 2), random.randint(100, 2000), cat)
            )
            
        # Get medicine IDs
        cursor.execute("SELECT medicine_id FROM medicines")
        medicine_ids = [row[0] for row in cursor.fetchall()]
        
        # Insert Prescriptions
        print("Seeding prescriptions...")
        for _ in range(30):
            cursor.execute(
                "INSERT INTO prescriptions (prescription_id, patient_id, doctor_id, medicine_id, dosage, issued_date) VALUES (%s, %s, %s, %s, %s, %s)",
                (str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4()), random.choice(medicine_ids), f"{random.randint(1,3)} times a day", fake.date_this_year())
            )
            
        print("PostgreSQL PharmacyDB seeded successfully!")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"ERROR seeding PostgreSQL: {e}")

if __name__ == "__main__":
    seed_pharmacy_db()
