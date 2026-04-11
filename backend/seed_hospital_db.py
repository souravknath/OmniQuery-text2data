import pyodbc
import uuid
import random
from faker import Faker
from pymongo import MongoClient
import os
from dotenv import load_dotenv

load_dotenv()

fake = Faker()

# Master connection to create databases
master_conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=master;Trusted_Connection=yes;"
db_name = "HospitalDB"
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER=(localdb)\\MSSQLLocalDB;DATABASE={db_name};Trusted_Connection=yes;"

def get_real_patient_ids():
    try:
        mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        client = MongoClient(mongo_uri)
        db = client["HealthcareDB"]
        patients = db["patients"]
        ids = [p["patient_id"] for p in patients.find({}, {"patient_id": 1}).limit(200)]
        client.close()
        return ids
    except Exception as e:
        print(f"Warning: Could not fetch real patient IDs from Mongo: {e}")
        return [str(uuid.uuid4()) for _ in range(50)]

def create_db():
    conn = pyodbc.connect(master_conn_str, autocommit=True)
    cursor = conn.cursor()
    cursor.execute(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{db_name}') CREATE DATABASE {db_name}")
    conn.close()

def seed_hospital_db():
    create_db()
    patient_ids = get_real_patient_ids()
    print(f"Fetched {len(patient_ids)} real patient IDs from MongoDB.")
    
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Create Tables
        print("Ensuring tables exist...")
        cursor.execute("""
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Doctors')
            BEGIN
                CREATE TABLE Doctors (
                    DoctorId UNIQUEIDENTIFIER PRIMARY KEY,
                    FirstName NVARCHAR(100),
                    LastName NVARCHAR(100),
                    Specialization NVARCHAR(100),
                    EmailId NVARCHAR(255),
                    FacilityId INT
                )
            END
            
            IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'Appointments')
            BEGIN
                CREATE TABLE Appointments (
                    AppointmentId UNIQUEIDENTIFIER PRIMARY KEY,
                    DoctorId UNIQUEIDENTIFIER,
                    PatientId NVARCHAR(100),
                    AppointmentDate DATETIME,
                    Status NVARCHAR(50)
                )
            END
        """)
        
        # Clear existing data
        print("Clearing existing Hospital data...")
        cursor.execute("DELETE FROM Appointments")
        cursor.execute("DELETE FROM Doctors")
        
        # Insert Doctors
        print("Inserting 20 doctors...")
        specs = ["Cardiology", "Neurology", "Pediatrics", "Oncology", "Orthopedics", "Dermatology", "General Medicine"]
        doctors = []
        for _ in range(20):
            did = uuid.uuid4()
            fname = fake.first_name()
            lname = fake.last_name()
            spec = random.choice(specs)
            email = f"dr.{fname.lower()}@healthcare.com"
            cursor.execute(
                "INSERT INTO Doctors (DoctorId, FirstName, LastName, Specialization, EmailId, FacilityId) VALUES (?, ?, ?, ?, ?, ?)",
                (did, fname, lname, spec, email, random.randint(1, 10))
            )
            doctors.append(did)
            
        # Insert Appointments
        print(f"Inserting {len(patient_ids)} linked appointments...")
        statuses = ["Scheduled", "Completed", "Cancelled", "Pending"]
        
        for pid in patient_ids:
            aid = uuid.uuid4()
            did = random.choice(doctors)
            adate = fake.date_time_this_year()
            status = random.choice(statuses)
            cursor.execute(
                "INSERT INTO Appointments (AppointmentId, DoctorId, PatientId, AppointmentDate, Status) VALUES (?, ?, ?, ?, ?)",
                (aid, did, pid, adate, status)
            )
            
        conn.commit()
        print(f"SUCCESS: {db_name} seeded successfully with linked data!")
        
    except Exception as e:
        print(f"ERROR seeding Hospital DB: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    seed_hospital_db()
