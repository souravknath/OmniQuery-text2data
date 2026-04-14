import os
import json
from dotenv import load_dotenv
from schema_fetcher import (
    fetch_sql_server_schema, 
    fetch_postgres_schema, 
    fetch_mongo_schema, 
    fetch_relationships,
    fetch_samples
)

# Load environment variables
load_dotenv()

# --- Connection Strings ---
HR_DB_CONN = os.getenv("HR_DB_CONN")
# If env is sqlite, we might need to use the ODBC string if we're on localdb
if not HR_DB_CONN or HR_DB_CONN.startswith("sqlite"):
    HR_DB_CONN = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=HospitalDB;Trusted_Connection=yes;"

SALES_DB_CONN = os.getenv("SALES_DB_CONN")
if not SALES_DB_CONN or SALES_DB_CONN.startswith("sqlite"):
    SALES_DB_CONN = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=FacilityDB;Trusted_Connection=yes;"

PG_DB_CONN = os.getenv("PG_DB_CONN")
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")

# --- MongoDB HealthcareDB Metadata (NoSQL) ---
print("Fetching MongoDB schema...")
_MONGODB_HEALTHCARE_DB_SCHEMA_DYNAMIC = fetch_mongo_schema(MONGO_URI, "HealthcareDB")
MONGODB_HEALTHCARE_DB_SCHEMA = _MONGODB_HEALTHCARE_DB_SCHEMA_DYNAMIC if _MONGODB_HEALTHCARE_DB_SCHEMA_DYNAMIC else {
    "patients": {
        "description": "Patient profiles and clinical status.",
        "fields": {
            "patient_id": "Unique UUID for the patient",
            "profile": {"name": "Full name", "age": "Age in years", "gender": "Male or Female"},
            "clinical": {"blood_type": "A+, O-, etc.", "allergies": "LIST of allergy strings", "chronic_conditions": "LIST of existing conditions"},
            "facility_id": "INT - links to SQL Facilities.dbo.Facilities.FacilityId",
            "is_active": "Boolean status of patient"
        }
    },
    "encounters": {
        "description": "Record of doctor-patient interactions and visits.",
        "fields": {
            "encounter_id": "Unique UUID",
            "patient_id": "Reference to patients.patient_id",
            "reason": "Primary complaint or reason for visit",
            "diagnosis": "Clinical diagnosis codes or text",
            "medications": "LIST of prescribed drugs",
            "timestamp": "ISO Date of encounter"
        }
    }
}

_MONGODB_HEALTHCARE_DB_SAMPLES_DYNAMIC = fetch_samples("mongo", MONGO_URI, MONGODB_HEALTHCARE_DB_SCHEMA, "HealthcareDB")
MONGODB_HEALTHCARE_DB_SAMPLES = _MONGODB_HEALTHCARE_DB_SAMPLES_DYNAMIC if _MONGODB_HEALTHCARE_DB_SAMPLES_DYNAMIC else {
    "patients": [{"patient_id": "p123-abc-456", "profile": {"name": "Suresh Raina", "age": 45, "gender": "Male"}, "clinical": {"blood_type": "B+", "allergies": ["Peanuts", "Penicillin"], "chronic_conditions": ["Hypertension"]}, "facility_id": 1, "is_active": True}],
    "encounters": [{"patient_id": "p123-abc-456", "reason": "Annual Checkup", "diagnosis": "Healthy", "medications": ["Vitamin D3"], "timestamp": "2024-05-10T09:00:00Z"}]
}

# --- SQL Server Hospital Metadata ---
print("Fetching SQL Server Hospital schema...")
_SQL_HOSPITAL_DB_SCHEMA_DYNAMIC = fetch_sql_server_schema(HR_DB_CONN)
_SQL_HOSPITAL_RELATIONSHIPS = fetch_relationships("sql_server", HR_DB_CONN)

SQL_HOSPITAL_DB_SCHEMA = _SQL_HOSPITAL_DB_SCHEMA_DYNAMIC if _SQL_HOSPITAL_DB_SCHEMA_DYNAMIC else {
    "Doctors": {
        "columns": ["DoctorId (UUID)", "FirstName (NVARCHAR)", "LastName (NVARCHAR)", "Specialization (NVARCHAR)", "EmailId (NVARCHAR)", "FacilityId (INT)"],
        "relationships": "FacilityId links to FacilityDB.dbo.Facilities.FacilityId"
    },
    "Appointments": {
        "columns": ["AppointmentId (UUID)", "DoctorId (UUID)", "PatientId (UUID or STRING)", "AppointmentDate (DATETIME)", "Status (NVARCHAR)"]
    }
}
if _SQL_HOSPITAL_RELATIONSHIPS:
    SQL_HOSPITAL_DB_SCHEMA["_metadata"] = {"relationships": _SQL_HOSPITAL_RELATIONSHIPS}

_SQL_HOSPITAL_DB_SAMPLES_DYNAMIC = fetch_samples("sql_server", HR_DB_CONN, SQL_HOSPITAL_DB_SCHEMA)
SQL_HOSPITAL_DB_SAMPLES = _SQL_HOSPITAL_DB_SAMPLES_DYNAMIC if _SQL_HOSPITAL_DB_SAMPLES_DYNAMIC else {
    "Doctors": [{"DoctorId": "d1-uuid", "FirstName": "Anita", "LastName": "Desai", "Specialization": "Cardiology", "EmailId": "dr.anita@hospital.com", "FacilityId": 1}],
    "Appointments": [{"AppointmentId": "a1-uuid", "DoctorId": "d1-uuid", "PatientId": "p123-abc-456", "AppointmentDate": "2024-06-15 11:30:00", "Status": "Scheduled"}]
}

# --- SQL Server Facilities Metadata ---
print("Fetching SQL Server Facilities schema...")
_SQL_FACILITIES_DB_SCHEMA_DYNAMIC = fetch_sql_server_schema(SALES_DB_CONN)
_SQL_FACILITIES_RELATIONSHIPS = fetch_relationships("sql_server", SALES_DB_CONN)

SQL_FACILITIES_DB_SCHEMA = _SQL_FACILITIES_DB_SCHEMA_DYNAMIC if _SQL_FACILITIES_DB_SCHEMA_DYNAMIC else {
    "Facilities": {
        "columns": ["FacilityId (INT)", "Name (NVARCHAR)", "Address (NVARCHAR)", "City (NVARCHAR)", "State (NVARCHAR)", "Country (NVARCHAR)", "ZipCode (NVARCHAR)"]
    }
}
if _SQL_FACILITIES_RELATIONSHIPS:
    SQL_FACILITIES_DB_SCHEMA["_metadata"] = {"relationships": _SQL_FACILITIES_RELATIONSHIPS}

_SQL_FACILITIES_DB_SAMPLES_DYNAMIC = fetch_samples("sql_server", SALES_DB_CONN, SQL_FACILITIES_DB_SCHEMA)
SQL_FACILITIES_DB_SAMPLES = _SQL_FACILITIES_DB_SAMPLES_DYNAMIC if _SQL_FACILITIES_DB_SAMPLES_DYNAMIC else {
    "Facilities": [{"FacilityId": 1, "Name": "Appolo Hospital", "Address": "45 Bannerghatta Rd", "City": "Bangalore", "State": "Karnataka", "Country": "India", "ZipCode": "560076"}]
}

# --- PostgreSQL Pharmacy Metadata ---
print("Fetching PostgreSQL Pharmacy schema...")
_POSTGRES_PHARMACY_DB_SCHEMA_DYNAMIC = fetch_postgres_schema(PG_DB_CONN)
_POSTGRES_PHARMACY_RELATIONSHIPS = fetch_relationships("postgres", PG_DB_CONN)

POSTGRES_PHARMACY_DB_SCHEMA = _POSTGRES_PHARMACY_DB_SCHEMA_DYNAMIC if _POSTGRES_PHARMACY_DB_SCHEMA_DYNAMIC else {
    "Medicines": {
        "columns": ["medicine_id (SERIAL)", "name (VARCHAR)", "manufacturer (VARCHAR)", "price (DECIMAL)", "inventory_count (INT)", "category (VARCHAR)"],
        "description": "Stock of medicines available in the pharmacy inventory."
    },
    "Prescriptions": {
        "columns": ["prescription_id (UUID)", "patient_id (UUID/STRING)", "doctor_id (UUID)", "medicine_id (INT)", "dosage (VARCHAR)", "issued_date (DATE)"],
        "relationships": "Links Patients (Mongo), Doctors (SQL), and Medicines (PG)."
    }
}
if _POSTGRES_PHARMACY_RELATIONSHIPS:
    POSTGRES_PHARMACY_DB_SCHEMA["_metadata"] = {"relationships": _POSTGRES_PHARMACY_RELATIONSHIPS}


if __name__ == "__main__":
    # Internal test print
    print("Database Schema Module Loaded.")
