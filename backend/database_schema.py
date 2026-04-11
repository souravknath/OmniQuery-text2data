
import json

# MongoDB HealthcareDB Metadata (NoSQL)
MONGODB_HEALTHCARE_DB_SCHEMA = {
    "patients": {
        "description": "Patient profiles and clinical status.",
        "fields": {
            "patient_id": "Unique UUID for the patient",
            "profile": {
                "name": "Full name",
                "age": "Age in years",
                "gender": "Male or Female"
            },
            "clinical": {
                "blood_type": "A+, O-, etc.",
                "allergies": "LIST of allergy strings",
                "chronic_conditions": "LIST of existing conditions"
            },
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

MONGODB_HEALTHCARE_DB_SAMPLES = {
    "patients": [
        {
            "patient_id": "p123-abc-456",
            "profile": {"name": "Suresh Raina", "age": 45, "gender": "Male"},
            "clinical": {
                "blood_type": "B+", 
                "allergies": ["Peanuts", "Penicillin"],
                "chronic_conditions": ["Hypertension"]
            },
            "facility_id": 1,
            "is_active": True
        }
    ],
    "encounters": [
        {
            "patient_id": "p123-abc-456",
            "reason": "Annual Checkup",
            "diagnosis": "Healthy",
            "medications": ["Vitamin D3"],
            "timestamp": "2024-05-10T09:00:00Z"
        }
    ]
}

# SQL Server Hospital Metadata
SQL_HOSPITAL_DB_SCHEMA = {
    "Doctors": {
        "columns": ["DoctorId (UUID)", "FirstName (NVARCHAR)", "LastName (NVARCHAR)", "Specialization (NVARCHAR)", "EmailId (NVARCHAR)", "FacilityId (INT)"],
        "relationships": "FacilityId links to FacilityDB.dbo.Facilities.FacilityId"
    },
    "Appointments": {
        "columns": ["AppointmentId (UUID)", "DoctorId (UUID)", "PatientId (UUID or STRING)", "AppointmentDate (DATETIME)", "Status (NVARCHAR)"]
    }
}

SQL_HOSPITAL_DB_SAMPLES = {
    "Doctors": [
        {"DoctorId": "d1-uuid", "FirstName": "Anita", "LastName": "Desai", "Specialization": "Cardiology", "EmailId": "dr.anita@hospital.com", "FacilityId": 1}
    ],
    "Appointments": [
        {"AppointmentId": "a1-uuid", "DoctorId": "d1-uuid", "PatientId": "p123-abc-456", "AppointmentDate": "2024-06-15 11:30:00", "Status": "Scheduled"}
    ]
}

# SQL Server Facilities Metadata
SQL_FACILITIES_DB_SCHEMA = {
    "Facilities": {
        "columns": ["FacilityId (INT)", "Name (NVARCHAR)", "Address (NVARCHAR)", "City (NVARCHAR)", "State (NVARCHAR)", "Country (NVARCHAR)", "ZipCode (NVARCHAR)"]
    }
}

SQL_FACILITIES_DB_SAMPLES = {
    "Facilities": [
        {"FacilityId": 1, "Name": "Appolo Hospital", "Address": "45 Bannerghatta Rd", "City": "Bangalore", "State": "Karnataka", "Country": "India", "ZipCode": "560076"}
    ]
}

# PostgreSQL Pharmacy Metadata
POSTGRES_PHARMACY_DB_SCHEMA = {
    "Medicines": {
        "columns": ["medicine_id (SERIAL)", "name (VARCHAR)", "manufacturer (VARCHAR)", "price (DECIMAL)", "inventory_count (INT)", "category (VARCHAR)"],
        "description": "Stock of medicines available in the pharmacy inventory."
    },
    "Prescriptions": {
        "columns": ["prescription_id (UUID)", "patient_id (UUID/STRING)", "doctor_id (UUID)", "medicine_id (INT)", "dosage (VARCHAR)", "issued_date (DATE)"],
        "relationships": "Links Patients (Mongo), Doctors (SQL), and Medicines (PG)."
    }
}

POSTGRES_PHARMACY_DB_SAMPLES = {
    "Medicines": [
        {"medicine_id": 101, "name": "Amoxicillin", "manufacturer": "Pfizer", "price": 450.00, "inventory_count": 1500, "category": "Antibiotic"}
    ],
    "Prescriptions": [
        {"prescription_id": "pr-uuid", "patient_id": "p123-abc-456", "doctor_id": "d1-uuid", "medicine_id": 101, "dosage": "500mg twice daily", "issued_date": "2024-05-12"}
    ]
}
