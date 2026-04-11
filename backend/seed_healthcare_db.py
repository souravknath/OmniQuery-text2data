
from pymongo import MongoClient
from faker import Faker
import random, uuid
from datetime import datetime, timedelta

fake = Faker()
client = MongoClient("mongodb://localhost:27017/")
db = client["HealthcareDB"]

patients = db["patients"]
encounters = db["encounters"]

# Clear existing data
patients.delete_many({})
encounters.delete_many({})

print("Creating patients...")
def create_patient():
    pid = str(uuid.uuid4())
    return {
        "patient_id": pid,
        "profile": {
            "name": fake.name(),
            "age": random.randint(1, 90),
            "gender": random.choice(["Male", "Female", "Other"])
        },
        "clinical": {
            "blood_type": random.choice(["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]),
            "allergies": random.sample(["Peanuts", "Penicillin", "Latex", "Shellfish", "Dust", "Pollen"], random.randint(0, 3)),
            "chronic_conditions": random.sample(["Hypertension", "Diabetes", "Asthma", "Arthritis", "Thyroid"], random.randint(0, 2))
        },
        "facility_id": random.randint(1, 10),
        "is_active": True,
        "created_at": fake.date_time_this_decade()
    }

# Insert Patients
patient_list = [create_patient() for _ in range(500)]
patients.insert_many(patient_list)

# Encounters
print("Creating medical encounters...")
encounter_list = []
reasons = ["Regular Checkup", "Fever", "Back Pain", "Cough", "Headache", "Vaccination", "Injury", "Skin Rash"]
diagnoses = ["Healthy", "Viral Infection", "Muscle Strain", "Common Cold", "Migraine", "Allergy Flare-up", "Minor Wound"]
meds = ["Paracetamol", "Amoxicillin", "Ibuprofen", "Cetirizine", "Lisinopril", "Metformin", "Multivitamins"]

for p in patient_list:
    for _ in range(random.randint(1, 5)):
        encounter_list.append({
            "encounter_id": str(uuid.uuid4()),
            "patient_id": p["patient_id"],
            "reason": random.choice(reasons),
            "diagnosis": random.choice(diagnoses),
            "medications": random.sample(meds, random.randint(0, 3)),
            "timestamp": fake.date_time_this_year()
        })

encounters.insert_many(encounter_list)

print("MongoDB HealthcareDB ready")
