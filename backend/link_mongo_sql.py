from pymongo import MongoClient
import random

def link_mongo_sql():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client["CustomerDB"]
        customers = db["customers"]
        
        print("Linking MongoDB customers to SQL Location IDs...")
        
        # We assume SQL locations have IDs 1-50
        all_customers = list(customers.find({}, {"_id": 1}))
        print(f"Total customers found: {len(all_customers)}")
        
        for doc in all_customers:
            loc_id = random.randint(1, 50)
            customers.update_one(
                {"_id": doc["_id"]},
                {"$set": {"location_id": loc_id}}
            )
            
        print("SUCCESS: MongoDB records now contain 'location_id' field linking to SQL Locations.")
        
    except Exception as e:
        print(f"ERROR: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    link_mongo_sql()
