from pymongo import MongoClient
from faker import Faker
import random, uuid

fake = Faker()
client = MongoClient("mongodb://localhost:27017/")
db = client["CustomerDB"]

customers = db["customers"]
activities = db["activities"]
tickets = db["support_tickets"]

# Clear existing data
customers.delete_many({})
activities.delete_many({})
tickets.delete_many({})

print("Creating customers...")
def create_customer():
    cid = str(uuid.uuid4())
    return {
        "customer_id": cid,
        "profile": {
            "name": fake.name(),
            "age": random.randint(18, 65),
            "gender": random.choice(["Male", "Female"])
        },
        "location": {
            "city": fake.city(),
            "state": fake.state(),
            "country": "India"
        },
        "segments": random.sample(["Premium", "New", "Frequent Buyer", "Churn Risk"], 2),
        "financial": {
            "total_spent": random.randint(1000, 500000),
            "avg_order_value": random.randint(500, 5000)
        },
        "created_at": fake.date_time_this_decade(),
        "is_active": random.choice([True, False])
    }

# Insert Customers
customer_list = [create_customer() for _ in range(1000)]
customers.insert_many(customer_list)

# Activities
print("Creating activities batch 1...")
activity_list = []
total_customers = len(customer_list)
for i, c in enumerate(customer_list):
    if i % 10000 == 0 and i > 0:
        print(f"Processed {i}/{total_customers} customers...")
    for _ in range(random.randint(10, 20)):
        activity_list.append({
            "activity_id": str(uuid.uuid4()),
            "customer_id": c["customer_id"],
            "activity_type": random.choice(["View", "Cart", "Purchase"]),
            "product_category": random.choice(["Electronics", "Fashion", "Grocery"]),
            "amount": random.randint(100, 10000),
            "timestamp": fake.date_time_this_year()
        })
    if len(activity_list) > 100000:
        activities.insert_many(activity_list)
        activity_list = []

if activity_list:
    activities.insert_many(activity_list)

# Support Tickets
print("Creating support tickets...")
ticket_list = []
for c in customer_list[:300]:
    ticket_list.append({
        "ticket_id": str(uuid.uuid4()),
        "customer_id": c["customer_id"],
        "issue_type": random.choice(["Payment", "Delivery", "Refund"]),
        "status": random.choice(["Open", "Closed"]),
        "priority": random.choice(["Low", "Medium", "High"]),
        "created_at": fake.date_time_this_year()
    })
tickets.insert_many(ticket_list)

print("✅ MongoDB CustomerDB ready")
