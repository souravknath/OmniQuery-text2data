import pyodbc
import uuid
import random
from faker import Faker

fake = Faker()

# Connection string
conn_str = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=(localdb)\\MSSQLLocalDB;DATABASE=Users;Trusted_Connection=yes;"

def seed_sql_db():
    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
        
        # Clear existing data
        print("Clearing existing SQL data...")
        cursor.execute("DELETE FROM User_Orders")
        cursor.execute("DELETE FROM Orders")
        cursor.execute("DELETE FROM Users")
        
        # Insert Users
        print("Inserting 50 users...")
        users = []
        for _ in range(50):
            userid = uuid.uuid4()
            fname = fake.first_name()
            lname = fake.last_name()
            uname = f"{fname.lower()}.{lname.lower()}_{random.randint(10,99)}"
            email = f"{uname}@example.com"
            cursor.execute(
                "INSERT INTO Users (UserId, FirstName, LastName, EmailId, UserName) VALUES (?, ?, ?, ?, ?)",
                (userid, fname, lname, email, uname)
            )
            users.append(userid)
            
        # Insert Orders
        print("Inserting 100 orders...")
        orders = []
        for _ in range(100):
            orderid = uuid.uuid4()
            product = random.choice(["Laptop", "Phone", "Monitor", "Keyboard", "Mouse", "Desk", "Chair", "Headphones"])
            oname = f"{product} - {fake.word().capitalize()}"
            amount = round(random.uniform(20.0, 2000.0), 2)
            odate = fake.date_time_this_year()
            cursor.execute(
                "INSERT INTO Orders (OrderId, OrderName, Amount, OrderDate) VALUES (?, ?, ?, ?)",
                (orderid, oname, amount, odate)
            )
            orders.append(orderid)
            
        # Map Users to Orders
        print("Mapping users to orders...")
        for i, orderid in enumerate(orders):
            userid = random.choice(users)
            # Id is not identity, so we provide i+1
            cursor.execute(
                "INSERT INTO User_Orders (Id, UserId, OrderId) VALUES (?, ?, ?)",
                (i + 1, userid, orderid)
            )
            
        conn.commit()
        print("SUCCESS: SQL Database users/orders seeded successfully!")
        
    except Exception as e:
        print(f"ERROR seeding SQL DB: {e}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    seed_sql_db()
