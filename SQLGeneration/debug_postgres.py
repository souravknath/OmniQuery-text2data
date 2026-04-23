import psycopg2, psycopg2.extras
import os
from dotenv import load_dotenv

load_dotenv()

def check_overlap():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    # Check total orders
    cur.execute('SELECT COUNT(*) FROM "Order"')
    print("Total Orders:", cur.fetchone())
    
    # Check total order items
    cur.execute('SELECT COUNT(*) FROM order_items')
    print("Total Order Items:", cur.fetchone())
    
    # Check customer_id range in Order
    cur.execute('SELECT MIN(customer_id), MAX(customer_id) FROM "Order"')
    print("Customer ID range in Order:", cur.fetchone())
    
    conn.close()

if __name__ == "__main__":
    check_overlap()
