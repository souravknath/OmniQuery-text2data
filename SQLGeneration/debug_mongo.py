import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

def check_mongo():
    client = pymongo.MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    db = client[os.getenv("MONGO_DB", "CustomerDB")]
    print("Collections:", db.list_collection_names())
    
    for coll_name in db.list_collection_names():
        print(f"\nSample from {coll_name}:")
        print(db[coll_name].find_one())

if __name__ == "__main__":
    check_mongo()
