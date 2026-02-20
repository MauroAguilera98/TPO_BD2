from pymongo import MongoClient
import os

# client = MongoClient("mongodb://localhost:27017")
# 
MONGO_HOST = os.getenv("MONGO_HOST", "mongo")

client = MongoClient(f"mongodb://{MONGO_HOST}:27017")

db = client["edugrade"]
grades_collection = db["grades"]

grades_collection.create_index("student_id")