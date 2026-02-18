from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017")
db = client["edugrade"]
grades_collection = db["grades"]

grades_collection.create_index("student_id")
