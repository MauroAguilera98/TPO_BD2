from pymongo import MongoClient

client = MongoClient("mongodb://mongo:27017")
db = client["edugrade"]
grades_collection = db["grades"]

