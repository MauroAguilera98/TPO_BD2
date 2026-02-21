import os
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_URI = f"mongodb://{MONGO_HOST}:27017"

client = AsyncIOMotorClient(MONGO_URI)
db = client["edugrade"]
grades_collection = db["grades"]

# La creación de índices en Motor es asíncrona, debe hacerse en el arranque de la app,
# pero para evitar fallos de importación ahora, lo removemos de este archivo a nivel global.