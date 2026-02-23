import os
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_URI = f"mongodb://{MONGO_HOST}:27017"

client = AsyncIOMotorClient(MONGO_URI)
db = client["edugrade"]
grades_collection = db["grades"]
students_collection = db["students"]
institutions_collection = db["institutions"]
subjects_collection = db["subjects"]

async def init_mongo_indices():
    # Garantiza búsquedas O(1) para el millón de registros
    await grades_collection.create_index([("student_id", 1), ("issued_at", -1)])
    await grades_collection.create_index("correction_of")
    await institutions_collection.create_index("country")
    await institutions_collection.create_index("is_active")
    await institutions_collection.create_index("name")
    await subjects_collection.create_index("institution_id")
    await subjects_collection.create_index("kind")
    await subjects_collection.create_index("is_active")
    await subjects_collection.create_index([("institution_id", 1), ("name", 1)])
    print("✅ Índices de MongoDB verificados/creados.")

# La creación de índices en Motor es asíncrona, debe hacerse en el arranque de la app,
# pero para evitar fallos de importación ahora, lo removemos de este archivo a nivel global.

def close_mongo():
    """Cierra el cliente de MongoDB (síncrono por diseño de la librería)."""
    client.close()
    print("🔌 Conexión a MongoDB cerrada correctamente.")