import os
from motor.motor_asyncio import AsyncIOMotorClient

MONGO_HOST = os.getenv("MONGO_HOST", "mongo")
MONGO_URI = f"mongodb://{MONGO_HOST}:27017"

client = AsyncIOMotorClient(MONGO_URI)
db = client["edugrade"]
grades_collection = db["grades"]
students_collection = db["students"]

async def init_mongo_indices():
    # Garantiza búsquedas O(1) para el millón de registros
    await grades_collection.create_index("grade_id", unique=True)
    await students_collection.create_index("student_id", unique=True)
    print("✅ Índices de MongoDB verificados/creados.")

# La creación de índices en Motor es asíncrona, debe hacerse en el arranque de la app,
# pero para evitar fallos de importación ahora, lo removemos de este archivo a nivel global.

def close_mongo():
    """Cierra el cliente de MongoDB (síncrono por diseño de la librería)."""
    client.close()
    print("🔌 Conexión a MongoDB cerrada correctamente.")