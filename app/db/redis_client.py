import os
import redis.asyncio as redis # Cambio fundamental a la variante asíncrona

REDIS_HOST = os.getenv("REDIS_HOST", "redis")

# Instancia asíncrona. Ya no bloquea la API.
redis_client = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    decode_responses=True
)

async def close_redis():
    """Libera los sockets asíncronos de Redis."""
    await redis_client.aclose()
    print("🔌 Conexión a Redis cerrada correctamente.")