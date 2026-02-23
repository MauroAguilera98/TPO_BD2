import json
from app.db.redis_client import redis_client

CACHE_TTL = 1800

async def get_cache(key: str):
    # Ahora usamos await nativo
    data = await redis_client.get(key)
    if data:
        return json.loads(data)
    return None

async def set_cache(key: str, value):
    # Ahora usamos await nativo
    await redis_client.setex(key, CACHE_TTL, json.dumps(value))