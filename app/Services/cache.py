import redis
import json

redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

CACHE_TTL = 60


def get_cache(key: str):
    data = redis_client.get(key)
    if data:
        return json.loads(data)
    return None


def set_cache(key: str, value):
    redis_client.setex(key, CACHE_TTL, json.dumps(value))