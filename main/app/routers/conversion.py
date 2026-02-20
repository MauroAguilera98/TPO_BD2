from fastapi import APIRouter
from app.db.redis_client import redis_client

router = APIRouter(prefix="/conversion", tags=["Conversion"])

# Conversión simple ejemplo
@router.post("/")
def convert_grade(
    grade: float,
    from_system: str,
    to_system: str
):

    cache_key = f"conversion:{from_system}:{to_system}:{grade}"

    # Buscar en cache
    cached = redis_client.get(cache_key)
    if cached:
        return {
            "converted": float(cached),
            "cached": True
        }

    # Lógica ejemplo
    converted = 0

    if from_system == "AR" and to_system == "US":
        converted = (grade / 10) * 4

    elif from_system == "US" and to_system == "AR":
        converted = (grade / 4) * 10

    elif from_system == "AR" and to_system == "DE":
        # Escala alemana inversa 1.0–6.0
        converted = 6 - (grade / 10 * 5)

    else:
        return {"error": "Conversion rule not defined"}

    # Guardar en cache por 1 hora
    redis_client.setex(cache_key, 3600, converted)

    return {
        "converted": round(converted, 2),
        "cached": False
    }
