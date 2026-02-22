from fastapi import APIRouter, HTTPException
from fastapi.concurrency import run_in_threadpool
from app.db.redis_client import redis_client
from app.audit.audit_service import AuditService
from uuid import uuid4

router = APIRouter(prefix="/conversion", tags=["Conversion"])

# --- MAPEOS ESTÁTICOS ---
UK_MAP = {"A*": 10.0, "A": 9.0, "B": 8.0, "C": 7.0, "D": 6.0, "E": 5.0, "F": 4.0}
US_LETTER_MAP = {"A": 4.0, "B": 3.0, "C": 2.0, "D": 1.0, "F": 0.0}



def to_standard_ar(grade: str, system: str) -> float:
    """Convierte la entrada al pivote AR (0-10) asumiendo GPA para US."""
    system = system.upper()
    try:
        # Intento de conversión numérica directa
        val = float(grade)
        
        if system == "US":
            # Cálculo directo de GPA: (Valor / 4.0) * 10
            return (val / 4.0) * 10.0
        
        if system == "DE":
            # Escala inversa alemana
            return 10.0 - ((val - 1.0) * 2.0)
            
        return val

    except ValueError:
        # Manejo de letras si no es un número
        grade_up = grade.upper()
        if system == "US" and grade_up in US_LETTER_MAP:
            return (US_LETTER_MAP[grade_up] / 4.0) * 10.0
        if system == "UK" and grade_up in UK_MAP:
            return UK_MAP[grade_up]
            
        raise ValueError(f"No se pudo procesar la nota '{grade}'")

def from_standard_ar(val: float, system: str) -> str:
    """Convierte desde el pivote AR al sistema destino."""
    system = system.upper()
    if system == "US":
        return str(round((val / 10.0) * 4.0, 2))
    if system == "DE":
        return str(round(6.0 - (val / 10.0 * 5.0), 1))
    return str(round(val, 2))

@router.post("/")
async def convert_grade(grade: str, from_system: str, to_system: str):

    cache_key = f"conv:{from_system}:{to_system}:{grade}".upper()

    # 1. Redis
    cached = await run_in_threadpool(redis_client.get, cache_key)

    if cached:
        return {"converted": cached, "cached": True}

    # 2. Conversión
    try:
        standard_val = to_standard_ar(grade, from_system)
        result = from_standard_ar(standard_val, to_system)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # 3. Guardar cache
    await run_in_threadpool(redis_client.setex, cache_key, 3600, str(result))

    # 4. AUDITORÍA
    conversion_id = str(uuid4())

    AuditService.register_event(
        entity_type="conversion",
        entity_id=conversion_id,
        action="GRADE_CONVERSION",
        actor="system",
        payload={
            "grade": grade,
            "from": from_system,
            "to": to_system,
            "result": result
        }
    )

    return {
        "conversion_id": conversion_id,
        "converted": result,
        "from": from_system.upper(),
        "to": to_system.upper(),
        "cached": False
    }