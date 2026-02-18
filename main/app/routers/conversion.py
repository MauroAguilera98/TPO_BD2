from fastapi import APIRouter, HTTPException
from app.db.redis_client import redis_client
from app.db.mongo import grades_collection

router = APIRouter(prefix="/conversion", tags=["Conversion"])

def convert_numeric(grade: float, from_system: str, to_system: str) -> float:
    if from_system == "AR" and to_system == "US":
        return (grade / 10) * 4
    if from_system == "US" and to_system == "AR":
        return (grade / 4) * 10
    if from_system == "AR" and to_system == "DE":
        return 6 - (grade / 10 * 5)
    raise ValueError("Conversion rule not defined")

@router.post("/student")
def convert_student_term(req: dict):
    """
    Body ejemplo:
    {
      "student_id": "stu_001",
      "from_system": "AR",
      "to_system": "US",
      "year": 2025,
      "term": "S1"
    }
    """
    student_id = req.get("student_id")
    from_system = req.get("from_system")
    to_system = req.get("to_system")
    year = req.get("year")
    term = req.get("term")

    if not all([student_id, from_system, to_system, year, term]):
        raise HTTPException(400, "student_id, from_system, to_system, year, term are required")

    # Cache “por tramo”
    cache_key = f"conv:student:{student_id}:{year}:{term}:{from_system}:{to_system}"
    cached = redis_client.get(cache_key)
    if cached:
        return {"cached": True, "result": json.loads(cached)}

    # Buscamos notas de ese tramo (S1/S2)
    rows = grades_collection.find({
        "student_id": student_id,
        "country": from_system,
        "metadata.year": year,
        "metadata.term": term
    })

    out = []
    for doc in rows:
        val = doc.get("original_grade", {}).get("value")

        # Solo convertimos numéricas en este MVP
        try:
            num = float(val)
        except Exception:
            out.append({
                "grade_id": doc.get("grade_id"),
                "subject": doc.get("subject"),
                "original": val,
                "converted": None,
                "error": "non-numeric grade"
            })
            continue

        try:
            converted = convert_numeric(num, from_system, to_system)
        except ValueError as e:
            raise HTTPException(400, str(e))

        out.append({
            "grade_id": doc.get("grade_id"),
            "subject": doc.get("subject"),
            "original": num,
            "converted": round(converted, 2)
        })

    result = {
        "student_id": student_id,
        "year": year,
        "term": term,
        "from_system": from_system,
        "to_system": to_system,
        "items": out
    }

    redis_client.setex(cache_key, 3600, json.dumps(result))
    return {"cached": False, "result": result}
