from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from app.db.cassandra import session
from app.Services.cache import get_cache, set_cache


router = APIRouter(prefix="/reports", tags=["Reports"])



# 1. Promedio delegando el cálculo matemático a Cassandra
@router.get("/average/{country}/{year}")
async def get_country_average(country: str, year: int):
    # Cassandra suma y cuenta nativamente en C++, devolviendo solo 1 fila a Python
    query = """
        SELECT AVG(grade) as avg_grade, COUNT(grade) as total 
        FROM grades_by_country_year 
        WHERE country=%s AND year=%s
    """
    row = await run_in_threadpool(lambda: session.execute(query, (country, year)).one())

    if not row or row.total == 0:
        return {"country": country, "year": year, "average": None}

    return {
        "country": country,
        "year": year,
        "average": round(row.avg_grade, 2),
        "total_records": row.total
    }

# 2. Top 10 agrupando nativamente (Si Cassandra > 3.0)
@router.get("/top10/{country}/{year}")
async def top_10_students(country: str, year: int):
    # Agrupamos por student_id dentro de la partición. 
    # Python recibe una fracción mínima de los datos.
    query = """
        SELECT student_id, AVG(grade) as student_avg 
        FROM grades_by_country_year 
        WHERE country=%s AND year=%s 
        GROUP BY student_id
    """
    # Ejecutamos en threadpool y convertimos a lista
    rows = await run_in_threadpool(lambda: list(session.execute(query, (country, year))))

    # Cassandra no ordena por agregaciones, ordenamos en Python (ahora sí es seguro porque hay poca data)
    top10 = sorted(rows, key=lambda x: x.student_avg, reverse=True)[:10]

    return {
        "country": country,
        "year": year,
        "top10": [
            {"student_id": row.student_id, "average": round(row.student_avg, 2)}
            for row in top10
        ]
    }

# 3. Distribución (Requiere iteración porque 'grade' no es clave de clustering)
@router.get("/distribution/{country}/{year}")
async def grade_distribution(country: str, year: int):
    query = """
        SELECT grade 
        FROM grades_by_country_year 
        WHERE country=%s AND year=%s
    """
    # Nota de Arquitectura: Si el volumen es extremo, en producción usaríamos Spark o 
    # re-modelaríamos la tabla. Para este proyecto, iteramos en background.
    rows = await run_in_threadpool(lambda: session.execute(query, (country, year)))

    distribution = {}
    for row in rows:
        distribution[row.grade] = distribution.get(row.grade, 0) + 1

    return {
        "country": country,
        "year": year,
        "distribution": distribution
    }

@router.get("/top-subjects")
def top_subjects():

    rows = session.execute("""
    SELECT subject, avg_grade
    FROM subject_averages
    LIMIT 10
    """)

    return list(rows)


@router.get("/student/{student_id}")
async def student_report(student_id: str):

    cache_key = f"report:{student_id}"
    cached = get_cache(cache_key)

    if cached:
        return {"source": "cache", "data": cached}

    # consulta real (mongo / cassandra)
    data = {"student_id": student_id, "grades": []}

    set_cache(cache_key, data)

    return {"source": "db", "data": data}