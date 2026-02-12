from fastapi import APIRouter
from db.cassandra import session

router = APIRouter(prefix="/reports", tags=["Reports"])

# Promedio por país y año
@router.get("/average/{country}/{year}")
def get_country_average(country: str, year: int):

    rows = session.execute("""
        SELECT grade FROM grades_by_country_year
        WHERE country=%s AND year=%s
    """, (country, year))

    grades = [row.grade for row in rows]

    if not grades:
        return {"country": country, "year": year, "average": None}

    avg = sum(grades) / len(grades)

    return {
        "country": country,
        "year": year,
        "average": round(avg, 2),
        "total_records": len(grades)
    }


# Top 10 estudiantes por promedio en un país/año
@router.get("/top10/{country}/{year}")
def top_10_students(country: str, year: int):

    rows = session.execute("""
        SELECT student_id, grade FROM grades_by_country_year
        WHERE country=%s AND year=%s
    """, (country, year))

    data = {}

    for row in rows:
        if row.student_id not in data:
            data[row.student_id] = []
        data[row.student_id].append(row.grade)

    averages = [
        (student, sum(grades)/len(grades))
        for student, grades in data.items()
    ]

    top10 = sorted(averages, key=lambda x: x[1], reverse=True)[:10]

    return {
        "country": country,
        "year": year,
        "top10": [
            {"student_id": s, "average": round(avg, 2)}
            for s, avg in top10
        ]
    }
# Distribución de calificaciones por país y año
@router.get("/distribution/{country}/{year}")