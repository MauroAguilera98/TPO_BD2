from __future__ import annotations

import asyncio
from fastapi import APIRouter, Query
from typing import Dict, Any, Optional, List

from app.db.cassandra import session
from app.db.mongo import subjects_collection

router = APIRouter(prefix="/reports", tags=["Reports"])

SUM_SCALE = 1000.0


def _avg(sum_milli: int, count_grade: int) -> Optional[float]:
    if not count_grade:
        return None
    return round((sum_milli / SUM_SCALE) / count_grade, 2)


@router.get("/average/{country}/{year}")
async def avg_country_year(country: str, year: int):
    query = """
    SELECT sum_milli, count_grade
    FROM stats_by_dim_year
    WHERE dim=%s AND dim_id=%s AND year=%s
    """
    rs = await asyncio.to_thread(session.execute, query, ("country", country.upper(), year))
    row = rs.one()
    if not row:
        return {"country": country.upper(), "year": year, "average": None, "total_records": 0}

    avg = _avg(int(row.sum_milli or 0), int(row.count_grade or 0))
    return {
        "country": country.upper(),
        "year": year,
        "average": avg,
        "total_records": int(row.count_grade or 0),
    }


@router.get("/average-institution/{institution_id}/{year}")
async def avg_institution_year(institution_id: str, year: int):
    query = """
    SELECT sum_milli, count_grade
    FROM stats_by_dim_year
    WHERE dim=%s AND dim_id=%s AND year=%s
    """
    rs = await asyncio.to_thread(session.execute, query, ("institution", institution_id, year))
    row = rs.one()
    if not row:
        return {"institution_id": institution_id, "year": year, "average": None, "total_records": 0}

    avg = _avg(int(row.sum_milli or 0), int(row.count_grade or 0))
    return {
        "institution_id": institution_id,
        "year": year,
        "average": avg,
        "total_records": int(row.count_grade or 0),
    }


@router.get("/top10/{country}/{year}")
async def top10_students(country: str, year: int):
    query = """
    SELECT student_id, sum_milli, count_grade
    FROM student_stats_by_country_year
    WHERE country=%s AND year=%s
    """
    rs = await asyncio.to_thread(session.execute, query, (country.upper(), year))

    rows = []
    for r in rs:
        cnt = int(r.count_grade or 0)
        sm = int(r.sum_milli or 0)
        avg = _avg(sm, cnt)
        if avg is None:
            continue
        rows.append({"student_id": r.student_id, "average": avg, "count": cnt})

    rows.sort(key=lambda x: x["average"], reverse=True)
    return {"country": country.upper(), "year": year, "top10": rows[:10]}


@router.get("/distribution/{country}/{year}")
async def grade_distribution(country: str, year: int):
    query = """
    SELECT bucket, count
    FROM grade_hist_by_country_year
    WHERE country=%s AND year=%s
    """
    rs = await asyncio.to_thread(session.execute, query, (country.upper(), year))

    dist: Dict[str, int] = {}
    for r in rs:
        dist[str(r.bucket)] = int(r.count or 0)

    return {"country": country.upper(), "year": year, "distribution": dist}


@router.get("/top-subjects")
async def top_subjects(
    limit: int = Query(10, ge=1, le=100),
    country: Optional[str] = Query(default=None),
    year: Optional[int] = Query(default=None),
    with_names: bool = Query(default=True, description="Si true, intenta traer nombres desde Mongo para los top N"),
):
    if country is not None and year is not None:
        query = """
        SELECT subject_id, sum_milli, count_grade
        FROM subject_stats_by_country_year
        WHERE country=%s AND year=%s
        """
        rs = await asyncio.to_thread(session.execute, query, (country.upper(), int(year)))
    else:
        # Global
        query = """
        SELECT subject_id, sum_milli, count_grade
        FROM subject_stats_global
        WHERE k=%s
        """
        rs = await asyncio.to_thread(session.execute, query, ("ALL",))

    rows = []
    for r in rs:
        cnt = int(r.count_grade or 0)
        sm = int(r.sum_milli or 0)
        avg = _avg(sm, cnt)
        if avg is None:
            continue
        rows.append({"subject_id": r.subject_id, "average": avg, "count": cnt})

    rows.sort(key=lambda x: x["average"], reverse=True)
    top = rows[:limit]

    if with_names and top:
        # Lookup liviano en Mongo (solo top N)
        ids = [x["subject_id"] for x in top]
        cursor = subjects_collection.find({"_id": {"$in": ids}}, {"_id": 1, "name": 1})
        docs = await cursor.to_list(length=len(ids))
        name_map = {d["_id"]: d.get("name") for d in docs}
        for x in top:
            x["name"] = name_map.get(x["subject_id"])

    out: Dict[str, Any] = {"limit": limit, "top": top}
    if country is not None and year is not None:
        out["country"] = country.upper()
        out["year"] = int(year)
    else:
        out["scope"] = "global"
    return out