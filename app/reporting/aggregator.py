from __future__ import annotations

import asyncio
import math
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.db.cassandra import session


class ReportsAggregator:
    """
    Actualiza tablas analíticas en Cassandra al crear una grade.
    - Best-effort: nunca rompe el POST /grades.
    - Idempotente: usa LWT contra grade_ledger_by_id (IF NOT EXISTS) para no duplicar contadores.
    """

    SUM_SCALE = 1000  # guardamos sum_milli = round(grade * 1000)

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _parse_numeric_grade(doc: Dict[str, Any]) -> Optional[float]:
        """
        Extrae doc["original_grade"]["value"] y lo convierte a float si se puede.
        Si no es convertible, devuelve None.
        """
        og = doc.get("original_grade") or {}
        val = og.get("value")
        if val is None:
            return None
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _resolve_year(doc: Dict[str, Any]) -> int:
        y = doc.get("year")
        if isinstance(y, int):
            return y
        issued_at = doc.get("issued_at")
        if isinstance(issued_at, datetime):
            return int(issued_at.year)
        # fallback (no debería pasar)
        return int(ReportsAggregator._now().year)

    @staticmethod
    def _bucket_0_10(grade: float) -> int:
        # histograma simple 0..10
        b = int(math.floor(grade))
        if b < 0:
            return 0
        if b > 10:
            return 10
        return b

    @staticmethod
    async def on_grade_created(doc: Dict[str, Any]) -> None:
        try:
            grade_id = doc.get("grade_id") or doc.get("_id")
            if not grade_id:
                return

            country = (doc.get("country") or "").upper()
            institution_id = doc.get("institution_id")
            student_id = doc.get("student_id")
            subject_id = doc.get("subject_id")
            year = ReportsAggregator._resolve_year(doc)

            # Solo RF4 para notas numéricas
            grade = ReportsAggregator._parse_numeric_grade(doc)
            if grade is None:
                return

            # 1) Idempotencia: si ya procesamos este grade_id, salimos
            ledger_q = """
            INSERT INTO edugrade.grade_ledger_by_id
              (grade_id, country, year, student_id, institution_id, subject_id, grade, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """
            rs = await asyncio.to_thread(
                session.execute,
                ledger_q,
                (grade_id, country, year, student_id, institution_id, subject_id, float(grade), ReportsAggregator._now()),
            )
            row = rs.one()
            # LWT devuelve una fila con "[applied]" como primer columna
            if row is not None and row[0] is False:
                return  # ya estaba, no duplicar contadores

            delta = int(round(float(grade) * ReportsAggregator.SUM_SCALE))
            bucket = ReportsAggregator._bucket_0_10(float(grade))

            # 2) Updates (counters) - best-effort interno
            # Promedio por país/año
            q_stats_dim = """
            UPDATE edugrade.stats_by_dim_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE dim = %s AND dim_id = %s AND year = %s
            """

            # Top estudiantes (por país/año)
            q_student = """
            UPDATE edugrade.student_stats_by_country_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE country = %s AND year = %s AND student_id = %s
            """

            # Top materias (por país/año)
            q_subject = """
            UPDATE edugrade.subject_stats_by_country_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE country = %s AND year = %s AND subject_id = %s
            """

            # Top materias global
            q_subject_global = """
            UPDATE edugrade.subject_stats_global
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE k = %s AND subject_id = %s
            """

            # Histograma
            q_hist = """
            UPDATE edugrade.grade_hist_by_country_year
            SET count = count + 1
            WHERE country = %s AND year = %s AND bucket = %s
            """

            # Ejecutamos en threadpool (driver sync)
            await asyncio.to_thread(session.execute, q_stats_dim, (delta, "country", country, year))
            if institution_id:
                await asyncio.to_thread(session.execute, q_stats_dim, (delta, "institution", institution_id, year))

            if student_id:
                await asyncio.to_thread(session.execute, q_student, (delta, country, year, student_id))

            if subject_id:
                await asyncio.to_thread(session.execute, q_subject, (delta, country, year, subject_id))
                await asyncio.to_thread(session.execute, q_subject_global, (delta, "ALL", subject_id))

            await asyncio.to_thread(session.execute, q_hist, (country, year, bucket))

        except Exception:
            # best-effort total: jamás propagamos error
            return