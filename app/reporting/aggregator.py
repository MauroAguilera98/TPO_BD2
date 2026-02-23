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

            # system: preferimos el campo derivado; fallback a scale
            system = (doc.get("system") or (doc.get("original_grade") or {}).get("scale") or "").upper()

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

            # 2) Updates (counters)
            q_stats_dim = """
            UPDATE edugrade.stats_by_dim_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE dim = %s AND dim_id = %s AND year = %s
            """

            q_student = """
            UPDATE edugrade.student_stats_by_country_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE country = %s AND year = %s AND student_id = %s
            """

            q_subject = """
            UPDATE edugrade.subject_stats_by_country_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE country = %s AND year = %s AND subject_id = %s
            """

            q_subject_global = """
            UPDATE edugrade.subject_stats_global
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + 1
            WHERE k = %s AND subject_id = %s
            """

            q_hist = """
            UPDATE edugrade.grade_hist_by_country_year
            SET count = count + 1
            WHERE country = %s AND year = %s AND bucket = %s
            """

            # Promedios por dimensión/año
            await asyncio.to_thread(session.execute, q_stats_dim, (delta, "country", country, year))

            if institution_id:
                await asyncio.to_thread(session.execute, q_stats_dim, (delta, "institution", institution_id, year))

            if system:
                await asyncio.to_thread(session.execute, q_stats_dim, (delta, "system", system, year))

            # Top students / subjects
            if student_id:
                await asyncio.to_thread(session.execute, q_student, (delta, country, year, student_id))

            if subject_id:
                await asyncio.to_thread(session.execute, q_subject, (delta, country, year, subject_id))
                await asyncio.to_thread(session.execute, q_subject_global, (delta, "ALL", subject_id))

            # Histograma
            await asyncio.to_thread(session.execute, q_hist, (country, year, bucket))

        except Exception:
            # best-effort total: jamás propagamos error
            return
        
    @staticmethod
    async def on_grade_corrected(old_doc: Dict[str, Any], new_doc: Dict[str, Any]) -> None:
        try:
            new_grade_id = new_doc.get("grade_id") or new_doc.get("_id")
            if not new_grade_id:
                return

            # Solo si la nueva es numérica (si no, no tocamos stats)
            new_grade = ReportsAggregator._parse_numeric_grade(new_doc)
            old_grade = ReportsAggregator._parse_numeric_grade(old_doc)
            if new_grade is None or old_grade is None:
                return

            country_new = (new_doc.get("country") or "").upper()
            country_old = (old_doc.get("country") or "").upper()
            year_new = ReportsAggregator._resolve_year(new_doc)
            year_old = ReportsAggregator._resolve_year(old_doc)

            system_new = (new_doc.get("system") or (new_doc.get("original_grade") or {}).get("scale") or "").upper()
            system_old = (old_doc.get("system") or (old_doc.get("original_grade") or {}).get("scale") or "").upper()

            inst_new = new_doc.get("institution_id")
            inst_old = old_doc.get("institution_id")

            student_new = new_doc.get("student_id")
            student_old = old_doc.get("student_id")

            subject_new = new_doc.get("subject_id")
            subject_old = old_doc.get("subject_id")

            # Idempotencia: si ya procesamos el NEW grade_id, no hacemos nada (ni suma ni resta)
            ledger_q = """
            INSERT INTO edugrade.grade_ledger_by_id
            (grade_id, country, year, student_id, institution_id, subject_id, grade, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            IF NOT EXISTS
            """
            rs = await asyncio.to_thread(
                session.execute,
                ledger_q,
                (new_grade_id, country_new, year_new, student_new, inst_new, subject_new, float(new_grade), ReportsAggregator._now()),
            )
            row = rs.one()
            if row is not None and row[0] is False:
                return

            d_new = int(round(float(new_grade) * ReportsAggregator.SUM_SCALE))
            d_old = int(round(float(old_grade) * ReportsAggregator.SUM_SCALE))
            b_new = ReportsAggregator._bucket_0_10(float(new_grade))
            b_old = ReportsAggregator._bucket_0_10(float(old_grade))

            q_stats_dim = """
            UPDATE edugrade.stats_by_dim_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + %s
            WHERE dim = %s AND dim_id = %s AND year = %s
            """

            q_student = """
            UPDATE edugrade.student_stats_by_country_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + %s
            WHERE country = %s AND year = %s AND student_id = %s
            """

            q_subject = """
            UPDATE edugrade.subject_stats_by_country_year
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + %s
            WHERE country = %s AND year = %s AND subject_id = %s
            """

            q_subject_global = """
            UPDATE edugrade.subject_stats_global
            SET sum_milli = sum_milli + %s,
                count_grade = count_grade + %s
            WHERE k = %s AND subject_id = %s
            """

            q_hist = """
            UPDATE edugrade.grade_hist_by_country_year
            SET count = count + %s
            WHERE country = %s AND year = %s AND bucket = %s
            """

            # Helper: aplica +new y -old
            async def apply_dim(dim: str, dim_id_new: str, year_new_: int, dim_id_old: str, year_old_: int):
                # suma new
                if dim_id_new:
                    await asyncio.to_thread(session.execute, q_stats_dim, (d_new, 1, dim, dim_id_new, year_new_))
                # resta old
                if dim_id_old:
                    await asyncio.to_thread(session.execute, q_stats_dim, (-d_old, -1, dim, dim_id_old, year_old_))

            await apply_dim("country", country_new, year_new, country_old, year_old)
            await apply_dim("institution", inst_new or "", year_new, inst_old or "", year_old)
            await apply_dim("system", system_new, year_new, system_old, year_old)

            # Student stats (normalmente mismo student/country/year)
            if student_new and country_new:
                await asyncio.to_thread(session.execute, q_student, (d_new, 1, country_new, year_new, student_new))
            if student_old and country_old:
                await asyncio.to_thread(session.execute, q_student, (-d_old, -1, country_old, year_old, student_old))

            # Subject stats
            if subject_new and country_new:
                await asyncio.to_thread(session.execute, q_subject, (d_new, 1, country_new, year_new, subject_new))
                await asyncio.to_thread(session.execute, q_subject_global, (d_new, 1, "ALL", subject_new))
            if subject_old and country_old:
                await asyncio.to_thread(session.execute, q_subject, (-d_old, -1, country_old, year_old, subject_old))
                await asyncio.to_thread(session.execute, q_subject_global, (-d_old, -1, "ALL", subject_old))

            # Histograma
            await asyncio.to_thread(session.execute, q_hist, (1, country_new, year_new, b_new))
            await asyncio.to_thread(session.execute, q_hist, (-1, country_old, year_old, b_old))

        except Exception:
            return