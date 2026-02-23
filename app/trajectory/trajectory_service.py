from fastapi import HTTPException
from typing import Any, Dict, List

from app.db.neo4j import driver as neo4j_driver
from app.db.mongo import grades_collection  # <-- colección real
from app.conversion.conversion_service import ConversionService
from app.conversion.conversion_model import ConversionRequest


class TrajectoryService:
    @staticmethod
    async def get_full_trajectory(student_id: str, to_system: str, version: str):
        """
        Devuelve trayectoria completa + conversión on-the-fly.
        - Lee estructura en Neo4j (TOOK con grade_id, year, term)
        - Lee nota fuente en Mongo (grades)
        - Convierte usando ConversionService (sin HTTP)
        """

        to_system = (to_system or "").upper()
        version = version or "v1"

        # En tu grafo: TOOK está entre Student -> Subject y guarda grade_id/year/term.
        # Institution se recupera desde Grade -> AT_INSTITUTION -> Institution (según tu MERGE del POST /grades).
        cypher_query = """
        MATCH (s:Student {id: $student_id})-[t:TOOK]->(sub:Subject)
        OPTIONAL MATCH (g:Grade {grade_id: t.grade_id})-[:AT_INSTITUTION]->(i:Institution)
        RETURN
            sub.id AS subject_id,
            t.grade_id AS grade_id,
            i.id AS institution_id,
            t.year AS year,
            t.term AS term
        ORDER BY t.year DESC
        """

        async with neo4j_driver.session() as session:
            result = await session.run(cypher_query, student_id=student_id)
            records: List[Dict[str, Any]] = await result.data()

        if not records:
            raise HTTPException(status_code=404, detail="Trayectoria no encontrada")

        academic_path: List[Dict[str, Any]] = []

        for rec in records:
            grade_id = rec.get("grade_id")
            if not grade_id:
                continue

            # Mongo: tu colección se llama "grades" y está exportada como grades_collection
            grade_doc = await grades_collection.find_one({"_id": grade_id})
            if not grade_doc:
                continue

            # OJO: value puede ser float/int/str (UK letras), no castees a float acá
            original_value = (grade_doc.get("original_grade") or {}).get("value")
            if original_value is None:
                continue

            # Sistema de origen:
            # - preferimos el campo "system" que vos ya guardás en grade
            # - fallback a original_grade.scale
            from_sys = (
                (grade_doc.get("system") or "")
                or ((grade_doc.get("original_grade") or {}).get("scale") or "")
                or "AR"
            ).upper()

            conv_req = ConversionRequest(
                student_id=student_id,
                subject_id=rec.get("subject_id"),
                original_value=original_value,
                from_system=from_sys,
            )

            # Importante: background_tasks=None para que el GET no audite (read-only)
            converted_data = await ConversionService.convert_grade(
                conv_req,
                to_system=to_system,
                version=version,
                background_tasks=None,
            )

            academic_path.append(
                {
                    "subject_id": rec.get("subject_id"),
                    "institution_id": rec.get("institution_id"),
                    "year": rec.get("year"),
                    "term": rec.get("term"),
                    "grade_id": grade_id,
                    "original_grade": grade_doc.get("original_grade"),
                    "converted_grade": converted_data,
                }
            )

        return {
            "student_id": student_id,
            "to_system": to_system,
            "version": version,
            "total_records": len(academic_path),
            "academic_path": academic_path,
        }