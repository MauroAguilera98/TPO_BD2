from fastapi import HTTPException
from app.db.neo4j import driver as neo4j_driver
from app.db.mongo import db as mongo_db
from app.conversion.conversion_service import ConversionService
from app.conversion.conversion_model import ConversionRequest

class TrajectoryService:
    @staticmethod
    async def get_full_trajectory(student_id: str, to_system: str, version: str):
        # 1. Consultar estructura en Neo4j
        cypher_query = """
        MATCH (s:Student {id: $student_id})-[r:TOOK]->(sub:Subject)
        OPTIONAL MATCH (sub)-[:BELONGS_TO]->(i:Institution)
        RETURN sub.id AS subject_id, r.grade_id AS grade_id, i.id AS institution_id, r.year AS year
        ORDER BY r.year DESC
        """
        
        async with neo4j_driver.session() as session:
            result = await session.run(cypher_query, student_id=student_id)
            records = await result.data()

        if not records:
            raise HTTPException(status_code=404, detail="Trayectoria no encontrada")

        academic_path = []

        # 2. Enriquecer con MongoDB y convertir notas al vuelo
        for rec in records:
            grade_id = rec["grade_id"]
            
            grade_doc = await mongo_db["grades_collection"].find_one({"_id": grade_id})
            if not grade_doc:
                continue

            original_val = float(grade_doc["original_grade"]["value"])
            from_sys = grade_doc.get("origin_system", "AR")
            
            # 3. Llamar al servicio de conversión interno (sin HTTP)
            conv_req = ConversionRequest(
                student_id=student_id,
                subject_id=rec["subject_id"],
                original_value=original_val,
                from_system=from_sys
            )
            
            # Pasamos None a background_tasks para lecturas masivas y evitar saturar auditoría
            # O Nacho puede manejarlo desde el router principal
            converted_data = await ConversionService.convert_grade(conv_req, to_system, version, background_tasks=None)

            academic_path.append({
                "subject_id": rec["subject_id"],
                "institution_id": rec["institution_id"],
                "year": rec["year"],
                "original_grade": grade_doc["original_grade"],
                "converted_grade": converted_data
            })

        return {
            "student_id": student_id,
            "total_records": len(academic_path),
            "academic_path": academic_path
        }