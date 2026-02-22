from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
import hashlib
import json
import uuid

from fastapi.concurrency import run_in_threadpool

from app.db.mongo import grades_collection
from app.db.neo4j import driver
from app.audit.audit_service import AuditService

router = APIRouter()


# -----------------------------
# MODELOS
# -----------------------------

class OriginalGrade(BaseModel):
    scale: str
    value: float | int | str


class Grade(BaseModel):
    student_id: str
    country: str
    institution: str
    subject: str
    original_grade: OriginalGrade
    metadata: Dict[str, Any]

# -----------------------------
# FUNCION ASÍNCRONA PARA NEO4J
# -----------------------------

# 1. Creamos una función auxiliar que representa la transacción atómica
async def _create_trajectory_tx(tx, payload: dict):
    # Nota que aquí usamos 'tx.run' en lugar de 'session.run'
    await tx.run(
        """
        MERGE (s:Student {id: $student_id})
        MERGE (i:Institution {name: $institution, country: $country})
        MERGE (sub:Subject {name: $subject})

        MERGE (g:Grade {grade_id: $grade_id})
        SET g.immutable_hash = coalesce($immutable_hash, g.immutable_hash)

        MERGE (s)-[:STUDIED_AT]->(i)
        
        // Relación temporal
        MERGE (s)-[:TOOK {year: $year, term: $term}]->(sub)
        
        MERGE (g)-[:IN_SUBJECT]->(sub)
        MERGE (g)-[:AT_INSTITUTION]->(i)
        """, **payload
    )

# 2. Refactorizamos la función principal para usar 'execute_write'
async def async_neo4j_insert(grade: Grade, grade_id: str, hash_value: str, year: int, term: str):
    
    # Armamos el diccionario de parámetros
    payload = {
        "student_id": grade.student_id,
        "country": grade.country,
        "institution": grade.institution,
        "subject": grade.subject,
        "grade_id": grade_id,
        "immutable_hash": hash_value,
        "year": year,
        "term": term
    }
    
    async with driver.session() as session:
        # execute_write ejecutará la función. Si hay Deadlock, 
        # esperará unos milisegundos y lo reintentará automáticamente.
        await session.execute_write(_create_trajectory_tx, payload)


# -----------------------------
# ENDPOINT PRINCIPAL
# -----------------------------

@router.post("/grades")
async def register_grade(grade: Grade):

    grade_data = grade.model_dump()

    # ID único
    grade_id = str(uuid.uuid4())
    grade_data["grade_id"] = grade_id

    # Timestamp
    created_at = datetime.utcnow()
    grade_data["created_at"] = created_at

    # Hash de integridad
    hash_value = hashlib.sha256(
        json.dumps(grade_data, sort_keys=True, default=str).encode()
    ).hexdigest()

    grade_data["immutable_hash"] = hash_value

    # -------------------------------------------------
    # 1) MONGO - SOURCE OF TRUTH
    # -------------------------------------------------
    await grades_collection.insert_one(grade_data)

    # -------------------------------------------------
    # 2) NEO4J - TRAYECTORIA ACADEMICA
    # -------------------------------------------------
# -------------------------------------------------
    # 2) NEO4J - TRAYECTORIA ACADEMICA (Ahora es 100% Async)
    # -------------------------------------------------
    trajectory_linked = False

    try:
        year = int(grade.metadata.get("year", created_at.year))
        term = str(grade.metadata.get("term", ""))

        # ¡Adiós run_in_threadpool! Ahora lo ejecutamos nativamente de forma asíncrona
        await async_neo4j_insert(
            grade,
            grade_id,
            hash_value,
            year,
            term
        )

        trajectory_linked = True

    except Exception as e:
        print(f"Neo4j error: {e}")
        # Auditoría 100% Async (Sin threadpool)
        await AuditService.register_event(
            entity_type="grade",
            entity_id=grade_id,
            action="TRAJECTORY_FAILED",
            actor="system",
            payload={"error": str(e)}
        )

    # -------------------------------------------------
    # 3) AUDITORIA (APPEND ONLY)
    # -------------------------------------------------
    await AuditService.register_event(
        entity_type="grade",
        entity_id=grade_id,
        action="CREATE_GRADE",
        actor="system",
        payload={
            "grade_id": grade_id, "student_id": grade.student_id,
            "subject": grade.subject, "value": grade.original_grade.value
        }
    )
    
    # -------------------------------------------------
    # 4) CASSANDRA - VISTA ANALÍTICA
    # -------------------------------------------------
    try:
        numeric_grade = float(grade.original_grade.value)
        import asyncio
        from app.db.cassandra import session
        
        query = """
            INSERT INTO edugrade.grades_by_country_year (country, year, student_id, grade)
            VALUES (%s, %s, %s, %s)
        """
        # Delegamos la llamada bloqueante de Cassandra de forma segura
        await asyncio.to_thread(session.execute, query, (grade.country, year, grade.student_id, numeric_grade))
    except ValueError:
        pass # Ignoramos notas con letras puras para el promedio matemático directo

    # Última Auditoría (Estudiante actualizado)
    await AuditService.register_event(
        entity_type="student",
        entity_id=grade.student_id,
        action="GRADE_CREATED",
        actor="system",
        payload={
            "grade_id": grade_id, "subject": grade.subject,
            "value": grade.original_grade.value, "country": grade.country,
            "institution": grade.institution, "year": int(grade.metadata.get("year", created_at.year)),
            "term": str(grade.metadata.get("term", ""))
        }
    )

    # -------------------------------------------------
    # RESPUESTA
    # -------------------------------------------------
    return {
        "status": "OK",
        "grade_id": grade_id,
        "hash": hash_value,
        "trajectory_linked": trajectory_linked
    }

@router.get("/grades/{grade_id}")
async def get_grade(grade_id: str):
    grade = await grades_collection.find_one({"grade_id": grade_id})

    if not grade:
        raise HTTPException(status_code=404, detail="Grade not found")

    grade["_id"] = str(grade["_id"])
    return grade
