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
# FUNCION SINCRONA PARA NEO4J
# -----------------------------

def sync_neo4j_insert(
    grade: Grade,
    grade_id: str,
    hash_value: str,
    year: int,
    term: str
):
    with driver.session() as neo:
        neo.run(
            """
            MERGE (s:Student {id: $student_id})
            MERGE (i:Institution {name: $institution, country: $country})
            MERGE (sub:Subject {name: $subject})

            MERGE (g:Grade {grade_id: $grade_id})
            SET g.immutable_hash = $immutable_hash

            MERGE (s)-[:STUDIED_AT]->(i)
            MERGE (s)-[:TOOK {year: $year, term: $term}]->(sub)

            MERGE (g)-[:IN_SUBJECT]->(sub)
            MERGE (g)-[:AT_INSTITUTION]->(i)
            """,
            student_id=grade.student_id,
            institution=grade.institution,
            country=grade.country,
            subject=grade.subject,
            grade_id=grade_id,
            immutable_hash=hash_value,
            year=year,
            term=term
        )


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
    trajectory_linked = False

    try:
        year = int(grade.metadata.get("year", created_at.year))
        term = str(grade.metadata.get("term", ""))

        await run_in_threadpool(
            sync_neo4j_insert,
            grade,
            grade_id,
            hash_value,
            year,
            term
        )

        trajectory_linked = True

    except Exception as e:

        print(f"Neo4j error: {e}")

        # registrar fallo en auditoría
        await run_in_threadpool(
            AuditService.register_event,
            "grade",
            grade_id,
            "TRAJECTORY_FAILED",
            "system",
            {"error": str(e)}
        )

    # -------------------------------------------------
    # 3) AUDITORIA (APPEND ONLY)
    # -------------------------------------------------
    await run_in_threadpool(
        AuditService.register_event,
        "grade",
        grade_id,
        "CREATE_GRADE",
        "system",
        {
            "grade_id": grade_id,
            "student_id": grade.student_id,
            "subject": grade.subject,
            "value": grade.original_grade.value
        }
    )

    await run_in_threadpool(
    AuditService.register_event,
    "student",
    grade.student_id,          # <- entity_id = ID del alumno
    "GRADE_CREATED",
    "system",
    {
        "grade_id": grade_id,
        "subject": grade.subject,
        "value": grade.original_grade.value,
        "country": grade.country,
        "institution": grade.institution,
        "year": int(grade.metadata.get("year", created_at.year)),
        "term": str(grade.metadata.get("term", "")),
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
