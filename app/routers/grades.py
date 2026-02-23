from fastapi import APIRouter
import asyncio

from app.db.mongo import grades_collection, institutions_collection, subjects_collection
from app.db.neo4j import driver
from app.audit.audit_service import AuditService
from app.grade.grade_model import GradeCreate
from app.grade.grade_service import GradeService
from app.db.cassandra import session as cass_session


router = APIRouter()


# -----------------------------
# MODELOS
# -----------------------------


# -----------------------------
# FUNCION ASÍNCRONA PARA NEO4J
# -----------------------------

# 1. Creamos una función auxiliar que representa la transacción atómica
async def _create_trajectory_tx(tx, payload: dict):
    await tx.run(
        """
        MERGE (s:Student {id: $student_id})
        MERGE (i:Institution {id: $institution_id})
        SET i.country = coalesce(i.country, $country)

        MERGE (sub:Subject {id: $subject_id})

        MERGE (g:Grade {grade_id: $grade_id})
        SET g.immutable_hash = coalesce(g.immutable_hash, $immutable_hash)

        MERGE (s)-[:STUDIED_AT]->(i)
        MERGE (s)-[t:TOOK {grade_id: $grade_id}]->(sub)
        SET t.year = $year, t.term = $term

        MERGE (g)-[:IN_SUBJECT]->(sub)
        MERGE (g)-[:AT_INSTITUTION]->(i)
        """,
        **payload
    )

# 2. Refactorizamos la función principal para usar 'execute_write'
async def async_neo4j_insert(doc: dict, year: int, term: str):
    payload = {
        "student_id": doc["student_id"],
        "country": doc["country"],
        "institution_id": doc["institution_id"],
        "subject_id": doc["subject_id"],
        "grade_id": doc["grade_id"],
        "immutable_hash": doc["immutable_hash"],
        "year": year,
        "term": term,
    }

    async with driver.session() as session:
        await session.execute_write(_create_trajectory_tx, payload)


# -----------------------------
# ENDPOINT PRINCIPAL
# -----------------------------

@router.post("/grades")
async def register_grade(body: GradeCreate):
    created = await GradeService.create(body.model_dump(), actor="system")
    grade_id = created["grade_id"]

    trajectory_linked = False
    try:
        issued_at = created["issued_at"]
        meta = created.get("metadata", {}) or {}
        year = int(meta.get("year", issued_at.year))
        term = str(meta.get("term", ""))

        await async_neo4j_insert(created, year, term)
        trajectory_linked = True

    except Exception as e:
        await AuditService.register_event(
            entity_type="grade",
            entity_id=grade_id,
            action="TRAJECTORY_FAILED",
            actor="system",
            payload={"error": str(e)},
        )

    # Cassandra (best-effort)
    try:
        numeric_grade = float(created["original_grade"]["value"])
        issued_at = created["issued_at"]
        meta = created.get("metadata", {}) or {}
        year = int(meta.get("year", issued_at.year))

        query = """
            INSERT INTO edugrade.grades_by_country_year (country, year, student_id, grade)
            VALUES (%s, %s, %s, %s)
        """
        await asyncio.to_thread(
            cass_session.execute,
            query,
            (created["country"], year, created["student_id"], numeric_grade),
        )
    except Exception:
        pass

    return {
        "status": "OK",
        "grade_id": grade_id,
        "hash": created.get("immutable_hash"),
        "trajectory_linked": trajectory_linked,
    }


@router.get("/grades/{grade_id}")
async def get_grade(grade_id: str):
    return await GradeService.get(grade_id)