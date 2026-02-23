from fastapi import APIRouter

from app.db.neo4j import driver
from app.audit.audit_service import AuditService
from app.grade.grade_model import GradeCreate
from app.grade.grade_service import GradeService
from app.reporting.aggregator import ReportsAggregator

router = APIRouter()


# -----------------------------
# FUNCION ASÍNCRONA PARA NEO4J
# -----------------------------

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
        **payload,
    )


async def async_neo4j_insert(doc: dict, year: int, term: str):
    payload = {
        "student_id": doc["student_id"],
        "country": doc["country"],
        "institution_id": doc["institution_id"],
        "subject_id": doc["subject_id"],
        "grade_id": doc["grade_id"],
        "immutable_hash": doc.get("immutable_hash"),
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
        # Best-effort: no debe romper el POST si cae Cassandra (audit usa Cassandra)
        try:
            await AuditService.register_event(
                entity_type="grade",
                entity_id=grade_id,
                action="TRAJECTORY_FAILED",
                actor="system",
                payload={"error": str(e)},
            )
        except Exception:
            pass

    # Cassandra RF4 (best-effort)
    # Nota: el aggregator ya es best-effort, pero igual lo dejamos envuelto por seguridad.
    try:
        await ReportsAggregator.on_grade_created(created)
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