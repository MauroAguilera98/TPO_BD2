# app/routers/trajectory.py
from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

from app.db.neo4j import driver

router = APIRouter(prefix="/trajectory", tags=["Trajectory"])


# ---------
# Models
# ---------
class TrajectoryLink(BaseModel):
    student_id: str
    subject: str
    institution: str
    country: str = "UNK"

    year: int = Field(..., ge=1900, le=2100)
    term: Optional[str] = ""  # "S1", "S2", "Q1", etc.

    # Referencia a la nota real (Mongo) por id/hash
    grade_id: str
    immutable_hash: Optional[str] = None


class Equivalence(BaseModel):
    subject_a: str
    subject_b: str


# ---------
# Routes
# ---------
@router.post("/link")
def link_grade_to_trajectory(data: TrajectoryLink):
    """
    Crea/asegura nodos Student, Institution, Subject, Grade y los relaciona.
    """
    payload = data.model_dump()
    # Aseguramos term no-null
    payload["term"] = payload.get("term") or ""

    with driver.session() as session:
        session.run(
            """
            MERGE (s:Student {id: $student_id})
            MERGE (i:Institution {name: $institution, country: $country})
            MERGE (sub:Subject {name: $subject})

            MERGE (g:Grade {grade_id: $grade_id})
            SET g.immutable_hash = coalesce($immutable_hash, g.immutable_hash)

            MERGE (s)-[:STUDIED_AT]->(i)

            // relación temporal (si el mismo alumno cursa la misma materia en mismo año/term, queda "colapsado")
            MERGE (s)-[:TOOK {year: $year, term: $term}]->(sub)

            MERGE (g)-[:IN_SUBJECT]->(sub)
            MERGE (g)-[:AT_INSTITUTION]->(i)
            """,
            **payload
        )

    return {"status": "linked"}


@router.get("/{student_id}")
def get_trajectory(student_id: str) -> List[Dict[str, Any]]:
    """
    Devuelve la trayectoria ordenada por año/term, incluyendo los grade_id vinculados.
    """
    with driver.session() as session:
        result = session.run(
            """
            MATCH (s:Student {id: $student_id})-[t:TOOK]->(sub:Subject)

            // Vinculamos notas si existen para esa materia
            OPTIONAL MATCH (g:Grade)-[:IN_SUBJECT]->(sub)
            OPTIONAL MATCH (g)-[:AT_INSTITUTION]->(i:Institution)

            RETURN
                t.year AS year,
                t.term AS term,
                sub.name AS subject,
                collect(DISTINCT {
                    grade_id: g.grade_id,
                    immutable_hash: g.immutable_hash,
                    institution: i.name,
                    country: i.country
                }) AS records
            ORDER BY year ASC, term ASC
            """,
            student_id=student_id
        )

        # Normalizamos records: puede venir un objeto con nulls si no hay g/i
        out = []
        for r in result:
            records = [
                rec for rec in (r["records"] or [])
                if rec.get("grade_id") is not None
            ]
            out.append({
                "year": r["year"],
                "term": r["term"],
                "subject": r["subject"],
                "records": records
            })
        return out


@router.post("/equivalence")
def add_equivalence(eq: Equivalence):
    """
    Crea equivalencias bidireccionales entre materias.
    """
    with driver.session() as session:
        session.run(
            """
            MERGE (a:Subject {name: $a})
            MERGE (b:Subject {name: $b})
            MERGE (a)-[:EQUIVALENT]->(b)
            MERGE (b)-[:EQUIVALENT]->(a)
            """,
            a=eq.subject_a,
            b=eq.subject_b
        )

    return {"status": "equivalence_added"}


@router.get("/student-path/{student_id}")
def get_student_path(student_id: str):
    """
    Similar a tu endpoint actual: materias + equivalentes (hasta 2 saltos).
    """
    with driver.session() as session:
        result = session.run(
            """
            MATCH (s:Student {id: $student_id})-[:TOOK]->(sub:Subject)
            OPTIONAL MATCH (sub)-[:EQUIVALENT*1..2]-(eq:Subject)
            RETURN sub.name AS subject,
                   collect(DISTINCT eq.name) AS equivalents
            """,
            student_id=student_id
        )

        return [
            {"subject": r["subject"], "equivalents": r["equivalents"]}
            for r in result
        ]
