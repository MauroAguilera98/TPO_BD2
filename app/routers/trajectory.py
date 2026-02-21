from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from fastapi.concurrency import run_in_threadpool

from app.db.neo4j import driver

router = APIRouter(prefix="/trajectory", tags=["Trajectory"])

# ----------
# Models
# ----------
class TrajectoryLink(BaseModel):
    student_id: str
    subject: str
    institution: str
    country: str = "UNK"
    year: int = Field(..., ge=1900, le=2100)
    term: Optional[str] = "" 
    grade_id: str
    immutable_hash: Optional[str] = None

class Equivalence(BaseModel):
    subject_a: str
    subject_b: str


# ----------
# Funciones Auxiliares Síncronas (Thread-Safe)
# ----------
def sync_link_grade(payload: dict):
    with driver.session() as session:
        session.run("""
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
        """, **payload)

def sync_get_trajectory(student_id: str) -> List[Dict[str, Any]]:
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Student {id: $student_id})-[t:TOOK]->(sub:Subject)
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
        """, student_id=student_id)
        
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

def sync_add_equivalence(a: str, b: str):
    with driver.session() as session:
        session.run("""
            MERGE (a:Subject {name: $a})
            MERGE (b:Subject {name: $b})
            MERGE (a)-[:EQUIVALENT]->(b)
            MERGE (b)-[:EQUIVALENT]->(a)
        """, a=a, b=b)

def sync_get_student_path(student_id: str):
    with driver.session() as session:
        result = session.run("""
            MATCH (s:Student {id: $student_id})-[:TOOK]->(sub:Subject)
            OPTIONAL MATCH (sub)-[:EQUIVALENT*1..2]-(eq:Subject)
            RETURN sub.name AS subject,
                   collect(DISTINCT eq.name) AS equivalents
        """, student_id=student_id)
        return [{"subject": r["subject"], "equivalents": r["equivalents"]} for r in result]


# ----------
# Routes (Async)
# ----------
@router.post("/link")
async def link_grade_to_trajectory(data: TrajectoryLink):
    payload = data.model_dump()
    payload["term"] = payload.get("term") or ""
    
    await run_in_threadpool(sync_link_grade, payload)
    return {"status": "linked"}

@router.get("/{student_id}")
async def get_trajectory(student_id: str) -> List[Dict[str, Any]]:
    return await run_in_threadpool(sync_get_trajectory, student_id)

@router.post("/equivalence")
async def add_equivalence(eq: Equivalence):
    await run_in_threadpool(sync_add_equivalence, eq.subject_a, eq.subject_b)
    return {"status": "equivalence_added"}

@router.get("/student-path/{student_id}")
async def get_student_path(student_id: str):
    return await run_in_threadpool(sync_get_student_path, student_id)

#Endpoint para historial académico del estudiante
@router.get("/student/{student_id}")
def get_student_trajectory(student_id: str):

    with driver.session() as session:

        result = session.run("""
        MATCH (s:Student {id:$id})-[:TOOK]->(sub:Subject)
        RETURN sub.name
        """, id=student_id)

        return [r["sub.name"] for r in result]