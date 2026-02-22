from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any

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
# Funciones Auxiliares Asíncronas
# ----------
# 1. Función auxiliar que encapsula la transacción atómica
async def _link_grade_tx(tx, payload: dict):
    # Utilizamos 'tx.run' en lugar de 'session.run'
    await tx.run("""
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

# 2. Función principal refactorizada con reintentos automáticos
async def async_link_grade(payload: dict):
    async with driver.session() as session:
        # execute_write captura el DeadlockDetected y reintenta la función _link_grade_tx
        await session.execute_write(_link_grade_tx, payload)

async def async_get_trajectory(student_id: str) -> List[Dict[str, Any]]:
    async with driver.session() as session:
        # 1. Ejecutamos la query y esperamos el cursor (AsyncResult)
        result = await session.run("""
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
        # 2. Iteramos asíncronamente sobre los resultados a medida que llegan por red
        async for r in result:
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

async def async_add_equivalence(a: str, b: str):
    async with driver.session() as session:
        await session.run("""
            MERGE (a:Subject {name: $a})
            MERGE (b:Subject {name: $b})
            MERGE (a)-[:EQUIVALENT]->(b)
            MERGE (b)-[:EQUIVALENT]->(a)
        """, a=a, b=b)

async def async_get_student_path(student_id: str):
    async with driver.session() as session:
        result = await session.run("""
            MATCH (s:Student {id: $student_id})-[:TOOK]->(sub:Subject)
            OPTIONAL MATCH (sub)-[:EQUIVALENT*1..2]-(eq:Subject)
            RETURN sub.name AS subject,
                   collect(DISTINCT eq.name) AS equivalents
        """, student_id=student_id)
        
        # 3. Comprensión de lista asíncrona
        return [{"subject": r["subject"], "equivalents": r["equivalents"]} async for r in result]


# ----------
# Routes (Async)
# ----------
@router.post("/link")
async def link_grade_to_trajectory(data: TrajectoryLink):
    payload = data.model_dump()
    payload["term"] = payload.get("term") or ""
    
    await async_link_grade(payload)
    return {"status": "linked"}

@router.get("/{student_id}")
async def get_trajectory(student_id: str) -> List[Dict[str, Any]]:
    return await async_get_trajectory(student_id)

@router.post("/equivalence")
async def add_equivalence(eq: Equivalence):
    await async_add_equivalence(eq.subject_a, eq.subject_b)
    return {"status": "equivalence_added"}

@router.get("/student-path/{student_id}")
async def get_student_path(student_id: str):
    return await async_get_student_path(student_id)

# Endpoint para historial académico del estudiante
@router.get("/student/{student_id}")
async def get_student_trajectory(student_id: str):
    # 4. El endpoint olvidado, ahora asíncrono y seguro
    async with driver.session() as session:
        result = await session.run("""
        MATCH (s:Student {id:$id})-[:TOOK]->(sub:Subject)
        RETURN sub.name
        """, id=student_id)

        return [r["sub.name"] async for r in result]