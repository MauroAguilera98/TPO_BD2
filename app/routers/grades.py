from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
import hashlib
import json
import uuid

# IMPORTANTE: Herramienta de FastAPI para delegar tareas síncronas a hilos secundarios
from fastapi.concurrency import run_in_threadpool 

from app.db.mongo import grades_collection
from app.db.cassandra import session
from app.db.neo4j import driver

router = APIRouter()

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

# --- FUNCIÓN AUXILIAR SÍNCRONA PARA NEO4J ---
# Aislamos el código bloqueante del context manager (with...)
def sync_neo4j_insert(grade: Grade, grade_id: str, hash_value: str, year: int, term: str):
    with driver.session() as neo:
        neo.run("""
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
        term=term)

# --- ENDPOINT PRINCIPAL ASÍNCRONO ---
@router.post("/grades")
async def register_grade(grade: Grade):
    grade_data = grade.model_dump()
    
    grade_id = str(uuid.uuid4())
    grade_data["grade_id"] = grade_id
    
    created_at = datetime.utcnow()
    grade_data["created_at"] = created_at
    
    hash_value = hashlib.sha256(
        json.dumps(grade_data, sort_keys=True, default=str).encode()
    ).hexdigest()
    grade_data["immutable_hash"] = hash_value
    
    # 1) Mongo (Source of Truth) - Nativo Asíncrono
    await grades_collection.insert_one(grade_data)
    
    # 2) Cassandra Audit - Delegado a Threadpool para no bloquear
    query = """
        INSERT INTO audit_log (student_id, event_time, action, hash)
        VALUES (%s, toTimestamp(now()), %s, %s)
    """
    params = (grade.student_id, "CREATE", hash_value)
    await run_in_threadpool(session.execute, query, params)
    
    # 3) Neo4j Trajectory (Best-effort) - Delegado a Threadpool
    trajectory_linked = False
    try:
        year = int(grade.metadata.get("year", created_at.year))
        term = str(grade.metadata.get("term", ""))
        
        # Ejecutamos nuestra función auxiliar síncrona en un hilo separado
        await run_in_threadpool(sync_neo4j_insert, grade, grade_id, hash_value, year, term)
        trajectory_linked = True
    except Exception as e:
        # Falla silenciosa: Mongo + Audit ya quedaron persistidos
        print(f"Error en Neo4j: {e}")
        trajectory_linked = False
        
    return {
        "status": "OK",
        "grade_id": grade_id,
        "hash": hash_value,
        "trajectory_linked": trajectory_linked
    }
