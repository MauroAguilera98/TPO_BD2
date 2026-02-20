# app/routers/grades.py
from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
import hashlib
import json
import uuid

from app.db.mongo import grades_collection
from app.db.cassandra import session
from app.db.neo4j import driver  # <-- nuevo

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

@router.post("/grades")
def register_grade(grade: Grade):
    grade_data = grade.model_dump()

    grade_id = str(uuid.uuid4())
    grade_data["grade_id"] = grade_id

    created_at = datetime.utcnow()
    grade_data["created_at"] = created_at

    hash_value = hashlib.sha256(
        json.dumps(grade_data, sort_keys=True, default=str).encode()
    ).hexdigest()
    grade_data["immutable_hash"] = hash_value

    # 1) Mongo (source of truth)
    grades_collection.insert_one(grade_data)

    # 2) Cassandra audit
    session.execute("""
        INSERT INTO audit_log (student_id, event_time, action, hash)
        VALUES (%s, toTimestamp(now()), %s, %s)
    """, (grade.student_id, "CREATE", hash_value))

    # 3) Neo4j trajectory (best-effort)
    trajectory_linked = False
    try:
        year = int(grade.metadata.get("year", created_at.year))
        term = str(grade.metadata.get("term", ""))

        with driver.session() as neo:
            neo.run("""
                MERGE (s:Student {id: $student_id})
                MERGE (i:Institution {name: $institution, country: $country})
                MERGE (sub:Subject {name: $subject})

                MERGE (g:Grade {grade_id: $grade_id})
                SET g.immutable_hash = $immutable_hash

                MERGE (s)-[:STUDIED_AT]->(i)
                MERGE (s)-[t:TOOK {year: $year, term: $term}]->(sub)

                MERGE (g)-[:IN_SUBJECT]->(sub)
                MERGE (g)-[:AT_INSTITUTION]->(i)
            """, student_id=grade.student_id,
                 institution=grade.institution,
                 country=grade.country,
                 subject=grade.subject,
                 grade_id=grade_id,
                 immutable_hash=hash_value,
                 year=year,
                 term=term)
        trajectory_linked = True
    except Exception:
        # No rompemos el POST /grades: Mongo + audit ya quedaron persistidos.
        trajectory_linked = False

    return {
        "status": "OK",
        "grade_id": grade_id,
        "hash": hash_value,
        "trajectory_linked": trajectory_linked
    }
