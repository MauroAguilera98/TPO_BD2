from fastapi import APIRouter
from pydantic import BaseModel
from typing import Dict, Any
from datetime import datetime
import hashlib
import json

from db.mongo import grades_collection
from db.cassandra import session

router = APIRouter()

class Grade(BaseModel):
    student_id: str
    country: str
    institution: str
    subject: str
    original_grade: Dict[str, Any]
    metadata: Dict[str, Any]

@router.post("/grades")
def register_grade(grade: Grade):

    grade_data = grade.model_dump()

    hash_value = hashlib.sha256(
        json.dumps(grade_data).encode()
    ).hexdigest()

    grade_data["immutable_hash"] = hash_value
    grade_data["created_at"] = datetime.utcnow()

    grades_collection.insert_one(grade_data)

    session.execute("""
        INSERT INTO audit_log (student_id, event_time, action, hash)
        VALUES (%s, toTimestamp(now()), %s, %s)
    """, (grade.student_id, "CREATE", hash_value))

    return {"status": "OK", "hash": hash_value}
