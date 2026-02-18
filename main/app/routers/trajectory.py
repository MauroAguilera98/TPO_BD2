from fastapi import APIRouter
from db.neo4j import driver

router = APIRouter()

@router.post("/trajectory")
def create_trajectory(student_id: str, subject: str, institution: str):

    with driver.session() as session:
        session.run("""
            MERGE (s:Student {id: $student_id})
            MERGE (i:Institution {name: $institution})
            MERGE (m:Subject {name: $subject})

            MERGE (s)-[:STUDIED_AT]->(i)
            MERGE (s)-[:TOOK]->(m)
        """, student_id=student_id,
             institution=institution,
             subject=subject)

    return {"status": "trajectory_created"}

@router.get("/student-path/{student_id}")
def get_student_path(student_id: str):

    with driver.session() as session:
        result = session.run("""
            MATCH (s:Student {id: $student_id})-[:TOOK]->(sub)
            OPTIONAL MATCH (sub)-[:EQUIVALENT*1..2]-(eq)
            RETURN sub.name AS subject,
                   collect(DISTINCT eq.name) AS equivalents
        """, student_id=student_id)

        return [
            {
                "subject": r["subject"],
                "equivalents": r["equivalents"]
            }
            for r in result
        ]
