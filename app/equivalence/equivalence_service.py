from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Any, List

from fastapi import HTTPException

from app.db.neo4j import driver
from app.audit.audit_service import AuditService
from app.subject.subject_repository import SubjectRepository


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _get_subject_or_404(subject_id: str) -> Dict[str, Any]:
    s = await SubjectRepository.get(subject_id, include_inactive=False)
    if not s:
        raise HTTPException(status_code=404, detail=f"subject_id {subject_id} no existe o está inactiva")
    return s


class EquivalenceService:
    @staticmethod
    async def create(payload: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        a_id = payload["subject_id_a"]
        b_id = payload["subject_id_b"]
        if a_id == b_id:
            raise HTTPException(status_code=422, detail="subject_id_a y subject_id_b no pueden ser iguales")

        # Validación referencial (Mongo)
        a = await _get_subject_or_404(a_id)
        b = await _get_subject_or_404(b_id)

        created_at = _now_iso()
        partial = bool(payload.get("partial", False))
        coverage = payload.get("coverage", None)
        note = payload.get("note", None)
        bidirectional = bool(payload.get("bidirectional", True))

        async def _tx_create(tx):
            # Creamos/MERGE nodos (con id) y seteamos name para queries amigables
            await tx.run(
                """
                MERGE (sa:Subject {id: $a_id})
                SET sa.name = coalesce(sa.name, $a_name)
                MERGE (sb:Subject {id: $b_id})
                SET sb.name = coalesce(sb.name, $b_name)

                MERGE (sa)-[r:EQUIVALENT_TO]->(sb)
                SET r.partial = $partial,
                    r.coverage = $coverage,
                    r.note = $note,
                    r.created_at = $created_at,
                    r.created_by = $actor
                """,
                a_id=a_id,
                b_id=b_id,
                a_name=a.get("name"),
                b_name=b.get("name"),
                partial=partial,
                coverage=coverage,
                note=note,
                created_at=created_at,
                actor=actor,
            )

            if bidirectional:
                await tx.run(
                    """
                    MATCH (sa:Subject {id:$a_id}), (sb:Subject {id:$b_id})
                    MERGE (sb)-[r:EQUIVALENT_TO]->(sa)
                    SET r.partial = $partial,
                        r.coverage = $coverage,
                        r.note = $note,
                        r.created_at = $created_at,
                        r.created_by = $actor
                    """,
                    a_id=a_id,
                    b_id=b_id,
                    partial=partial,
                    coverage=coverage,
                    note=note,
                    created_at=created_at,
                    actor=actor,
                )

        async with driver.session() as session:
            await session.execute_write(_tx_create)

        # Auditoría (Cassandra)
        eq_id = f"{a_id}__{b_id}"
        await AuditService.register_event(
            entity_type="equivalence",
            entity_id=eq_id,
            action="CREATE",
            actor=actor,
            payload={
                "subject_id_a": a_id,
                "subject_id_b": b_id,
                "bidirectional": bidirectional,
                "partial": partial,
                "coverage": coverage,
                "note": note,
            },
        )

        return {"status": "OK", "equivalence_id": eq_id}

    @staticmethod
    async def list_for_subject(subject_id: str, depth: int = 1) -> List[Dict[str, Any]]:
        if depth < 1 or depth > 5:
            raise HTTPException(status_code=422, detail="depth debe estar entre 1 y 5")

        query = f"""
        MATCH (s:Subject {{id:$sid}})-[r:EQUIVALENT_TO*1..{depth}]->(x:Subject)
        RETURN DISTINCT x.id AS subject_id, x.name AS name
        LIMIT 200
        """

        async with driver.session() as session:
            result = await session.run(query, sid=subject_id)
            return [{"subject_id": r["subject_id"], "name": r.get("name")} async for r in result]

    @staticmethod
    async def delete(payload: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        a_id = payload["subject_id_a"]
        b_id = payload["subject_id_b"]
        bidirectional = bool(payload.get("bidirectional", True))

        async def _tx_delete(tx):
            await tx.run(
                """
                MATCH (:Subject {id:$a_id})-[r:EQUIVALENT_TO]->(:Subject {id:$b_id})
                DELETE r
                """,
                a_id=a_id,
                b_id=b_id,
            )
            if bidirectional:
                await tx.run(
                    """
                    MATCH (:Subject {id:$b_id})-[r:EQUIVALENT_TO]->(:Subject {id:$a_id})
                    DELETE r
                    """,
                    a_id=a_id,
                    b_id=b_id,
                )

        async with driver.session() as session:
            await session.execute_write(_tx_delete)

        eq_id = f"{a_id}__{b_id}"
        await AuditService.register_event(
            entity_type="equivalence",
            entity_id=eq_id,
            action="DELETE",
            actor=actor,
            payload={"subject_id_a": a_id, "subject_id_b": b_id, "bidirectional": bidirectional},
        )

        return {"status": "OK", "equivalence_id": eq_id}