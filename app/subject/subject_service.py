from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import HTTPException

from app.audit.audit_service import AuditService
from app.institution.institution_repository import InstitutionRepository
from app.subject.subject_repository import SubjectRepository


def _serialize(obj: Any) -> Any:
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    return obj


def _mongo_to_api(doc: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(doc)
    out["subject_id"] = out.pop("_id")
    return out


class SubjectService:
    @staticmethod
    async def create(payload: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        subject_id = payload["subject_id"]
        institution_id = payload["institution_id"]

        existing = await SubjectRepository.get(subject_id, include_inactive=True)
        if existing:
            raise HTTPException(status_code=409, detail="subject_id ya existe")

        # Política: no crear materias para instituciones inexistentes
        inst = await InstitutionRepository.get(institution_id, include_inactive=False)
        if not inst:
            raise HTTPException(status_code=404, detail="institution_id no existe o está inactiva")

        now = datetime.now(timezone.utc)
        doc = {
            "_id": subject_id,
            "institution_id": institution_id,
            "name": payload["name"],
            "kind": payload.get("kind", "subject"),
            "level": payload.get("level"),
            "credits": payload.get("credits"),
            "external_code": payload.get("external_code"),
            "metadata": payload.get("metadata", {}),
            "is_active": True,
            "created_at": now,
            "updated_at": now,
            "deleted_at": None,
        }

        await SubjectRepository.create(doc)

        await AuditService.register_event(
            entity_type="subject",
            entity_id=subject_id,
            action="CREATE",
            actor=actor,
            payload={"snapshot": _serialize(doc)},
        )

        return _mongo_to_api(doc)

    @staticmethod
    async def get(subject_id: str, include_inactive: bool = False) -> Dict[str, Any]:
        doc = await SubjectRepository.get(subject_id, include_inactive=include_inactive)
        if not doc:
            raise HTTPException(status_code=404, detail="materia/evaluación no encontrada")
        return _mongo_to_api(doc)

    @staticmethod
    async def list(limit: int = 50, skip: int = 0, include_inactive: bool = False, institution_id: str | None = None, kind: str | None = None):
        docs = await SubjectRepository.list(
            limit=limit,
            skip=skip,
            include_inactive=include_inactive,
            institution_id=institution_id,
            kind=kind,
        )
        return [_mongo_to_api(d) for d in docs]

    @staticmethod
    async def update(subject_id: str, changes: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        allowed = {"institution_id", "name", "kind", "level", "credits", "external_code", "metadata"}
        safe_changes = {k: v for k, v in changes.items() if k in allowed and v is not None}

        # Si quieren permitir mover una materia a otra institución, verificamos que exista
        if "institution_id" in safe_changes:
            inst = await InstitutionRepository.get(safe_changes["institution_id"], include_inactive=False)
            if not inst:
                raise HTTPException(status_code=404, detail="institution_id no existe o está inactiva")

        updated = await SubjectRepository.update(subject_id, safe_changes)
        if not updated:
            raise HTTPException(status_code=404, detail="materia/evaluación no encontrada o inactiva")

        await AuditService.register_event(
            entity_type="subject",
            entity_id=subject_id,
            action="UPDATE",
            actor=actor,
            payload={"changes": _serialize(safe_changes), "snapshot": _serialize(updated)},
        )

        return _mongo_to_api(updated)

    @staticmethod
    async def delete(subject_id: str, actor: str = "system") -> Dict[str, Any]:
        deleted = await SubjectRepository.soft_delete(subject_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="materia/evaluación no encontrada o inactiva")

        await AuditService.register_event(
            entity_type="subject",
            entity_id=subject_id,
            action="DELETE",
            actor=actor,
            payload={"snapshot": _serialize(deleted)},
        )

        return _mongo_to_api(deleted)