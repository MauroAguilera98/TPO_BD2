from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from fastapi import HTTPException

from app.audit.audit_service import AuditService
from app.institution.institution_repository import InstitutionRepository


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
    out["institution_id"] = out.pop("_id")
    return out


class InstitutionService:
    @staticmethod
    async def create(payload: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        institution_id = payload["institution_id"]

        existing = await InstitutionRepository.get(institution_id, include_inactive=True)
        if existing:
            raise HTTPException(status_code=409, detail="institution_id ya existe")

        doc = {
            "_id": institution_id,
            "name": payload["name"],
            "country": payload["country"],
            "system": payload.get("system"),
            "metadata": payload.get("metadata", {}),
            "is_active": True,
            "created_at": payload.get("created_at"),  # normalmente no viene; lo seteamos abajo si falta
            "updated_at": payload.get("updated_at"),
            "deleted_at": None,
        }

        # timestamps server-side
        from datetime import timezone, datetime as dt

        now = dt.now(timezone.utc)
        doc["created_at"] = now
        doc["updated_at"] = now

        await InstitutionRepository.create(doc)

        await AuditService.register_event(
            entity_type="institution",
            entity_id=institution_id,
            action="CREATE",
            actor=actor,
            payload={"snapshot": _serialize(doc)},
        )

        return _mongo_to_api(doc)

    @staticmethod
    async def get(institution_id: str, include_inactive: bool = False) -> Dict[str, Any]:
        doc = await InstitutionRepository.get(institution_id, include_inactive=include_inactive)
        if not doc:
            raise HTTPException(status_code=404, detail="institución no encontrada")
        return _mongo_to_api(doc)

    @staticmethod
    async def list(limit: int = 50, skip: int = 0, include_inactive: bool = False):
        docs = await InstitutionRepository.list(limit=limit, skip=skip, include_inactive=include_inactive)
        return [_mongo_to_api(d) for d in docs]

    @staticmethod
    async def update(institution_id: str, changes: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        allowed = {"name", "country", "system", "metadata"}
        safe_changes = {k: v for k, v in changes.items() if k in allowed and v is not None}

        updated = await InstitutionRepository.update(institution_id, safe_changes)
        if not updated:
            raise HTTPException(status_code=404, detail="institución no encontrada o inactiva")

        await AuditService.register_event(
            entity_type="institution",
            entity_id=institution_id,
            action="UPDATE",
            actor=actor,
            payload={"changes": _serialize(safe_changes), "snapshot": _serialize(updated)},
        )

        return _mongo_to_api(updated)

    @staticmethod
    async def delete(institution_id: str, actor: str = "system") -> Dict[str, Any]:
        deleted = await InstitutionRepository.soft_delete(institution_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="institución no encontrada o inactiva")

        await AuditService.register_event(
            entity_type="institution",
            entity_id=institution_id,
            action="DELETE",
            actor=actor,
            payload={"snapshot": _serialize(deleted)},
        )

        return _mongo_to_api(deleted)