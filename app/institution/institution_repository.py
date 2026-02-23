from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import ReturnDocument

from app.db.mongo import institutions_collection


def _now() -> datetime:
    return datetime.now(timezone.utc)


class InstitutionRepository:
    """
    Capa de persistencia (Mongo).
    No mete auditoría ni reglas de negocio.
    """

    @staticmethod
    async def create(doc: Dict[str, Any]) -> Dict[str, Any]:
        await institutions_collection.insert_one(doc)
        return doc

    @staticmethod
    async def get(institution_id: str, include_inactive: bool = False) -> Optional[Dict[str, Any]]:
        query: Dict[str, Any] = {"_id": institution_id}
        if not include_inactive:
            query["is_active"] = True
        return await institutions_collection.find_one(query)

    @staticmethod
    async def list(limit: int = 50, skip: int = 0, include_inactive: bool = False) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if not include_inactive:
            query["is_active"] = True
        cursor = institutions_collection.find(query).skip(skip).limit(limit)
        return await cursor.to_list(length=limit)

    @staticmethod
    async def update(institution_id: str, changes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not changes:
            return await InstitutionRepository.get(institution_id, include_inactive=True)

        changes = dict(changes)
        changes["updated_at"] = _now()

        return await institutions_collection.find_one_and_update(
            {"_id": institution_id, "is_active": True},
            {"$set": changes},
            return_document=ReturnDocument.AFTER,
        )

    @staticmethod
    async def soft_delete(institution_id: str) -> Optional[Dict[str, Any]]:
        now = _now()
        return await institutions_collection.find_one_and_update(
            {"_id": institution_id, "is_active": True},
            {"$set": {"is_active": False, "deleted_at": now, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )