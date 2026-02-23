from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import ReturnDocument

from app.db.mongo import subjects_collection


def _now() -> datetime:
    return datetime.now(timezone.utc)


class SubjectRepository:
    @staticmethod
    async def create(doc: Dict[str, Any]) -> Dict[str, Any]:
        await subjects_collection.insert_one(doc)
        return doc

    @staticmethod
    async def get(subject_id: str, include_inactive: bool = False) -> Optional[Dict[str, Any]]:
        query: Dict[str, Any] = {"_id": subject_id}
        if not include_inactive:
            query["is_active"] = True
        return await subjects_collection.find_one(query)

    @staticmethod
    async def list(
        limit: int = 50,
        skip: int = 0,
        include_inactive: bool = False,
        institution_id: Optional[str] = None,
        kind: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        query: Dict[str, Any] = {}
        if not include_inactive:
            query["is_active"] = True
        if institution_id:
            query["institution_id"] = institution_id
        if kind:
            query["kind"] = kind

        cursor = subjects_collection.find(query).skip(skip).limit(limit)
        return await cursor.to_list(length=limit)

    @staticmethod
    async def update(subject_id: str, changes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not changes:
            return await SubjectRepository.get(subject_id, include_inactive=True)

        changes = dict(changes)
        changes["updated_at"] = _now()

        return await subjects_collection.find_one_and_update(
            {"_id": subject_id, "is_active": True},
            {"$set": changes},
            return_document=ReturnDocument.AFTER,
        )

    @staticmethod
    async def soft_delete(subject_id: str) -> Optional[Dict[str, Any]]:
        now = _now()
        return await subjects_collection.find_one_and_update(
            {"_id": subject_id, "is_active": True},
            {"$set": {"is_active": False, "deleted_at": now, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )