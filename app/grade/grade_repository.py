from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.db.mongo import grades_collection


class GradeRepository:
    @staticmethod
    async def create(doc: Dict[str, Any]) -> Dict[str, Any]:
        await grades_collection.insert_one(doc)
        return doc

    @staticmethod
    async def get(grade_id: str) -> Optional[Dict[str, Any]]:
        return await grades_collection.find_one({"_id": grade_id})

    @staticmethod
    async def list_by_student(student_id: str, limit: int = 50, skip: int = 0) -> List[Dict[str, Any]]:
        cursor = (
            grades_collection.find({"student_id": student_id})
            .sort("issued_at", -1)
            .skip(skip)
            .limit(limit)
        )
        return await cursor.to_list(length=limit)

    @staticmethod
    async def list(limit: int = 50, skip: int = 0) -> List[Dict[str, Any]]:
        cursor = grades_collection.find({}).sort("issued_at", -1).skip(skip).limit(limit)
        return await cursor.to_list(length=limit)