from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import ReturnDocument

from app.db.mongo import students_collection


class StudentRepository:
    """
    Capa de persistencia (Mongo).
    No mete lógica de negocio ni auditoría.
    Devuelve documentos tal como están en Mongo (dict).
    """

    @staticmethod
    async def create(student_doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Inserta un estudiante.
        Espera que student_doc ya venga armado (incluyendo _id = STU-...).
        """
        await students_collection.insert_one(student_doc)
        return student_doc

    @staticmethod
    async def get(student_id: str, include_inactive: bool = False) -> Optional[Dict[str, Any]]:
        """
        Busca un estudiante por _id (student_id).
        Por defecto solo devuelve activos.
        """
        query: Dict[str, Any] = {"_id": student_id}
        if not include_inactive:
            query["is_active"] = True
        return await students_collection.find_one(query)

    @staticmethod
    async def list(limit: int = 50, skip: int = 0, include_inactive: bool = False) -> List[Dict[str, Any]]:
        """
        Lista estudiantes (paginado simple).
        Por defecto solo activos.
        """
        query: Dict[str, Any] = {}
        if not include_inactive:
            query["is_active"] = True

        cursor = students_collection.find(query).skip(skip).limit(limit)
        return await cursor.to_list(length=limit)

    @staticmethod
    async def update_profile(student_id: str, changes: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Actualiza campos del 'perfil' (por ejemplo full_name/email).
        NO toca trajectories (append-only).
        Devuelve el documento actualizado o None si no existe/está inactivo.
        """
        if not changes:
            return await StudentRepository.get(student_id)

        now = datetime.now(timezone.utc)
        changes = {**changes, "updated_at": now}

        return await students_collection.find_one_and_update(
            {"_id": student_id, "is_active": True},
            {"$set": changes},
            return_document=ReturnDocument.AFTER,
        )

    @staticmethod
    async def add_trajectory(student_id: str, trajectory_out: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Append-only: agrega una trayectoria al array trajectories.
        Devuelve el estudiante actualizado o None si no existe/está inactivo.
        """
        now = datetime.now(timezone.utc)

        return await students_collection.find_one_and_update(
            {"_id": student_id, "is_active": True},
            {
                "$push": {"trajectories": trajectory_out},
                "$set": {"updated_at": now},
            },
            return_document=ReturnDocument.AFTER,
        )

    @staticmethod
    async def soft_delete(student_id: str) -> Optional[Dict[str, Any]]:
        """
        Baja lógica: no borra el documento.
        Marca is_active=False y setea deleted_at.
        Devuelve el documento actualizado o None si no existe/ya estaba inactivo.
        """
        now = datetime.now(timezone.utc)

        return await students_collection.find_one_and_update(
            {"_id": student_id, "is_active": True},
            {"$set": {"is_active": False, "deleted_at": now, "updated_at": now}},
            return_document=ReturnDocument.AFTER,
        )
    @staticmethod
    async def update_expected_end_year(student_id: str, trajectory_id: str, expected_end_year: int):
        now = datetime.now(timezone.utc)

        return await students_collection.find_one_and_update(
            {"_id": student_id, "is_active": True},
            {
                "$set": {
                    "trajectories.$[t].expected_end_year": expected_end_year,
                    "updated_at": now,
                }
            },
            array_filters=[{"t.trajectory_id": trajectory_id}],
            return_document=ReturnDocument.AFTER,
        )