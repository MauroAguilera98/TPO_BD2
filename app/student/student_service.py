from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from fastapi import HTTPException

from app.student.student_repository import StudentRepository
from app.audit.audit_service import AuditService


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize(obj: Any) -> Any:
    """
    Convierte datetimes (y estructuras anidadas) a formatos JSON-friendly.
    Sirve para payloads de auditoría.
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_serialize(v) for v in obj]
    return obj


def _mongo_to_api_student(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mongo guarda el ID en _id. La API expone student_id.
    """
    out = dict(doc)
    out["student_id"] = out.pop("_id")
    return out


class StudentService:
    @staticmethod
    async def create(payload: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        """
        Crea estudiante en Mongo:
        - _id = student_id (STU-...)
        - trajectories: mínimo 1 (ya validado por modelo)
        - agrega trajectory_id y created_at a cada trayectoria
        - registra auditoría CREATE
        """
        student_id = payload["student_id"]

        # Evita duplicados (aunque esté "borrado lógico", no lo recreamos con el mismo ID)
        existing = await StudentRepository.get(student_id, include_inactive=True)
        if existing:
            raise HTTPException(status_code=409, detail="student_id ya existe")

        now = _now()

        # Enriquecer trayectorias con metadata server-side
        trajectories_out = []
        for t in payload.get("trajectories", []):
            trajectories_out.append({
                **t,
                "trajectory_id": f"trj_{uuid4().hex}",
                "created_at": now,
            })

        doc = {
            "_id": student_id,
            "full_name": payload["full_name"],
            "email": payload.get("email"),
            "trajectories": trajectories_out,
            "is_active": True,
            "created_at": now,
            "updated_at": now,
            "deleted_at": None,
        }

        await StudentRepository.create(doc)

        # Auditoría (sync) -> threadpool
        await AuditService.register_event(
            entity_type="student",
            entity_id=student_id,
            action="CREATE",
            actor=actor,
            payload={"snapshot": _serialize(doc)},
)

        return _mongo_to_api_student(doc)

    @staticmethod
    async def get(student_id: str, include_inactive: bool = False) -> Dict[str, Any]:
        doc = await StudentRepository.get(student_id, include_inactive=include_inactive)
        if not doc:
            raise HTTPException(status_code=404, detail="student no encontrado")
        return _mongo_to_api_student(doc)

    @staticmethod
    async def update_profile(student_id: str, changes: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        """
        Actualiza solo perfil (full_name/email). NO toca trajectories.
        Registra auditoría UPDATE.
        """
        # Defensa extra por si alguien manda cosas indebidas
        allowed = {"full_name", "email"}
        safe_changes = {k: v for k, v in changes.items() if k in allowed}

        updated = await StudentRepository.update_profile(student_id, safe_changes)
        if not updated:
            raise HTTPException(status_code=404, detail="student no encontrado o inactivo")

        await AuditService.register_event(
            entity_type="student",
            entity_id=student_id,
            action="UPDATE",
            actor=actor,
            payload={
                "changes": _serialize(safe_changes),
                "snapshot": _serialize(updated),
    },
)

        return _mongo_to_api_student(updated)

    @staticmethod
    async def add_trajectory(student_id: str, trajectory_in: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        """
        Append-only: agrega una trayectoria al array.
        Registra auditoría TRAJECTORY_ADD.
        """
        now = _now()
        trajectory_out = {
            **trajectory_in,
            "trajectory_id": f"trj_{uuid4().hex}",
            "created_at": now,
        }

        updated = await StudentRepository.add_trajectory(student_id, trajectory_out)
        if not updated:
            raise HTTPException(status_code=404, detail="student no encontrado o inactivo")

        await AuditService.register_event(
            entity_type="student",
            entity_id=student_id,
            action="TRAJECTORY_ADD",
            actor=actor,
            payload={"trajectory": _serialize(trajectory_out)},
        )

        return _mongo_to_api_student(updated)

    @staticmethod
    async def delete(student_id: str, actor: str = "system") -> Dict[str, Any]:
        """
        Baja lógica (soft delete).
        Registra auditoría DELETE.
        """
        deleted = await StudentRepository.soft_delete(student_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="student no encontrado o inactivo")

        await AuditService.register_event(
            entity_type="student",
            entity_id=student_id,
            action="DELETE",
            actor=actor,
            payload={"snapshot": _serialize(deleted)},
        )

        return _mongo_to_api_student(deleted)
    @staticmethod
    async def update_expected_end_year(student_id: str, trajectory_id: str, expected_end_year: int, actor: str = "system"):
        student = await StudentRepository.get(student_id, include_inactive=False)
        if not student:
            raise HTTPException(status_code=404, detail="student no encontrado o inactivo")

        traj = next((t for t in student.get("trajectories", []) if t.get("trajectory_id") == trajectory_id), None)
        if not traj:
            raise HTTPException(status_code=404, detail="trajectory_id no encontrada para este estudiante")

        start_year = traj.get("start_year")
        if start_year is not None and expected_end_year < start_year:
            raise HTTPException(status_code=422, detail="expected_end_year debe ser >= start_year")

        old_val = traj.get("expected_end_year")

        updated = await StudentRepository.update_expected_end_year(student_id, trajectory_id, expected_end_year)
        if not updated:
            raise HTTPException(status_code=404, detail="student no encontrado o inactivo")

        await AuditService.register_event(
            entity_type="student",
            entity_id=student_id,
            action="TRAJECTORY_PLAN_UPDATED",
            actor=actor,
            payload={
                "trajectory_id": trajectory_id,
                "old_expected_end_year": old_val,
                "new_expected_end_year": expected_end_year,
            },
        )

        # convertir _id -> student_id para API (si ya tenés helper, usalo)
        updated["student_id"] = str(updated.pop("_id"))
        return updated