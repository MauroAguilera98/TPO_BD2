from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional
from uuid import uuid4

from annotated_types import doc
from fastapi import HTTPException

from app.audit.audit_service import AuditService
from app.grade.grade_repository import GradeRepository
from app.institution.institution_repository import InstitutionRepository
from app.student.student_repository import StudentRepository
from app.subject.subject_repository import SubjectRepository
from app.db.mongo import institutions_collection, subjects_collection


def _now() -> datetime:
    return datetime.now(timezone.utc)


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
    out["grade_id"] = out.pop("_id")
    return out


class GradeService:
    @staticmethod
    async def create(payload: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        """
        1) valida referencias (student/institution/subject) en Mongo
        2) inserta grade en Mongo (source of truth)
        3) audita: grade + evento espejo en student
        (Neo4j y agregados RF4 se manejan fuera del service por ahora)
        """
        student_id = payload["student_id"]
        institution_id = payload["institution_id"]
        subject_id = payload["subject_id"]

        # --- Validación referencial (consistencia)
        student = await StudentRepository.get(student_id, include_inactive=False)
        if not student:
            raise HTTPException(status_code=404, detail="student_id no existe o está inactivo")

        inst = await InstitutionRepository.get(institution_id, include_inactive=False)
        if not inst:
            raise HTTPException(status_code=404, detail="institution_id no existe o está inactiva")

        subj = await SubjectRepository.get(subject_id, include_inactive=False)
        if not subj:
            raise HTTPException(status_code=404, detail="subject_id no existe o está inactiva")

        # (Opcional recomendado) consistencia catálogo: subject pertenece a institution
        if subj.get("institution_id") and subj.get("institution_id") != institution_id:
            raise HTTPException(status_code=422, detail="subject_id no pertenece a institution_id")

        now = _now()
        issued_at: datetime = payload.get("issued_at") or now

        # --- Derivados (evita inconsistencias)
        year = issued_at.year
        country = (inst.get("country") or "").upper()
        if not country:
            raise HTTPException(status_code=400, detail="Institution sin country")
        system = (inst.get("system") or payload.get("original_grade", {}).get("scale") or "").upper() or None

        grade_id = str(uuid4())

        doc = {
            "_id": grade_id,
            "student_id": student_id,
            "institution_id": institution_id,
            "subject_id": subject_id,
            "original_grade": payload["original_grade"],
            "issued_at": issued_at,
            "year": year,
            "country": country,
            "system": system,
            "assessment_type": payload.get("assessment_type"),
            "attempt": payload.get("attempt"),
            "raw": payload.get("raw", {}),
            "metadata": payload.get("metadata", {}),
            "created_at": now,
        }

        await GradeRepository.create(doc)

        # --- Auditoría (RF5): evento de la grade
        await AuditService.register_event(
            entity_type="grade",
            entity_id=grade_id,
            action="CREATE",
            actor=actor,
            payload={"snapshot": _serialize(doc)},
        )

        # --- Auditoría espejo en student (útil para /audit/student/{id})
        await AuditService.register_event(
            entity_type="student",
            entity_id=student_id,
            action="GRADE_CREATED",
            actor=actor,
            payload={
                "grade_id": grade_id,
                "subject_id": subject_id,
                "institution_id": institution_id,
                "value": doc["original_grade"]["value"],
                "scale": doc["original_grade"]["scale"],
                "issued_at": doc["issued_at"].isoformat(),
            },
        )

        return _mongo_to_api(doc)
    @staticmethod
    async def get(grade_id: str) -> Dict[str, Any]:
        doc = await GradeRepository.get(grade_id)
        if not doc:
            raise HTTPException(status_code=404, detail="grade no encontrada")
        return _mongo_to_api(doc)

    @staticmethod
    async def list_by_student(student_id: str, limit: int = 50, skip: int = 0):
        st = await StudentRepository.get(student_id, include_inactive=False)
        if not st:
            raise HTTPException(status_code=404, detail="student_id no existe o está inactivo")

        docs = await GradeRepository.list_by_student(student_id, limit=limit, skip=skip)
        return [_mongo_to_api(d) for d in docs]
    
    @staticmethod
    async def correct(grade_id: str, payload: Dict[str, Any], actor: str = "system") -> Dict[str, Any]:
        old = await GradeRepository.get(grade_id)
        if not old:
            raise HTTPException(status_code=404, detail="grade no encontrada")

        student_id = old["student_id"]
        institution_id = old["institution_id"]
        subject_id = old["subject_id"]

        # institución activa (para derivar country/system)
        inst = await institutions_collection.find_one({"_id": institution_id, "deleted": {"$ne": True}})
        if not inst:
            raise HTTPException(status_code=404, detail="institution_id no existe o está inactiva")

        # subject activo (y pertenencia a institution, si tu modelo lo exige)
        subj = await subjects_collection.find_one({"_id": subject_id, "deleted": {"$ne": True}})
        if not subj:
            raise HTTPException(status_code=404, detail="subject_id no existe o está inactiva")
        if subj.get("institution_id") != institution_id:
            raise HTTPException(status_code=400, detail="subject_id no pertenece a institution_id")

        issued_at = payload.get("issued_at") or old.get("issued_at") or _now()
        year = issued_at.year

        country = (old.get("country") or inst.get("country") or "").upper()
        if not country:
            raise HTTPException(status_code=400, detail="Institution sin country")

        system = (old.get("system") or inst.get("system") or (payload.get("original_grade") or {}).get("scale") or "").upper() or None

        new_id = str(uuid4())

        meta_old = old.get("metadata") or {}
        meta_new = payload.get("metadata") or {}
        reason = payload.get("reason")

        doc = {
            "_id": new_id,
            "student_id": student_id,
            "institution_id": institution_id,
            "subject_id": subject_id,
            "original_grade": payload["original_grade"],
            "issued_at": issued_at,
            "year": year,
            "country": country,
            "system": system,
            "correction_of": grade_id,
            "version": int(old.get("version", 1)) + 1,
            "metadata": {**meta_old, **meta_new, "correction_of": grade_id, "reason": reason},
            # mantené tu patrón de hash (si tenés uno específico, reemplazá esta línea por tu lógica)
            "immutable_hash": str(uuid4()),
            "created_at": _now(),
        }

        await GradeRepository.create(doc)

        # Auditoría (best-effort)
        try:
            await AuditService.register_event(
                entity_type="grade",
                entity_id=new_id,
                action="CORRECTION_CREATE",
                actor=actor,
                payload={"previous_grade_id": grade_id},
            )
            await AuditService.register_event(
                entity_type="grade",
                entity_id=grade_id,
                action="SUPERSEDED",
                actor=actor,
                payload={"new_grade_id": new_id},
            )
            await AuditService.register_event(
                entity_type="student",
                entity_id=student_id,
                action="GRADE_CORRECTED",
                actor=actor,
                payload={"previous_grade_id": grade_id, "new_grade_id": new_id},
            )
        except Exception:
            pass

        return _mongo_to_api(doc)