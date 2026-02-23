from __future__ import annotations

from fastapi import APIRouter, Query
from typing import List, Optional

from app.subject.subject_model import SubjectCreate, SubjectUpdate, SubjectOut
from app.subject.subject_service import SubjectService

router = APIRouter(prefix="/subjects", tags=["Subjects"])


@router.post("", response_model=SubjectOut, status_code=201)
async def create_subject(body: SubjectCreate):
    return await SubjectService.create(body.model_dump())


@router.get("/{subject_id}", response_model=SubjectOut)
async def get_subject(
    subject_id: str,
    include_inactive: bool = Query(False),
):
    return await SubjectService.get(subject_id, include_inactive=include_inactive)


@router.get("", response_model=List[SubjectOut])
async def list_subjects(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    include_inactive: bool = Query(False),
    institution_id: Optional[str] = Query(default=None),
    kind: Optional[str] = Query(default=None, description="subject | evaluation"),
):
    return await SubjectService.list(limit=limit, skip=skip, include_inactive=include_inactive, institution_id=institution_id, kind=kind)


@router.patch("/{subject_id}", response_model=SubjectOut)
async def update_subject(subject_id: str, body: SubjectUpdate):
    changes = {k: v for k, v in body.model_dump().items() if v is not None}
    return await SubjectService.update(subject_id, changes)


@router.delete("/{subject_id}", response_model=SubjectOut)
async def delete_subject(subject_id: str):
    return await SubjectService.delete(subject_id)