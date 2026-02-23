from __future__ import annotations

from fastapi import APIRouter, Query
from typing import List

from app.equivalence.equivalence_model import EquivalenceCreate, EquivalenceDelete
from app.equivalence.equivalence_service import EquivalenceService

router = APIRouter(prefix="/equivalences", tags=["Equivalences"])


@router.post("", status_code=201)
async def create_equivalence(body: EquivalenceCreate):
    return await EquivalenceService.create(body.model_dump())


@router.get("/{subject_id}", response_model=List[dict])
async def list_equivalences(subject_id: str, depth: int = Query(1, ge=1, le=5)):
    return await EquivalenceService.list_for_subject(subject_id, depth=depth)


@router.delete("", status_code=200)
async def delete_equivalence(body: EquivalenceDelete):
    return await EquivalenceService.delete(body.model_dump())