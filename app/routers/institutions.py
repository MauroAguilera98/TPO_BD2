from __future__ import annotations

from fastapi import APIRouter, Query
from typing import List

from app.institution.institution_model import InstitutionCreate, InstitutionUpdate, InstitutionOut
from app.institution.institution_service import InstitutionService

router = APIRouter(prefix="/institutions", tags=["Institutions"])


@router.post("", response_model=InstitutionOut, status_code=201)
async def create_institution(body: InstitutionCreate):
    return await InstitutionService.create(body.model_dump())


@router.get("/{institution_id}", response_model=InstitutionOut)
async def get_institution(
    institution_id: str,
    include_inactive: bool = Query(False, description="Si true, permite consultar instituciones dadas de baja lógica."),
):
    return await InstitutionService.get(institution_id, include_inactive=include_inactive)


@router.get("", response_model=List[InstitutionOut])
async def list_institutions(
    limit: int = Query(50, ge=1, le=200),
    skip: int = Query(0, ge=0),
    include_inactive: bool = Query(False),
):
    return await InstitutionService.list(limit=limit, skip=skip, include_inactive=include_inactive)


@router.patch("/{institution_id}", response_model=InstitutionOut)
async def update_institution(institution_id: str, body: InstitutionUpdate):
    changes = {k: v for k, v in body.model_dump().items() if v is not None}
    return await InstitutionService.update(institution_id, changes)


@router.delete("/{institution_id}", response_model=InstitutionOut)
async def delete_institution(institution_id: str):
    return await InstitutionService.delete(institution_id)