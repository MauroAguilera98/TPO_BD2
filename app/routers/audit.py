from fastapi import APIRouter, Query
from typing import Literal
from app.audit.audit_service import AuditService

router = APIRouter(prefix="/audit", tags=["Audit"])

# 1. Agregamos 'async' a la función
@router.get("/{entity_type}/{entity_id}")
async def get_audit(
    entity_type: str,
    entity_id: str,
    order: Literal["asc", "desc"] = Query("desc"),
    limit: int = Query(100, ge=1, le=1000),
):
    # 2. Agregamos 'await' a la llamada
    return await AuditService.history(entity_type, entity_id, order=order, limit=limit)