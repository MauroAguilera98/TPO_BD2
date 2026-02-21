from fastapi import APIRouter, Query
from typing import Literal
from app.audit.audit_service import AuditService

router = APIRouter(prefix="/audit", tags=["Audit"])

@router.get("/{entity_type}/{entity_id}")
def get_audit(
    entity_type: str,
    entity_id: str,
    order: Literal["asc", "desc"] = Query("desc"),   # default: últimos primero
    limit: int = Query(100, ge=1, le=1000),          # default 100, máx 1000 (ajustable)
):
    return AuditService.history(entity_type, entity_id, order=order, limit=limit)