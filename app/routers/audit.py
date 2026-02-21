from fastapi import APIRouter
from app.audit.audit_service import AuditService

router = APIRouter(prefix="/audit", tags=["Audit"])


@router.get("/{entity_type}/{entity_id}")
def get_audit(entity_type: str, entity_id: str):
    return AuditService.history(entity_type, entity_id)