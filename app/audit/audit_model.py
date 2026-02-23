from pydantic import BaseModel
from datetime import datetime
from typing import Optional, Dict, Any


class AuditEvent(BaseModel):
    entity_type: str
    entity_id: str
    action: str
    actor: str
    payload: Dict[str, Any]
    timestamp: datetime
    previous_hash: Optional[str] = None
    hash: str