from datetime import datetime
import json
from app.audit.audit_repository import AuditRepository
from app.audit.hash_chain import generate_hash
from app.audit.audit_model import AuditEvent
from app.services.cache import get_cache, set_cache

class AuditService:

    @staticmethod
    async def register_event(entity_type, entity_id, action, actor, payload):
        cache_key = f"audit_hash:{entity_type}:{entity_id}"
        
        # 1. Caché Asíncrona
        previous_hash = await get_cache(cache_key)

        # 2. Base de Datos Asíncrona (si hay Cache Miss)
        if previous_hash is None:
            previous_hash = await AuditRepository.get_last_hash(entity_type, entity_id)

        event_data = {
            "entity_type": entity_type, "entity_id": entity_id,
            "action": action, "actor": actor, "payload": payload,
            "timestamp": datetime.utcnow().isoformat()
        }

        hash_value = generate_hash(event_data, previous_hash)

        event = AuditEvent(
            entity_type=entity_type, entity_id=entity_id, action=action,
            actor=actor, payload=payload, timestamp=datetime.utcnow(),
            previous_hash=previous_hash, hash=hash_value
        )

        # 3. Guardado Asíncrono
        await AuditRepository.save_event(event)
        
        # 4. Actualización de Caché Asíncrona
        await set_cache(cache_key, hash_value)

        return event

    @staticmethod
    async def history(entity_type: str, entity_id: str, order: str = "desc", limit: int = 100):
        # Mismo código que tenías, pero con await en el repositorio
        events = await AuditRepository.get_events(entity_type, entity_id, order, limit)
        out = []
        for e in events:
            try:
                payload = json.loads(e.payload) if e.payload else {}
            except Exception:
                payload = e.payload

            out.append({
                "entity_type": e.entity_type, 
                "entity_id": e.entity_id,
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                "action": e.action, 
                "actor": e.actor, 
                "payload": payload,              
                "previous_hash": e.previous_hash, 
                "hash": e.hash,
            })
        return out