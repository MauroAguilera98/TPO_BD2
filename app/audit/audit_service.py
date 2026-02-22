from datetime import datetime
import json
from app.audit.audit_repository import AuditRepository
from app.audit.hash_chain import generate_hash
from app.audit.audit_model import AuditEvent

# Importamos los métodos de Redis que tus compañeros ya prepararon
from app.services.cache import get_cache, set_cache


class AuditService:

    @staticmethod
    def register_event(entity_type, entity_id, action, actor, payload):
        
        # 1. Armamos la clave única para buscar en Redis
        cache_key = f"audit_hash:{entity_type}:{entity_id}"
        
        # 2. Intentamos obtener el hash desde la memoria RAM (Tarda ~1 ms)
        previous_hash = get_cache(cache_key)

        # 3. CACHE MISS: Solo si Redis NO lo tiene, vamos a sufrir la latencia de Cassandra
        if previous_hash is None:
            previous_hash = AuditRepository.get_last_hash(entity_type, entity_id)

        event_data = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "action": action,
            "actor": actor,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat()
        }

        # 4. Generamos la nueva firma criptográfica
        hash_value = generate_hash(event_data, previous_hash)

        event = AuditEvent(
            entity_type=entity_type,
            entity_id=entity_id,
            action=action,
            actor=actor,
            payload=payload,
            timestamp=datetime.utcnow(),
            previous_hash=previous_hash,
            hash=hash_value
        )

        # 5. Guardamos el evento inmutable en Cassandra
        AuditRepository.save_event(event)
        
        # 6. CACHE UPDATE: Actualizamos Redis con el hash NUEVO para la próxima petición
        set_cache(cache_key, hash_value)

        return event


    @staticmethod
    def history(entity_type: str, entity_id: str, order: str = "desc", limit: int = 100):
        order_db = "ASC" if order.lower() == "asc" else "DESC"
        events = AuditRepository.get_events(entity_type, entity_id, order=order_db, limit=limit)

        out = []
        for e in events:
            try:
                payload = json.loads(e.payload) if e.payload else {}
            except Exception:
                payload = e.payload  # por si quedó guardado como str(dict) viejo

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