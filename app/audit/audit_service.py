from datetime import datetime
from app.audit.audit_repository import AuditRepository
from app.audit.hash_chain import generate_hash
from app.audit.audit_model import AuditEvent


class AuditService:

    @staticmethod
    def register_event(entity_type, entity_id, action, actor, payload):

        previous_hash = None

        events = AuditRepository.get_events(entity_type, entity_id)

        for e in events:
            previous_hash = e.hash
            break

        event_data = {
            "entity_type": entity_type,
            "entity_id": entity_id,
            "action": action,
            "actor": actor,
            "payload": payload,
            "timestamp": datetime.utcnow().isoformat()
        }

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

        AuditRepository.save_event(event)

        return event

    @staticmethod
    def history(entity_type, entity_id):
        events = AuditRepository.get_events(entity_type, entity_id)
        return list(events)