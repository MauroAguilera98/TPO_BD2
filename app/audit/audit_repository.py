from app.db.cassandra import session
import json

class AuditRepository:

    

    @staticmethod
    def save_event(event):
        # session = session()
        payload_json = json.dumps(event.payload, sort_keys=True, default=str)
        query = """
        INSERT INTO audit_log (
            entity_type,
            entity_id,
            timestamp,
            action,
            actor,
            payload,
            previous_hash,
            hash
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """

        session.execute(query, (
            event.entity_type,
            event.entity_id,
            event.timestamp,
            event.action,
            event.actor,
            payload_json,
            event.previous_hash,
            event.hash
        ))

    @staticmethod
    def get_events(entity_type: str, entity_id: str, order: str = "DESC", limit: int = 100):
        order = order.upper()
        if order not in ("ASC", "DESC"):
            order = "DESC"

        query = f"""
        SELECT * FROM audit_log
        WHERE entity_type=%s AND entity_id=%s
        ORDER BY timestamp {order}
        LIMIT %s
        """
        return session.execute(query, (entity_type, entity_id, limit))
    
    # app/audit/audit_repository.py

    @staticmethod
    def get_last_hash(entity_type: str, entity_id: str):
        query = """
        SELECT hash
        FROM audit_log
        WHERE entity_type=%s AND entity_id=%s
        LIMIT 1
        """
        row = session.execute(query, (entity_type, entity_id)).one()
        return row.hash if row else None