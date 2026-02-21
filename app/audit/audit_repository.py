from app.db.cassandra import session


class AuditRepository:

    @staticmethod
    def save_event(event):
        # session = session()

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
            str(event.payload),
            event.previous_hash,
            event.hash
        ))

    @staticmethod
    def get_events(entity_type, entity_id):
        # session = session()

        query = """
        SELECT * FROM audit_log
        WHERE entity_type=%s AND entity_id=%s
        """

        return session.execute(query, (entity_type, entity_id))