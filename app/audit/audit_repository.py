from app.db.cassandra import session
import json
import asyncio

class AuditRepository:

    @staticmethod
    async def save_event(event):
        payload_json = json.dumps(event.payload, sort_keys=True, default=str)
        query = """
        INSERT INTO edugrade.audit_log (
            entity_type, entity_id, timestamp, action,
            actor, payload, previous_hash, hash
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        params = (event.entity_type, event.entity_id, event.timestamp,
                  event.action, event.actor, payload_json,
                  event.previous_hash, event.hash)
        
        # Delegamos la llamada bloqueante a un hilo de fondo de forma segura
        await asyncio.to_thread(session.execute, query, params)

    @staticmethod
    async def get_last_hash(entity_type: str, entity_id: str):
        query = """
        SELECT hash FROM edugrade.audit_log
        WHERE entity_type=%s AND entity_id=%s LIMIT 1
        """
        # Ejecutamos de forma asíncrona y obtenemos el primer resultado
        result = await asyncio.to_thread(session.execute, query, (entity_type, entity_id))
        row = result.one()
        return row.hash if row else None
        
    @staticmethod
    async def get_events(entity_type: str, entity_id: str, order: str = "DESC", limit: int = 100):
        order = "ASC" if order.upper() == "ASC" else "DESC"
        query = f"""
        SELECT * FROM edugrade.audit_log
        WHERE entity_type=%s AND entity_id=%s
        ORDER BY timestamp {order} LIMIT %s
        """
        return await asyncio.to_thread(session.execute, query, (entity_type, entity_id, limit))