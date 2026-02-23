import json
import asyncio
from app.db.redis_client import redis_client
# Asumimos que el módulo audit ya tiene un service para Cassandra
from app.audit.audit_service import AuditService 

class ConversionService:
    @staticmethod
    async def convert_grade(req, to_system: str, version: str, background_tasks):
        # 1. Definición de llaves para Redis
        cache_key = f"converted:{req.student_id}:{req.subject_id}:{to_system}:{version}"
        rule_key = f"rule:{req.from_system}:{to_system}:{version}"

        # 2. Buscar en caché (Evita recalcular)
        cached_result = await redis_client.get(cache_key)
        if cached_result:
            return json.loads(cached_result)

        # 3. Cache Miss: Obtener la regla y calcular
        rule_data = await redis_client.get(rule_key)
        
        # Lógica temporal hasta que las reglas estén cargadas en BD
        multiplier = 0.8 if to_system == "US" else 1.2
        if rule_data:
            multiplier = json.loads(rule_data).get("multiplier", 1.0)

        converted_value = round(req.original_value * multiplier, 2)

        result_payload = {
            "original_value": req.original_value,
            "converted_value": converted_value,
            "target_scale": to_system,
            "rule_version": version,
            "cached": True
        }

        # 4. Guardar resultado en Redis (TTL 24hs)
        await redis_client.setex(cache_key, 86400, json.dumps(result_payload))
        result_payload["cached"] = False

        # 5. Auditoría en Cassandra (Asíncrono para no frenar la API)
        background_tasks.add_task(
            asyncio.to_thread,
            AuditService.register_conversion, # Nacho debe tener este método
            req.student_id, req.subject_id, version, req.original_value, converted_value
        )

        return result_payload