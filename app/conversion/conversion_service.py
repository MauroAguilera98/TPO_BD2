import json
from uuid import uuid4
from app.db.redis_client import redis_client
from app.audit.audit_service import AuditService

class ConversionService:
    @staticmethod
    async def convert_grade(req, to_system: str, version: str, background_tasks):
        to_system = (to_system or "").upper()
        from_system = (req.from_system or "").upper()

        # Normalizamos original_value para:
        # - cache key estable
        # - parse consistente
        def normalize_value(v):
            if isinstance(v, str):
                return v.strip().upper()
            # float/int
            return f"{float(v):.4f}"

        original_norm = normalize_value(req.original_value)

        # Cache key DEBE incluir original_value, si no mezclás conversiones distintas
        cache_key = f"converted:{req.student_id}:{req.subject_id}:{from_system}:{to_system}:{version}:{original_norm}"
        rule_key = f"rule:{from_system}:{to_system}:{version}"

        # 1) Cache hit
        cached_result = await redis_client.get(cache_key)
        if cached_result:
            payload = json.loads(cached_result)
            payload["cached"] = True
            return payload

        # 2) Cache miss: (opcional) obtener regla
        rule_data = await redis_client.get(rule_key)
        rule = json.loads(rule_data) if rule_data else {}
        mode = (rule.get("mode") or "pivot").lower()

        # --- UK letters mapping (A*..F) ---
        UK_TO_AR = {
            "A*": 10.0,
            "A": 9.0,
            "B": 8.0,
            "C": 7.0,
            "D": 6.0,
            "E": 5.0,
            "F": 4.0,
        }

        def ar_to_uk(ar: float) -> str:
            a = float(ar)
            if a >= 9.5:
                return "A*"
            if a >= 8.5:
                return "A"
            if a >= 7.5:
                return "B"
            if a >= 6.5:
                return "C"
            if a >= 5.5:
                return "D"
            if a >= 4.5:
                return "E"
            return "F"

        def to_ar(value, src: str) -> float:
            s = (src or "").upper()

            if s == "UK":
                if not isinstance(value, str):
                    raise ValueError("Para UK, original_value debe ser string (A*, A, B, C, D, E, F)")
                key = value.strip().upper()
                if key not in UK_TO_AR:
                    raise ValueError("UK inválido: usar A*, A, B, C, D, E, F")
                return UK_TO_AR[key]

            # sistemas numéricos
            try:
                v = float(value)
            except Exception:
                raise ValueError(f"Para {s}, original_value debe ser numérico")

            if s == "AR":
                if not (0.0 <= v <= 10.0):
                    raise ValueError("AR debe estar entre 0 y 10")
                return v

            if s == "US":  # GPA 0-4 -> AR 0-10
                if not (0.0 <= v <= 4.0):
                    raise ValueError("US (GPA) debe estar entre 0 y 4")
                return (v / 4.0) * 10.0

            if s == "DE":  # 1 (mejor) .. 6 (peor) -> AR 0-10
                if not (1.0 <= v <= 6.0):
                    raise ValueError("DE debe estar entre 1 y 6")
                return 10.0 - ((v - 1.0) * 2.0)  # DE1->10, DE6->0

            raise ValueError(f"from_system no soportado: {s}")

        def from_ar(ar: float, dst: str):
            t = (dst or "").upper()
            a = float(ar)

            if t == "UK":
                return ar_to_uk(a)

            if t == "AR":
                return round(a, 2)

            if t == "US":
                return round((a / 10.0) * 4.0, 2)

            if t == "DE":
                return round(6.0 - (a / 2.0), 1)  # AR10->1.0, AR0->6.0

            raise ValueError(f"to_system no soportado: {t}")

        try:
            if mode == "multiplier":
                # Solo aplica si ambos son numéricos (no UK)
                if from_system == "UK" or to_system == "UK":
                    # ignoramos multiplier, usamos pivot
                    ar_value = to_ar(req.original_value, from_system)
                    converted_value = from_ar(ar_value, to_system)
                else:
                    mult = float(rule.get("multiplier", 1.0))
                    converted_value = round(float(req.original_value) * mult, 2)
            else:
                # modo recomendado: pivot AR (cubre todos los pares)
                ar_value = to_ar(req.original_value, from_system)
                converted_value = from_ar(ar_value, to_system)

        except ValueError as e:
            from fastapi import HTTPException
            raise HTTPException(status_code=422, detail=str(e))

        payload = {
            "student_id": req.student_id,
            "subject_id": req.subject_id,
            "from_system": from_system,
            "to_system": to_system,
            "rule_version": version,
            "original_value": req.original_value,
            "converted_value": converted_value,
        }

        # 3) Guardar en Redis (TTL 24hs)
        await redis_client.setex(cache_key, 86400, json.dumps(payload))

        # 4) Auditoría (best-effort, fuera del request)
        conversion_id = str(uuid4())

        if background_tasks is not None:
            background_tasks.add_task(
                AuditService.register_event,
                "conversion",
                conversion_id,
                "GRADE_CONVERSION",
                "system",
                {
                    **payload,
                    "cache_key": cache_key,
                    "rule_key": rule_key,
                    "mode": mode,
                },
            )

        payload["cached"] = False
        payload["conversion_id"] = conversion_id
        return payload