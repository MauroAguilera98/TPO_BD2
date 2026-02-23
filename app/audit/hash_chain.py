import hashlib
import json


def generate_hash(payload, previous_hash):
    raw = json.dumps(payload, sort_keys=True) + str(previous_hash)
    return hashlib.sha256(raw.encode()).hexdigest()


# Esto crea la cadena de integridad.