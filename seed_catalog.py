import asyncio
import aiohttp

BASE_URL = "http://localhost:8000"


INSTITUTIONS = [
    # Para los ejemplos "UADE / UTN"
    {
        "institution_id": "INS-AR-0001",
        "name": "UADE",
        "country": "AR",
        "system": "AR",
        "metadata": {"type": "university"},
    },
    {
        "institution_id": "INS-AR-0002",
        "name": "UTN",
        "country": "AR",
        "system": "AR",
        "metadata": {"type": "university"},
    },
    # Para la cadena indirecta (Uni A/B/C)
    {
        "institution_id": "INS-AR-0003",
        "name": "Uni A",
        "country": "AR",
        "system": "AR",
        "metadata": {"type": "university"},
    },
    {
        "institution_id": "INS-AR-0004",
        "name": "Uni B",
        "country": "AR",
        "system": "AR",
        "metadata": {"type": "university"},
    },
    {
        "institution_id": "INS-AR-0005",
        "name": "Uni C",
        "country": "AR",
        "system": "AR",
        "metadata": {"type": "university"},
    },
]

SUBJECTS = [
    # UADE
    {
        "subject_id": "SUB-AR-0101",
        "institution_id": "INS-AR-0001",
        "name": "Álgebra I (UADE)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },
    {
        "subject_id": "SUB-AR-0201",
        "institution_id": "INS-AR-0001",
        "name": "Análisis I (UADE)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },
    {
        "subject_id": "SUB-AR-0301",
        "institution_id": "INS-AR-0001",
        "name": "Programación I (UADE)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "cs"},
    },
    {
        "subject_id": "SUB-AR-0401",
        "institution_id": "INS-AR-0001",
        "name": "Matemática Discreta (UADE)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },

    # UTN
    {
        "subject_id": "SUB-AR-0102",
        "institution_id": "INS-AR-0002",
        "name": "Álgebra I (UTN)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },
    {
        "subject_id": "SUB-AR-0202",
        "institution_id": "INS-AR-0002",
        "name": "Análisis Matemático I (UTN)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },
    {
        "subject_id": "SUB-AR-0302",
        "institution_id": "INS-AR-0002",
        "name": "Programación (UTN)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "cs"},
    },
    {
        "subject_id": "SUB-AR-0402",
        "institution_id": "INS-AR-0002",
        "name": "Lógica y Estructuras Discretas (UTN)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },

    # Cadena indirecta (Uni A/B/C)
    {
        "subject_id": "SUB-AR-0501",
        "institution_id": "INS-AR-0003",
        "name": "Cálculo I (Uni A)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },
    {
        "subject_id": "SUB-AR-0502",
        "institution_id": "INS-AR-0004",
        "name": "Análisis I (Uni B)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },
    {
        "subject_id": "SUB-AR-0503",
        "institution_id": "INS-AR-0005",
        "name": "Matemática I (Uni C)",
        "kind": "subject",
        "credits": 6,
        "metadata": {"area": "math"},
    },
]


async def post_json(session: aiohttp.ClientSession, url: str, payload: dict) -> bool:
    async with session.post(url, json=payload) as resp:
        text = await resp.text()

        # Mostrar el identificador correcto (prioriza subject_id; si no existe, institution_id)
        ref = payload.get("subject_id") or payload.get("institution_id") or "?"

        # 201/200 OK, 409 ya existe (skip)
        if resp.status in (200, 201):
            print(f"✅ {resp.status} POST {url} -> {ref}")
            return True

        if resp.status == 409:
            print(f"↩️  {resp.status} POST {url} (ya existe) -> {ref}")
            return True

        print(f"❌ {resp.status} POST {url} -> {text}")
        return False


async def main():
    async with aiohttp.ClientSession() as session:
        # 1) Institutions
        for inst in INSTITUTIONS:
            ok = await post_json(session, f"{BASE_URL}/institutions", inst)
            if not ok:
                return

        # 2) Subjects
        for sub in SUBJECTS:
            ok = await post_json(session, f"{BASE_URL}/subjects", sub)
            if not ok:
                return

    print("\n🎉 Seed terminado. Ya podés crear equivalencias.")


if __name__ == "__main__":
    asyncio.run(main())