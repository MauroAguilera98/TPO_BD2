# seed_students.py
import argparse
import asyncio
import json
import random
import time
from typing import Any, Dict, List, Tuple

import aiohttp

DEFAULT_BASE_URL = "http://localhost:8000"


def student_id(i: int) -> str:
    # STU-\d{5,12}
    return f"STU-{i:06d}"


def rand_name() -> str:
    first = ["Ana", "Juan", "Luis", "Sofia", "Mateo", "Valentina", "Emma", "Noah", "Mia", "Leo", "Martina", "Pedro"]
    last = ["Perez", "Gomez", "Rodriguez", "Fernandez", "Lopez", "Diaz", "Sanchez", "Romero", "Suarez", "Alvarez"]
    return f"{random.choice(first)} {random.choice(last)}"


async def fetch_all(session: aiohttp.ClientSession, base_url: str, path: str, limit: int = 200) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    skip = 0
    while True:
        url = f"{base_url}{path}?limit={limit}&skip={skip}"
        async with session.get(url) as r:
            r.raise_for_status()
            chunk = await r.json()
        if not chunk:
            break
        out.extend(chunk)
        skip += limit
    return out


async def post_json(
    session: aiohttp.ClientSession,
    url: str,
    payload: Dict[str, Any],
    sem: asyncio.Semaphore,
    retries: int = 2,
) -> Tuple[bool, int, str]:
    """
    Returns: (ok, status_code, err)
    ok=True si 2xx o 409 (ya existe).
    """
    async with sem:
        last_err = ""
        for attempt in range(retries + 1):
            try:
                async with session.post(url, json=payload) as r:
                    txt = await r.text()
                    if r.status in (200, 201):
                        return True, r.status, ""
                    if r.status == 409:
                        return True, r.status, ""  # ya existe (re-run friendly)
                    last_err = f"HTTP {r.status}: {txt}"
            except Exception as e:
                last_err = str(e)

            await asyncio.sleep(0.2 * (attempt + 1))  # backoff simple

        return False, 0, last_err


def pick_trajectory(inst: Dict[str, Any]) -> Dict[str, Any]:
    country = (inst.get("country") or "").upper()
    name = inst.get("name") or "Unknown"

    start_year = random.randint(2018, 2025)
    expected_end = start_year + random.choice([3, 4, 5])

    return {
        "country": country,
        "institution": name,  # tu modelo usa nombre, no ID
        "level": "Undergrad",
        "start_year": start_year,
        "expected_end_year": expected_end,
        "end_year": None,
        "status": "ongoing",
    }


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", type=str, default=DEFAULT_BASE_URL)
    p.add_argument("--n", type=int, default=20000)
    p.add_argument("--start-index", type=int, default=1, help="para reanudar: empieza en STU-(start_index)")
    p.add_argument("--concurrency", type=int, default=80)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--out", type=str, default="students_ids.json")
    p.add_argument("--email-ratio", type=float, default=0.3)
    args = p.parse_args()

    base_url = args.base_url.rstrip("/")
    sem = asyncio.Semaphore(args.concurrency)
    timeout = aiohttp.ClientTimeout(total=args.timeout)

    total = args.n
    processed = 0
    ok_total = 0
    created_total = 0
    fail_total = 0

    EMA_SPEED = None
    alpha = 0.25
    start_ts = time.perf_counter()

    print(f"Iniciando seed masivo de {total} students vía API...")

    async with aiohttp.ClientSession(timeout=timeout) as session:
        institutions = await fetch_all(session, base_url, "/institutions")
        if not institutions:
            raise RuntimeError("No hay instituciones. Corré seed_catalog_massive.py primero.")

        by_country: Dict[str, List[Dict[str, Any]]] = {}
        for inst in institutions:
            c = (inst.get("country") or "").upper()
            by_country.setdefault(c, []).append(inst)

        countries = [c for c in by_country.keys() if by_country[c]]
        if not countries:
            raise RuntimeError("Instituciones sin country válido.")

        # guardamos ids OK (2xx o 409)
        ok_ids: List[str] = []

        tasks: List[Tuple[str, asyncio.Task]] = []

        async def flush_batch(batch_start_ts: float, batch_size_expected: int):
            nonlocal processed, ok_total, created_total, fail_total, EMA_SPEED, tasks

            results = await asyncio.gather(*[t for _, t in tasks])
            batch_end_ts = time.perf_counter()
            batch_secs = max(batch_end_ts - batch_start_ts, 1e-9)

            ok_batch = 0
            created_batch = 0
            fail_batch = 0

            for (sid, _), (ok, status, err) in zip(tasks, results):
                if ok:
                    ok_batch += 1
                    ok_total += 1
                    ok_ids.append(sid)
                    if status in (200, 201):
                        created_batch += 1
                        created_total += 1
                else:
                    fail_batch += 1
                    fail_total += 1
                    if fail_total <= 5:
                        print("❌ Error student:", err)

            processed += batch_size_expected

            inst_speed = batch_size_expected / batch_secs
            EMA_SPEED = inst_speed if EMA_SPEED is None else (alpha * inst_speed + (1 - alpha) * EMA_SPEED)

            print(
                f"Progreso: {processed}/{total} | Éxitos API: {ok_batch}/{batch_size_expected} | Velocidad: {EMA_SPEED:.2f} req/s",
                flush=True,
            )

            tasks = []

        end_index = args.start_index + total - 1

        for i in range(args.start_index, end_index + 1):
            sid = student_id(i)

            c = random.choice(countries)
            inst = random.choice(by_country[c])

            email = None
            if random.random() < float(args.email_ratio):
                email = f"{sid.lower()}@example.com"

            payload = {
                "student_id": sid,
                "full_name": rand_name(),
                "email": email,
                "trajectories": [pick_trajectory(inst)],
            }

            if not tasks:
                batch_start_ts = time.perf_counter()

            tasks.append((sid, asyncio.create_task(post_json(session, f"{base_url}/students", payload, sem))))

            if len(tasks) >= args.batch_size:
                await flush_batch(batch_start_ts, batch_size_expected=len(tasks))

        if tasks:
            await flush_batch(time.perf_counter(), batch_size_expected=len(tasks))

    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(ok_ids, f)

    elapsed = max(time.perf_counter() - start_ts, 1e-9)
    print(f"✅ Fin students: ok={ok_total} created={created_total} fail={fail_total} | promedio={ok_total/elapsed:.2f} req/s")
    print(f"✅ Guardado en {args.out} (ids OK: {len(ok_ids)})")


if __name__ == "__main__":
    asyncio.run(main())