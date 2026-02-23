# seed_catalog_massive.py
import argparse
import asyncio
import json
import random
import time
from typing import Any, Dict, List, Tuple, AsyncIterator, Optional

import aiohttp

DEFAULT_BASE_URL = "http://localhost:8000"


def build_institution_id(country: str, inst_idx: int) -> str:
    # INS-[A-Z]{2}-\d{4,12}
    return f"INS-{country}-{inst_idx:04d}"


def build_subject_id(country: str, inst_idx: int, subj_idx: int) -> str:
    # SUB-[A-Z]{2}-\d{4,12}
    n = inst_idx * 100 + subj_idx  # 0101..5020 si inst<=50 y subj<=20
    return f"SUB-{country}-{n:04d}"


async def fetch_all(
    session: aiohttp.ClientSession,
    base_url: str,
    path: str,
    limit: int = 200,
) -> List[Dict[str, Any]]:
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
                        return True, r.status, ""  # ya existe, ok para re-runs
                    last_err = f"HTTP {r.status}: {txt}"
            except Exception as e:
                last_err = str(e)

            await asyncio.sleep(0.2 * (attempt + 1))  # backoff simple

        return False, 0, last_err


async def iter_institutions(countries: List[str], inst_per_country: int) -> AsyncIterator[Dict[str, Any]]:
    for cc in countries:
        for i in range(1, inst_per_country + 1):
            yield {
                "institution_id": build_institution_id(cc, i),
                "name": f"{cc} University {i:03d}",
                "country": cc,
                "system": cc,
                "metadata": {"seed": "massive"},
            }


async def iter_subjects(
    countries: List[str],
    inst_per_country: int,
    subjects_per_inst: int,
    evaluation_ratio: float,
) -> AsyncIterator[Dict[str, Any]]:
    for cc in countries:
        for inst_idx in range(1, inst_per_country + 1):
            inst_id = build_institution_id(cc, inst_idx)
            for subj_idx in range(1, subjects_per_inst + 1):
                subject_id = build_subject_id(cc, inst_idx, subj_idx)
                kind = "evaluation" if random.random() < evaluation_ratio else "subject"
                yield {
                    "subject_id": subject_id,
                    "institution_id": inst_id,
                    "name": f"{cc} Subject {subj_idx:02d} (Inst {inst_idx:03d})",
                    "kind": kind,
                    "level": "Undergrad",
                    "credits": float(random.choice([4, 6, 8])),
                    "external_code": f"{cc}-S{subj_idx:02d}",
                    "metadata": {"seed": "massive"},
                }


async def run_posts_in_batches(
    session: aiohttp.ClientSession,
    base_url: str,
    path: str,
    payload_iter: AsyncIterator[Dict[str, Any]],
    sem: asyncio.Semaphore,
    batch_size: int,
    total: int,
    label: str,
) -> Tuple[int, int, int]:
    """
    Ejecuta POSTs en batches, con logging:
    Progreso: X/Y | Éxitos API: ok_batch/batch | Velocidad: EMA req/s
    """
    ok_total = created_total = fail_total = 0
    processed = 0

    EMA_SPEED: Optional[float] = None
    alpha = 0.25

    tasks: List[asyncio.Task] = []
    batch_start_ts: Optional[float] = None

    async def flush():
        nonlocal tasks, batch_start_ts, ok_total, created_total, fail_total, processed, EMA_SPEED

        results = await asyncio.gather(*tasks)
        batch_end_ts = time.perf_counter()

        batch_len = len(tasks)
        batch_secs = max(batch_end_ts - (batch_start_ts or batch_end_ts), 1e-9)

        ok_batch = 0
        created_batch = 0
        for _ok, status, err in results:
            if _ok:
                ok_total += 1
                ok_batch += 1
                if status in (200, 201):
                    created_total += 1
                    created_batch += 1
            else:
                fail_total += 1
                if fail_total <= 5:
                    print(f"❌ {label} error:", err)

        processed += batch_len

        inst_speed = batch_len / batch_secs
        EMA_SPEED = inst_speed if EMA_SPEED is None else (alpha * inst_speed + (1 - alpha) * EMA_SPEED)

        print(
            f"{label} | Progreso: {processed}/{total} | Éxitos API: {ok_batch}/{batch_len} | Velocidad: {EMA_SPEED:.2f} req/s",
            flush=True,
        )

        tasks = []
        batch_start_ts = None

    async for payload in payload_iter:
        if not tasks:
            batch_start_ts = time.perf_counter()
        tasks.append(asyncio.create_task(post_json(session, f"{base_url}{path}", payload, sem)))

        if len(tasks) >= batch_size:
            await flush()

    if tasks:
        await flush()

    return ok_total, created_total, fail_total


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", type=str, default=DEFAULT_BASE_URL)
    p.add_argument("--countries", type=str, default="AR,US,UK,DE")
    p.add_argument("--inst-per-country", type=int, default=50)
    p.add_argument("--subjects-per-inst", type=int, default=20)
    p.add_argument("--evaluation-ratio", type=float, default=0.2)
    p.add_argument("--concurrency", type=int, default=30)
    p.add_argument("--batch-size", type=int, default=500)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--out", type=str, default="catalog_snapshot.json")
    args = p.parse_args()

    base_url = args.base_url.rstrip("/")
    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
    sem = asyncio.Semaphore(args.concurrency)

    inst_total = len(countries) * args.inst_per_country
    subj_total = len(countries) * args.inst_per_country * args.subjects_per_inst

    timeout = aiohttp.ClientTimeout(total=args.timeout)

    print(f"Iniciando seed catálogo masivo vía API...")
    print(f"Institution target: {inst_total} | Subject target: {subj_total}")

    start_ts = time.perf_counter()

    async with aiohttp.ClientSession(timeout=timeout) as session:
        # 1) Institutions (streaming)
        ok, created, fail = await run_posts_in_batches(
            session=session,
            base_url=base_url,
            path="/institutions",
            payload_iter=iter_institutions(countries, args.inst_per_country),
            sem=sem,
            batch_size=args.batch_size,
            total=inst_total,
            label="Institutions",
        )
        print(f"Institutions FINAL: ok={ok} created={created} fail={fail}")

        # 2) Subjects (streaming)
        ok, created, fail = await run_posts_in_batches(
            session=session,
            base_url=base_url,
            path="/subjects",
            payload_iter=iter_subjects(countries, args.inst_per_country, args.subjects_per_inst, args.evaluation_ratio),
            sem=sem,
            batch_size=args.batch_size,
            total=subj_total,
            label="Subjects",
        )
        print(f"Subjects FINAL: ok={ok} created={created} fail={fail}")

        # 3) Snapshot para usar en bulk
        institutions = await fetch_all(session, base_url, "/institutions")
        subjects = await fetch_all(session, base_url, "/subjects")

        subjects_by_inst: Dict[str, List[str]] = {}
        for s in subjects:
            inst_id = s.get("institution_id")
            sid = s.get("subject_id")
            if inst_id and sid:
                subjects_by_inst.setdefault(inst_id, []).append(sid)

        snapshot = {
            "base_url": base_url,
            "countries": countries,
            "inst_per_country": args.inst_per_country,
            "subjects_per_inst": args.subjects_per_inst,
            "institutions_count": len(institutions),
            "subjects_count": len(subjects),
            "subjects_by_institution": subjects_by_inst,
        }

        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(snapshot, f)

        elapsed = max(time.perf_counter() - start_ts, 1e-9)
        print(f"✅ Snapshot guardado en {args.out}")
        print(f"✅ institutions={len(institutions)} subjects={len(subjects)} inst_with_subjects={len(subjects_by_inst)}")
        print(f"✅ Tiempo total: {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(main())