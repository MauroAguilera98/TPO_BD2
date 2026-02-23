# bulk_grades.py
import argparse
import asyncio
import json
import os
import random
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import aiohttp

DEFAULT_BASE_URL = "http://localhost:8000"

UK_LETTERS = ["A*", "A", "B", "C", "D", "E", "F"]


def parse_inst_country(inst_id: str) -> str:
    # "INS-AR-0001" -> "AR"
    try:
        return inst_id.split("-")[1].upper()
    except Exception:
        return "AR"


def pick_term() -> str:
    return random.choice(["S1", "S2", "Q1", "Q2", ""])


def rand_grade_value(system: str, uk_ratio: float = 0.0):
    s = (system or "AR").upper()

    if s == "UK" and random.random() < uk_ratio:
        return random.choice(UK_LETTERS)

    if s == "US":
        # GPA 0-4 con sesgo al centro
        v = random.gauss(3.0, 0.7)
        v = min(max(v, 0.0), 4.0)
        return round(v, 2)

    if s == "DE":
        # 1 (mejor) .. 6 (peor) sesgo hacia 2-3
        v = random.gauss(2.5, 0.9)
        v = min(max(v, 1.0), 6.0)
        return round(v, 1)

    # AR default 0-10 sesgo a 6-8
    v = random.gauss(7.0, 1.5)
    v = min(max(v, 0.0), 10.0)
    return round(v, 2)


async def post_grade(
    session: aiohttp.ClientSession,
    url: str,
    payload: Dict[str, Any],
    sem: asyncio.Semaphore,
    retries: int = 2,
) -> Tuple[bool, int, str]:
    """
    ok=True si 2xx.
    Retry SOLO para: timeouts/excepciones, 5xx, 429.
    """
    async with sem:
        last_err = ""
        for attempt in range(retries + 1):
            try:
                async with session.post(url, json=payload) as r:
                    txt = await r.text()
                    if r.status in (200, 201):
                        return True, r.status, ""
                    if r.status in (429, 500, 502, 503, 504):
                        last_err = f"HTTP {r.status}: {txt}"
                    else:
                        # 4xx de validación no se reintenta
                        return False, r.status, f"HTTP {r.status}: {txt}"
            except Exception as e:
                last_err = str(e)

            # backoff con jitter
            await asyncio.sleep((0.3 * (attempt + 1)) + random.random() * 0.2)

        return False, 0, last_err


def atomic_write_json(path: str, data: Dict[str, Any]):
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--base-url", type=str, default=DEFAULT_BASE_URL)
    p.add_argument("--total", type=int, default=1_000_000)
    p.add_argument("--batch", type=int, default=500)
    p.add_argument("--concurrency", type=int, default=80)
    p.add_argument("--timeout", type=int, default=60)
    p.add_argument("--retries", type=int, default=2)

    p.add_argument("--students", type=str, default="students_ids.json")
    p.add_argument("--catalog", type=str, default="catalog_snapshot.json")

    p.add_argument("--year-from", type=int, default=2020)
    p.add_argument("--year-to", type=int, default=2026)

    p.add_argument("--uk-ratio", type=float, default=0.0, help="proporción de notas UK como letras (0..1)")
    p.add_argument("--seed", type=int, default=None, help="seed random para reproducibilidad")

    p.add_argument("--checkpoint", type=str, default="bulk_progress.json")
    p.add_argument("--failed-out", type=str, default="bulk_failed.ndjson")
    args = p.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    base_url = args.base_url.rstrip("/")
    grades_url = f"{base_url}/grades"

    if not os.path.exists(args.students):
        raise RuntimeError(f"No existe {args.students}. Corré seed_students.py primero.")
    if not os.path.exists(args.catalog):
        raise RuntimeError(f"No existe {args.catalog}. Corré seed_catalog_massive.py primero.")

    with open(args.students, "r", encoding="utf-8") as f:
        student_ids: List[str] = json.load(f)
    if not student_ids:
        raise RuntimeError("students_ids.json está vacío.")

    with open(args.catalog, "r", encoding="utf-8") as f:
        snap = json.load(f)

    subjects_by_inst: Dict[str, List[str]] = snap.get("subjects_by_institution") or {}
    inst_ids = [k for k, v in subjects_by_inst.items() if v]
    if not inst_ids:
        raise RuntimeError("catalog_snapshot.json no tiene subjects_by_institution usable.")

    sem = asyncio.Semaphore(args.concurrency)
    timeout = aiohttp.ClientTimeout(total=args.timeout)

    total = int(args.total)
    sent = 0
    ok_total = 0
    fail_total = 0

    EMA_SPEED: Optional[float] = None
    alpha = 0.25
    start_ts = time.perf_counter()

    print(f"Iniciando carga masiva de {total} registros vía API...")
    print(f"Batch={args.batch} | Concurrency={args.concurrency} | Retries={args.retries} | UK_ratio={args.uk_ratio}")

    # archivo de fallos (append)
    fail_fh = open(args.failed_out, "a", encoding="utf-8")

    async with aiohttp.ClientSession(timeout=timeout) as session:
        while sent < total:
            remaining = total - sent
            batch_n = min(args.batch, remaining)

            # construir batch de payloads
            payloads: List[Dict[str, Any]] = []
            for _ in range(batch_n):
                sid = random.choice(student_ids)
                inst_id = random.choice(inst_ids)
                sub_id = random.choice(subjects_by_inst[inst_id])

                # “sistema” lo inferimos del país del institution_id (INS-AR-xxxx)
                system = parse_inst_country(inst_id)

                year = random.randint(args.year_from, args.year_to)
                month = random.randint(1, 12)
                day = random.randint(1, 28)
                issued_at = datetime(year, month, day, tzinfo=timezone.utc)

                val = rand_grade_value(system, uk_ratio=float(args.uk_ratio))

                payloads.append(
                    {
                        "student_id": sid,
                        "institution_id": inst_id,
                        "subject_id": sub_id,
                        "original_grade": {"scale": system, "value": val},
                        "issued_at": issued_at.isoformat(),
                        "metadata": {"year": year, "term": pick_term()},
                    }
                )

            batch_start_ts = time.perf_counter()

            tasks = [
                asyncio.create_task(post_grade(session, grades_url, payload, sem, retries=args.retries))
                for payload in payloads
            ]
            results = await asyncio.gather(*tasks)

            batch_end_ts = time.perf_counter()
            batch_secs = max(batch_end_ts - batch_start_ts, 1e-9)

            ok_batch = 0
            for (ok, status, err), payload in zip(results, payloads):
                if ok:
                    ok_batch += 1
                    ok_total += 1
                else:
                    fail_total += 1
                    # guardamos el payload que falló para inspección/replay
                    fail_fh.write(json.dumps({"status": status, "error": err, "payload": payload}) + "\n")

            sent += batch_n

            inst_speed = batch_n / batch_secs
            EMA_SPEED = inst_speed if EMA_SPEED is None else (alpha * inst_speed + (1 - alpha) * EMA_SPEED)

            elapsed = max(batch_end_ts - start_ts, 1e-9)
            overall_speed = sent / elapsed

            print(
                f"Progreso: {sent}/{total} | Éxitos API: {ok_batch}/{batch_n} | "
                f"Velocidad: {EMA_SPEED:.2f} req/s | Promedio: {overall_speed:.2f} req/s",
                flush=True,
            )

            # checkpoint por batch
            atomic_write_json(
                args.checkpoint,
                {
                    "sent": sent,
                    "total": total,
                    "ok_total": ok_total,
                    "fail_total": fail_total,
                    "batch": args.batch,
                    "concurrency": args.concurrency,
                    "retries": args.retries,
                    "uk_ratio": args.uk_ratio,
                    "elapsed_seconds": elapsed,
                    "avg_req_s": overall_speed,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                },
            )

    fail_fh.close()
    elapsed = max(time.perf_counter() - start_ts, 1e-9)
    print(f"✅ FIN: total={total} ok={ok_total} fail={fail_total} | promedio={total/elapsed:.2f} req/s")
    print(f"✅ Checkpoint: {args.checkpoint} | Failed: {args.failed_out}")


if __name__ == "__main__":
    asyncio.run(main())