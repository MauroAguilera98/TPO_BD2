import asyncio
import aiohttp
import random
import time

# Configuración del estrés
API_URL = "http://localhost:8000/grades"
TOTAL_RECORDS = 1_000_000
BATCH_SIZE = 500        # Mantenemos lotes pequeños para no saturar RAM
CONCURRENCY_LIMIT = 100 # Reducido a 100 para compensar el doble query en Cassandra

COUNTRIES = ["AR", "US", "UK", "DE"]
INSTITUTIONS = {
    "AR": ["UADE", "UBA", "UTN", "ITBA"],
    "US": ["MIT", "Stanford", "Harvard", "UCLA"],
    "UK": ["Oxford", "Cambridge", "Imperial College", "UCL"],
    "DE": ["TUM", "LMU", "RWTH Aachen", "TU Berlin"]
}
SUBJECTS = ["Matemáticas", "Física", "Historia", "Literatura", "Programación", "Bases de Datos"]

def generate_random_grade(country: str) -> dict:
    if country == "AR":
        return {"scale": "1-10", "value": round(random.uniform(4.0, 10.0), 2)}
    elif country == "US":
        return {"scale": "GPA", "value": round(random.uniform(2.0, 4.0), 2)}
    elif country == "UK":
        return {"scale": "A-Levels", "value": random.choice(["A*", "A", "B", "C", "D"])}
    else: # DE
        return {"scale": "1-6 (Inversa)", "value": round(random.uniform(1.0, 4.0), 1)}

# En lugar de 1 millón de posibilidades, usamos solo 50 alumnos fijos
def create_payload():
    country = random.choice(COUNTRIES)
    # Al usar un rango de solo 1 a 50, Redis se va a llenar rápido y empezará a "volar"
    student_id = f"STU-{random.randint(1, 50)}" 
    
    return {
        "student_id": student_id,
        "country": country,
        "institution": random.choice(INSTITUTIONS[country]),
        "subject": random.choice(SUBJECTS),
        "original_grade": generate_random_grade(country),
        "metadata": {
            "year": random.choice([2023, 2024, 2025, 2026]),
            "term": random.choice(["S1", "S2"]),
            "eval_type": random.choice(["Final", "Parcial", "Coursework"])
        }
    }

async def send_request(session, payload, semaphore):
    async with semaphore:
        try:
            async with session.post(API_URL, json=payload) as response:
                return response.status
        except Exception:
            return 500

async def process_batch(session, batch_size, semaphore):
    tasks = [send_request(session, create_payload(), semaphore) for _ in range(batch_size)]
    return await asyncio.gather(*tasks)

async def main():
    print(f"🚀 Iniciando carga masiva de {TOTAL_RECORDS} registros vía API...")
    start_time = time.time()
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        records_processed = 0
        
        while records_processed < TOTAL_RECORDS:
            current_batch_size = min(BATCH_SIZE, TOTAL_RECORDS - records_processed)
            results = await process_batch(session, current_batch_size, semaphore)
            
            success_count = sum(1 for r in results if r == 200)
            records_processed += current_batch_size
            
            elapsed_time = time.time() - start_time
            req_per_sec = records_processed / elapsed_time
            
            print(f"📊 Progreso: {records_processed}/{TOTAL_RECORDS} | "
                  f"Éxitos API: {success_count}/{current_batch_size} | "
                  f"Velocidad: {req_per_sec:.2f} req/s")

    print(f"✅ Carga masiva completada en {time.time() - start_time:.2f} segundos.")

if __name__ == "__main__":
    asyncio.run(main())