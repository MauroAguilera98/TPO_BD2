import asyncio
import aiohttp
import random
import time
from faker import Faker

fake = Faker()

# Configuración del estrés
API_URL = "http://localhost:8000/grades"
TOTAL_RECORDS = 1_000_000
BATCH_SIZE = 500  # Procesamos de a 10 mil para no saturar la RAM de Python
CONCURRENCY_LIMIT = 200 # Límite de peticiones simultáneas a la API

# Catálogos para generar datos variados
COUNTRIES = ["AR", "US", "UK", "DE"]
INSTITUTIONS = {
    "AR": ["UADE", "UBA", "UTN", "ITBA"],
    "US": ["MIT", "Stanford", "Harvard", "UCLA"],
    "UK": ["Oxford", "Cambridge", "Imperial College", "UCL"],
    "DE": ["TUM", "LMU", "RWTH Aachen", "TU Berlin"]
}
SUBJECTS = ["Matemáticas", "Física", "Historia", "Literatura", "Programación", "Bases de Datos"]

def generate_random_grade(country: str) -> dict:
    """Genera una calificación coherente según el país."""
    if country == "AR":
        return {"scale": "1-10", "value": round(random.uniform(4.0, 10.0), 2)}
    elif country == "US":
        return {"scale": "GPA", "value": round(random.uniform(2.0, 4.0), 2)}
    elif country == "UK":
        return {"scale": "A-Levels", "value": random.choice(["A*", "A", "B", "C", "D"])}
    else: # DE
        return {"scale": "1-6 (Inversa)", "value": round(random.uniform(1.0, 4.0), 1)}

def create_payload():
    """Genera un JSON alineado con el esquema validado por la API."""
    country = random.choice(COUNTRIES)
    # Generamos los campos requeridos en la raíz del objeto
    return {
        "student_id": f"STU-{random.randint(10000, 999999)}",
        "grade_id": f"GRD-{random.getrandbits(32)}", # Campo requerido que faltaba
        "year": random.choice([2023, 2024, 2025, 2026]), # Movido a la raíz
        "country": country,
        "institution": random.choice(INSTITUTIONS[country]),
        "subject": random.choice(SUBJECTS),
        "original_grade": generate_random_grade(country),
        "metadata": {
            "term": random.choice(["S1", "S2"]),
            "eval_type": random.choice(["Final", "Parcial", "Coursework"])
        }
    }

async def send_request(session, payload, semaphore):
    """Envía la petición controlada por el semáforo."""
    async with semaphore:
        try:
            async with session.post(API_URL, json=payload) as response:
                return response.status
        except Exception as e:
            return 500

async def process_batch(session, batch_size, semaphore):
    """Procesa un lote de peticiones concurrentes."""
    tasks = [send_request(session, create_payload(), semaphore) for _ in range(batch_size)]
    results = await asyncio.gather(*tasks)
    return results

async def main():
    print(f"🚀 Iniciando carga masiva de {TOTAL_RECORDS} registros...")
    start_time = time.time()
    
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
    
    # Configuramos el cliente HTTP para mantener las conexiones abiertas (Connection Pooling)
    connector = aiohttp.TCPConnector(limit=CONCURRENCY_LIMIT)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        records_processed = 0
        
        while records_processed < TOTAL_RECORDS:
            current_batch_size = min(BATCH_SIZE, TOTAL_RECORDS - records_processed)
            
            # Ejecutamos el lote
            results = await process_batch(session, current_batch_size, semaphore)
            
            # Calculamos métricas
            success_count = sum(1 for r in results if r == 200)
            records_processed += current_batch_size
            
            elapsed_time = time.time() - start_time
            req_per_sec = records_processed / elapsed_time
            
            print(f"📊 Progreso: {records_processed}/{TOTAL_RECORDS} | "
                  f"Éxitos: {success_count}/{current_batch_size} | "
                  f"Velocidad: {req_per_sec:.2f} req/s")

    total_time = time.time() - start_time
    print(f"✅ Carga masiva completada en {total_time:.2f} segundos.")

if __name__ == "__main__":
    asyncio.run(main())