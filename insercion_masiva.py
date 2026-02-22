import asyncio
import aiohttp
import random
import time

# Configuración del estrés
API_URL = "http://localhost:8000/grades"
TOTAL_RECORDS = 1_000_000
BATCH_SIZE = 500        # Mantenemos lotes pequeños para no saturar RAM
CONCURRENCY_LIMIT = 20 # Reducido a 100 para compensar el doble query en Cassandra

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

def create_payload():
    """Genera un JSON perfectamente alineado con el nuevo modelo de la API."""
    country = random.choice(COUNTRIES)
    return {
        "student_id": f"STU-{random.randint(10000, 999999)}",
        # grade_id eliminado: ahora lo genera el backend
        "country": country,
        "institution": random.choice(INSTITUTIONS[country]),
        "subject": random.choice(SUBJECTS),
        "original_grade": generate_random_grade(country),
        "metadata": {
            "year": random.choice([2023, 2024, 2025, 2026]), # Movido aquí adentro
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

def print_database_stats():
    print("\n\n" + "="*60)
    print("🛑 CARGA DETENIDA. RECOPILANDO MÉTRICAS DE LOS CONTENEDORES...")
    print("="*60)

    try:
        # 1. Consultar MongoDB
        mongo_res = subprocess.run(
            ["docker", "exec", "edugrade_mongo", "mongosh", "edugrade", "--quiet", "--eval", "db.grades.countDocuments()"],
            capture_output=True, text=True
        )
        print(f"📊 MongoDB (Fuente de Verdad) : {mongo_res.stdout.strip()} documentos.")

        # 2. Consultar Redis
        redis_res = subprocess.run(
            ["docker", "exec", "edugrade_redis", "redis-cli", "DBSIZE"],
            capture_output=True, text=True
        )
        # Redis devuelve algo como "(integer) 50", lo limpiamos un poco
        redis_val = redis_res.stdout.strip().replace("(integer) ", "")
        print(f"📊 Redis (Caché de Hashes)  : {redis_val} claves activas.")

        # 3. Consultar Neo4j
        neo4j_res = subprocess.run(
            ["docker", "exec", "edugrade_neo4j", "cypher-shell", "-u", "neo4j", "-p", "password", "--format", "plain", "MATCH ()-[t:TOOK]->() RETURN count(t);"],
            capture_output=True, text=True
        )
        # cypher-shell en formato plain devuelve el título y luego el número. Tomamos la última línea.
        neo_lines = [line for line in neo4j_res.stdout.strip().split('\n') if line]
        neo_val = neo_lines[-1] if neo_lines else "0"
        print(f"📊 Neo4j (Trayectorias)     : {neo_val} relaciones académicas creadas.")

        # 4. Consultar Cassandra
        cass_res = subprocess.run(
            ["docker", "exec", "edugrade_cassandra", "cqlsh", "-e", "SELECT count(*) FROM edugrade.audit_log;"],
            capture_output=True, text=True
        )
        # Cassandra devuelve una tabla en formato texto. Buscamos la línea debajo de los guiones "---"
        cass_lines = cass_res.stdout.split('\n')
        cass_val = "Error al leer"
        for i, line in enumerate(cass_lines):
            if "---" in line and i + 1 < len(cass_lines):
                cass_val = cass_lines[i+1].strip()
                break
        print(f"📊 Cassandra (Auditoría)    : {cass_val} eventos inmutables registrados.")

    except Exception as e:
        print(f"❌ Error ejecutando comandos Docker: {e}")
        print("Asegúrate de que los contenedores estén corriendo.")

    print("="*60 + "\n")

if __name__ == "__main__":
    try:
        # Ejecuta la inyección masiva
        asyncio.run(main())
        # Si llega a 1,000,000 y termina naturalmente, también muestra las métricas
        print_database_stats()
    except KeyboardInterrupt:
        # Captura el Ctrl+C y muestra el resumen
        print_database_stats()