from fastapi import FastAPI
from contextlib import asynccontextmanager

# Importamos los routers
from app.routers import grades, trajectory, reports, conversion, audit, students

# 1. Importaciones MONGO
from app.db.mongo import init_mongo_indices, close_mongo, client as mongo_client

# 2. Importaciones NEO4J
from app.db.neo4j import init_neo4j_schema, driver as neo4j_driver

# 3. Importaciones CASSANDRA
from app.db.cassandra import init_cassandra_schema, close_cassandra

# 4. Importaciones REDIS
from app.db.redis_client import close_redis

@asynccontextmanager
async def lifespan(app: FastAPI):
    # ==========================================
    # STARTUP: Fase de Encendido y Verificación
    # ==========================================
    print("🚀 Iniciando sistema EduGrade Global...")
    
    # Ejecutamos las inicializaciones asíncronas
    await init_neo4j_schema()
    await init_cassandra_schema()
    await init_mongo_indices()
    
    print("✅ Todas las infraestructuras NoSQL están listas.")
    
    # ==========================================
    # YIELD: La API está viva y recibe tráfico
    # ==========================================
    yield 
    
    # ==========================================
    # SHUTDOWN: Fase de Apagado Seguro (Graceful)
    # ==========================================
    print("🛑 Apagando el sistema. Liberando sockets y memoria...")
    
    # Neo4j (El driver asíncrono requiere await)
    await neo4j_driver.close()
    
    # Redis (Nuestra función asíncrona requiere await)
    await close_redis()
    
    # MongoDB (Motor cierra sus pools de forma síncrona/segura automáticamente al llamarlo)
    close_mongo()
    
    # Cassandra (Llamada según la opción que elegiste en el paso anterior)
    close_cassandra() # (O 'await close_cassandra()' si usaste asyncio.to_thread)
    
    print("✅ Conexiones cerradas limpiamente!")

app = FastAPI(title="EduGrade Global API", lifespan=lifespan)

app.include_router(grades.router)
app.include_router(trajectory.router)
app.include_router(reports.router)
app.include_router(conversion.router)
app.include_router(audit.router)
app.include_router(students.router)