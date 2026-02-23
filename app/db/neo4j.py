from neo4j import AsyncGraphDatabase
import os

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password") 

# El driver ahora es asíncrono
driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

async def init_neo4j_schema():
    """Crea los índices y restricciones automáticamente al iniciar la API."""
    queries = [
        "CREATE CONSTRAINT IF NOT EXISTS FOR (s:Student) REQUIRE s.id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (g:Grade) REQUIRE g.grade_id IS UNIQUE;",
        "CREATE INDEX IF NOT EXISTS FOR (i:Institution) ON (i.name);",
        "CREATE INDEX IF NOT EXISTS FOR (sub:Subject) ON (sub.name);",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (sub:Subject) REQUIRE sub.id IS UNIQUE;",
        "CREATE CONSTRAINT IF NOT EXISTS FOR (i:Institution) REQUIRE i.id IS UNIQUE;",
    ]
    
    # Abrimos una sesión y ejecutamos cada query de forma asíncrona
    async with driver.session() as session:
        for q in queries:
            try:
                await session.run(q)
            except Exception as e:
                print(f"⚠️ Nota de Neo4j: {e}")
                
    print("✅ Índices y Restricciones de Neo4j verificados/creados.")
    
    async def close_neo4j_driver():
        """Cierra el pool de conexiones asíncronas de Neo4j de forma segura."""
    await driver.close()
    print("🔌 Conexión a Neo4j cerrada correctamente.")

    