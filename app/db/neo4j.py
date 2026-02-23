from neo4j import AsyncGraphDatabase
import os

# Configuración desde variables de entorno
URI = os.getenv("NEO4J_URI", "bolt://edugrade_neo4j:7687")
USER = os.getenv("NEO4J_USER", "neo4j")
PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = AsyncGraphDatabase.driver(URI, auth=(USER, PASSWORD))

async def init_neo4j_schema():
    """
    Crea las restricciones de unicidad para asegurar que los IDs 
    no se dupliquen durante la carga masiva.
    """
    constraints = [
        "CREATE CONSTRAINT student_id_unique IF NOT EXISTS FOR (s:Student) REQUIRE s.id IS UNIQUE",
        "CREATE CONSTRAINT subject_id_unique IF NOT EXISTS FOR (sub:Subject) REQUIRE sub.id IS UNIQUE",
        "CREATE CONSTRAINT institution_id_unique IF NOT EXISTS FOR (i:Institution) REQUIRE i.id IS UNIQUE",
        "CREATE CONSTRAINT grade_id_unique IF NOT EXISTS FOR (g:Grade) REQUIRE g.grade_id IS UNIQUE"
    ]
    
    async with driver.session() as session:
        for query in constraints:
            await session.run(query)
    print("✅ Neo4j: Constraints de unicidad verificados/creados.")

async def close_neo4j():
    await driver.close()