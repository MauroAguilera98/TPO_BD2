from neo4j import AsyncGraphDatabase
import os

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
# Pon aquí la contraseña que usaron tus compañeros
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password") 

# El driver ahora es asíncrono
driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))