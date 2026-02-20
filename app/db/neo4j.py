import os
from neo4j import GraphDatabase

# En Docker, usamos el nombre del servicio 'neo4j' en lugar de 'localhost'
NEO4J_HOST = os.getenv("NEO4J_HOST", "neo4j")
NEO4J_URI = f"bolt://{NEO4J_HOST}:7687"
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))