from cassandra.cluster import Cluster
import os
import asyncio

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")

# Inicializamos el cluster, pero NO bloqueamos el event loop con queries aún
cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()

async def init_cassandra_schema():
    """Ejecuta los DDL en un hilo separado solo durante el arranque."""
    queries = [
        """
        CREATE KEYSPACE IF NOT EXISTS edugrade
        WITH replication = {'class':'SimpleStrategy','replication_factor':1}
        """,
        """
        CREATE TABLE IF NOT EXISTS edugrade.audit_log (
            entity_type text,
            entity_id text,
            timestamp timestamp,
            action text,
            actor text,
            payload text,
            previous_hash text,
            hash text,
            PRIMARY KEY ((entity_type, entity_id), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
        """,
        """
        CREATE TABLE IF NOT EXISTS edugrade.grades_by_country_year (
            country text,
            year int,
            student_id text,
            grade float,
            PRIMARY KEY ((country, year), student_id)
        )
        """,
        """CREATE TABLE IF NOT EXISTS edugrade.subject_averages (
                subject text PRIMARY KEY,
                avg_grade float
            );
        """ 
    ]
    
    # Aseguramos el keyspace primero
    await asyncio.to_thread(session.execute, queries[0])
    session.set_keyspace("edugrade")
    
    # Ejecutamos las tablas
    for q in queries[1:]:
        await asyncio.to_thread(session.execute, q)
        
    print("✅ Esquema de Cassandra verificado/creado de forma segura.")

import asyncio

async def close_cassandra():
    """Apaga el cluster de Cassandra delegando el bloqueo a un hilo."""
    await asyncio.to_thread(cluster.shutdown)
    print("🔌 Conexión a Cassandra cerrada correctamente.")