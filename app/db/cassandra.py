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
        """ ,
            #1) Idempotencia (evitar doble conteo si reintenta)
        """CREATE TABLE IF NOT EXISTS edugrade.grade_ledger_by_id (
            grade_id text PRIMARY KEY,
            country text,
            year int,
            student_id text,
            institution_id text,
            subject_id text,
            grade double,
            created_at timestamp
        );""",

        #2) Promedios por dimensión y año (country/year e institution/year con el mismo diseño)
        # dim: 'country' | 'institution'
        # dim_id: 'AR' o 'INS-AR-0001'
        """CREATE TABLE IF NOT EXISTS edugrade.stats_by_dim_year (
            dim text,
            dim_id text,
            year int,
            sum_milli counter,
            count_grade counter,
            PRIMARY KEY ((dim, dim_id, year))
        );""",

        # 3) Top estudiantes por país y año (promedio = sum/count)
        """CREATE TABLE IF NOT EXISTS edugrade.student_stats_by_country_year (
            country text,
            year int,
            student_id text,
            sum_milli counter,
            count_grade counter,
            PRIMARY KEY ((country, year), student_id)
        );""",

        # 4) Top materias por país y año
        """CREATE TABLE IF NOT EXISTS edugrade.subject_stats_by_country_year (
            country text,
            year int,
            subject_id text,
            sum_milli counter,
            count_grade counter,
            PRIMARY KEY ((country, year), subject_id)
        );""",

        # 5) Top materias global
        """CREATE TABLE IF NOT EXISTS edugrade.subject_stats_global (
            k text,                -- siempre 'ALL'
            subject_id text,
            sum_milli counter,
            count_grade counter,
            PRIMARY KEY (k, subject_id)
        );""",

        # 6) Distribución/histograma simple por país y año (bucket 0..10)
        """CREATE TABLE IF NOT EXISTS edugrade.grade_hist_by_country_year (
            country text,
            year int,
            bucket int,
            count counter,
            PRIMARY KEY ((country, year), bucket)
        );""",
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