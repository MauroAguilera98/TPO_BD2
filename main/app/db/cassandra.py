from cassandra.cluster import Cluster

cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

session.execute("""
CREATE KEYSPACE IF NOT EXISTS edugrade
WITH replication = {'class':'SimpleStrategy','replication_factor':1}
""")

session.set_keyspace("edugrade")

session.execute("""
CREATE TABLE IF NOT EXISTS audit_log (
    student_id text,
    event_time timestamp,
    action text,
    hash text,
    PRIMARY KEY (student_id, event_time)
) WITH CLUSTERING ORDER BY (event_time DESC)
""")

session.execute("""
CREATE TABLE IF NOT EXISTS grades_by_country_year (
    country text,
    year int,
    student_id text,
    grade double,
    PRIMARY KEY ((country, year), student_id)
)
""")

