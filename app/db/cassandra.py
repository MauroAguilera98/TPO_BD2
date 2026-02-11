import os
from cassandra.cluster import Cluster

CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")

cluster = Cluster([CASSANDRA_HOST])
session = cluster.connect()
