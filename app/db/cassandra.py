from cassandra.cluster import Cluster

cluster = Cluster(["cassandra"])
session = cluster.connect("edugrade")

# si seteás keyspace:
# session.set_keyspace("edugrade")
