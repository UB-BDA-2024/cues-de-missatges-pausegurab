from cassandra.cluster import Cluster

class CassandraClient:
    def __init__(self, hosts):
        self.cluster = Cluster(hosts,protocol_version=4)
        self.session = self.cluster.connect()
        self.session.execute("""CREATE KEYSPACE IF NOT EXISTS sensor WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}""")
        self.session.execute("USE sensor")
        self.session.execute("""CREATE TABLE IF NOT EXISTS temperature ( sensor_id INT, temperature DOUBLE, PRIMARY KEY (sensor_id, temperature));""")
        self.session.execute("""CREATE TABLE IF NOT EXISTS types ( type TEXT, sensor_id INT, PRIMARY KEY (type, sensor_id));""")
        self.session.execute("""CREATE TABLE IF NOT EXISTS battery ( sensor_id INT, battery_level DOUBLE, PRIMARY KEY (sensor_id ,battery_level));""")             

    def get_session(self):
        return self.session

    def close(self):
        self.cluster.shutdown()

    def execute(self, query):
        return self.get_session().execute(query)
