from cassandra.cluster import Cluster

from cassandra_settings import cass_settings

# Init DB
cluster = Cluster(
    cass_settings['CASS_CONNECT_POINTS'],
    port=cass_settings['CASS_PORT']
)
session = cluster.connect()

# Creating KeySpace
keyspaceReq = "CREATE KEYSPACE IF NOT EXISTS {} WITH REPLICATION "
keyspaceReq += "= {{ 'class': '{}', 'replication_factor': {} }};"""
keyspaceReq = keyspaceReq.format(
    cass_settings['CASS_KEYSPACE'],
    cass_settings['CASS_STRATEGY'],
    cass_settings['CASS_REPL']
)

# Execution
session.execute(keyspaceReq)

# Creating Ais-position Table
aisPositionTableReq = """CREATE TABLE IF NOT EXISTS {}.ais_position (
    mmsi VARCHAR,
    trip_key VARCHAR,
    trip_number INT,
    timestamps TIMESTAMP,
    latitude FLOAT,
    longitude FLOAT,
    speed_over_ground INT,
    course_over_ground FLOAT,
    pac INT,
    heading FLOAT,
    adt TIMESTAMP,
    eta TIMESTAMP,
    ata TIMESTAMP,
    PRIMARY KEY ((mmsi, trip_key, trip_number), timestamps)
 ) WITH CLUSTERING ORDER BY (timestamps DESC);"""

aisPositionTableReq = aisPositionTableReq.format(cass_settings['CASS_KEYSPACE'])

# Execution
session.execute(aisPositionTableReq)

# Creating Trip_Table
tripTableReq = """CREATE TABLE IF NOT EXISTS {}.trip (
    trip_key VARCHAR,
    ship_mmsi VARCHAR,
    ship_nb_trip INT,
    departure_port VARCHAR,
    arrival_port VARCHAR,
    ett INT,
    PRIMARY KEY (trip_key, ship_mmsi, ship_nb_trip)
) WITH CLUSTERING ORDER BY (ship_mmsi ASC, ship_nb_trip DESC);"""

tripTableReq = tripTableReq.format(cass_settings['CASS_KEYSPACE'])

# Execution
session.execute(tripTableReq)

# Creating Ship Table
shipTableReq = """CREATE TABLE IF NOT EXISTS {}.ship (
    mmsi VARCHAR,
    name VARCHAR,
    country VARCHAR,
    date DATE,
    trip_key VARCHAR,
    trip_number INT,
    PRIMARY KEY (mmsi, trip_key, date)
) WITH CLUSTERING ORDER BY (trip_key ASC, date DESC);"""

shipTableReq = shipTableReq.format(cass_settings['CASS_KEYSPACE'])

# Execution
session.execute(shipTableReq)

# Shutdown du cluster
cluster.shutdown()
