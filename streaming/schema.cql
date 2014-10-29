DROP KEYSPACE IF EXISTS ts_data;

CREATE KEYSPACE IF NOT EXISTS ts_data WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ts_data;

CREATE TABLE measurements (
    sensor_net text,
    location text,
    grouping bigint,
    measured_at bigint,
    value float,
    local text,
    unit text,
    long float,
    lat float,
    PRIMARY KEY ((sensor_net, location, grouping), measured_at)
) WITH CLUSTERING ORDER BY (measured_at DESC);
