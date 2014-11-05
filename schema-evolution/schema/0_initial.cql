DROP KEYSPACE IF EXISTS ts_data;

CREATE KEYSPACE IF NOT EXISTS ts_data WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE ts_data;

CREATE TABLE readings_by_month (
    array_id uuid,
    array_name text static,
    year int,
    month int,
    sensor text,
    measured_at timestamp,
    value float,
    unit text,
    location text,
    PRIMARY KEY ((array_id, year, month), sensor, measured_at)
) WITH CLUSTERING ORDER BY (sensor ASC, measured_at DESC);

