USE ts_data;

CREATE TABLE readings_by_month_hour (
    array_id uuid,
    array_name text static,
    month_hour timestamp,
    sensor text,
    measured_at timestamp,
    value float,
    unit text,
    location text,
    PRIMARY KEY ((array_id, month_hour), sensor, measured_at)
) WITH CLUSTERING ORDER BY (sensor ASC, measured_at DESC);