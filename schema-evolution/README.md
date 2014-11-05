# Cassandra Schema Evolution with Spark

## Problem

### Option 1

We have an application taking sensor readings and storing them in Cassandra. We've only been storing data from 2 sensor arrays to evaluate the performance of our data model. Using `nodetool cfhistograms` showed what several partitions have too many values.

We want to reduce the values per partition by using a difference partition key.

### Option 2

We have an application taking only 1 sensor reading and storing them in Cassandra. We are going to add new sensors to the network that will provide 3 readings per sensor.

We know that will increase the number of values per partition so we'd like to modify the table's partition key to reduce the possible values per partition.

## Steps

1. Analyze possible columns to add to partition key, thus reducing values
2. Create a table with that new partition key
3. Change driver to write to both old & new table
4. Bulk move data from old table into the new table (run as background, batch job)
5. Wait for move to complete
6. Change driver to write to *only* new table
7. Truncate old table, drop old table
8. Run `nodetool cleanup` on cluster

### Old & New

#### Initial table:
```
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
```

#### New table:
```
CREATE TABLE reading_by_month_hour (
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
```

## How to do the bulk move?

The fourth step is easily stated but may not be straightforward in execution as you have a number of options:

* Hadoop job
* Hive/Shark or Spark SQL query
* Driver using paging
* Spark transformation

## Working with Spark


### case classes to represent tables

```scala

import java.util.Date
import java.util.UUID

case class ReadingsByMonth(arrayId: UUID, arrayName: String, year: Int,
                           month: Int, sensor: String, measuredAt: Date,
                           value: Float, unit: String)

case class ReadingsByMonthHour(arrayId: UUID, arrayName: String,
                               monthHour: Date, sensor: String,
                               measuredAt: Date, value: Float, unit: String)
```

file:///Users/andrewlenards/devel/ghf/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/api/index.html#com.datastax.spark.connector.rdd.CassandraRDD

input setting, number of partitions to fetch at a time - number of rows to fetch at a time...

CassandraRDD divides the dataset into smaller partitions, processed locally on every cluster node. A data partition consists of one or more contiguous token ranges. To reduce the number of roundtrips to Cassandra, every partition is fetched in batches. The following properties control the number of partitions and the fetch size:

- spark.cassandra.input.split.size: approx number of Cassandra partitions in a Spark partition, default 100000
- spark.cassandra.input.page.row.size: number of CQL rows fetched per roundtrip, default 1000




