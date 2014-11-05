# Cassandra Schema Evolution with Spark

## Problem Statement *(pick one to focus on)*

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

We'll want to create types that model our source and target tables along with a function that can convert from the source to the target.

### Defining case classes to represent tables

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

### Creating a conversion function

```scala
    def convertTo(in: ReadingsByMonth): ReadingsByMonthHour = {
        // Change date to only have hours time component
        var cal = Calendar.getInstance()
        cal.setTime(in.measuredAt)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MINUTE, 0)

        return ReadingsByMonthHour(in.arrayId, in.arrayName, cal.getTime,
                                   in.sensor, in.measuredAt, in.value, in.unit,
                                   in.location)
    }
```

Just to quick review of our logic. We're splitting up the wide rows from our source table by including a date with only the *year, month, and hour* components for the *timestamp*. This will limit the number of rows within the partition to a more management amount.

Again, we can choose the granularity of the values contained in a partition by altering the resolution of the time component in our composite partition key.

### Putting it all together

With the DataStax Spark Cassandra Connector, we can create a Resiliant Distributed Data (RDD) that represents the data within our source table (``readings_by_month``). Then, we just ``map`` the conversation function over that dataset and we can save the results back to Cassandra with the ``saveToCassandra`` function (this function is added to RDDs when you ``import com.datastax.spark.connect._``).

Let's look at the ``main`` method:
```scala
    def main(args: Array[String]) {
        val conf = new SparkConf(true)
                    .set("spark.cassandra.connection.host", "127.0.0.1")
                    .setAppName("Schema Evolution Spark App")
        val sc = new SparkContext(conf)

        val tableData = sc.cassandraTable[ReadingsByMonth]("ts_data", "readings_by_month")
        val transformed = tableData.map(convertTo)
        transformed.saveToCassandra("ts_data", "reading_by_month_hour", )

        println("Done...")

        sc.stop()
    }
```

### Tuning for large datasets

input setting, number of partitions to fetch at a time - number of rows to fetch at a time...

#### Reading from Cassandra

file:///Users/andrewlenards/devel/ghf/spark-cassandra-connector/spark-cassandra-connector/target/scala-2.10/api/index.html#com.datastax.spark.connector.rdd.CassandraRDD

> CassandraRDD divides the dataset into smaller partitions, processed locally on every cluster
> node. A data partition consists of one or more contiguous token ranges. To reduce the number
> of roundtrips to Cassandra, every partition is fetched in batches. The following properties
> control the number of partitions and the fetch size:
>
> - `spark.cassandra.input.split.size`: approx number of Cassandra partitions in a Spark partition, default 100000
> - `spark.cassandra.input.page.row.size`: number of CQL rows fetched per roundtrip, default 1000


If we do nothing, we're reading the data at ``LOCAL_ONE``, this likely is not desirable. We should show how to change that ...

> By default, reads are performed at ConsistencyLevel.LOCAL_ONE in order to leverage
> data-locality and minimize network traffic. This read consistency level is controlled by
> the following property:
>
> - `spark.cassandra.input.consistency.level`: consistency level for RDD reads, string matching
> the ConsistencyLevel enum name.

#### Writing to Cassandra

You can pass an instance of ``WriteConf`` to ``saveToCassandra`` which allows you to set the ``batchSize``, ``consistencyLevel``, and ``parallelismLevel``

columns .. AllColumns - should is just a companion object, can simply be passed
WriteConf - want to change from ConsistencyLevel.LOCAL_ONE to LOCAL_QUORUM





