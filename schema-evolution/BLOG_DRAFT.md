# Cassandra Schema Evolution with Spark

* Change Wording - Make it BETTER *
Changing the frequency with which you store data may change your data model. Cassandra users may find that they need to split up partitions given this new need. Since CQL does not currently a bulk operation like ``SELECT INTO`` (like in [Apache Hive]()), alternatives need to be used.

Integration with [Apache Spark]() offers an option for a variety of bulk operations that are short to write and provide solid throughput.

## Problem

Our application is taking in sensor data and storing it in Cassandra. The frequency of measuring from the individual sensors increased (from 60 seconds to 5 seconds). This increases the number of values per partition and changes the performance of our data model.

We want to reduce the values per partition. We can do this by changing the partition key (this can split up values per partition to a management amount).


## Solution

### Set the table (w/ current table schema)
### Discuss the code
### Show Schema change
### Outline the transform

## Putting It All Together

## Improvements

Read & write with `ConsistencyLevel.LOCAL_QUORUM`

Tweak partitions ... tweak values per partition