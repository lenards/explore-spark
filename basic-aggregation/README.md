
# Basic Aggregation of Cassandra data w/ Spark 

*Note: this code is meant for demonstration purposes only. This is not product ready code & must be treated as such.*

## Setup

## REPL Exploration 

Note: REPL & *"Spark shell"* are meant to be interchangeable... 

We're going to start the *Spark shell*, but we want to request some addition resources:

```
$ dse spark --total-executor-cores 4 --executor-memory 4g
```

The version of the *Spark shell* you have invoked includes the [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector) on the `CLASSPATH`. 

This means that we have some addition commands in the *Spark shell* (commands in the Scala REPL and *Spark shell* are prefixed by a colon): 

This will show us the schema of the Cassandra cluster that we're operting with: 

```
scala> :showSchema ticker tick
========================================
 Keyspace: ticker
========================================
 Table: tick
----------------------------------------
 - ticker_symbol  : String         (partition key column)
 - date_hour      : String         (partition key column)
 - trade_occurred : java.util.Date (clustering column)
 - trade_agent    : String         (clustering column)
 - alpha          : Float          
 - gamma          : Float          
 - price          : Float          
 - quantity       : Int           
```

Again, because the [Spark Cassandra Connector](https://github.com/datastax/spark-cassandra-connector) is included we have functions superimposed on the *Spark shell*'s `SparkContext`, referenced as `sc`, that allow us to get data from Cassandra given a keyspace and a table: 

```
scala> val tickRDD = sc.cassandraTable("ticker", "tick")
tickRDD: com.datastax.spark.connector.rdd.CassandraRDD[com.datastax.spark.connector.CassandraRow] = CassandraRDD[0] at RDD at CassandraRDD.scala:48
```

Remember, all RDD transformations are lazily evaluated. They will not be computed until an **action** on an RDD is encountered. 

We can force an **action** to peek inside:

```
scala> tickRDD.first()
res4: com.datastax.spark.connector.CassandraRow = CassandraRow{ticker_symbol: PBH, date_hour: 2015-02-13_12, trade_occurred: 2015-02-13 05:24:18-0700, trade_agent: TraderY, alpha: 0.12, gamma: 4.31, price: 39.37, quantity: 700}
```

We're going to going to calculate a *rollup*, or an aggregation of a series of tick instances (in CQL, you'd think of them as parititon given the schema for ``ticker.tick`` that contains all the trades for a symbol in a given hour of a trade date). The idea being, that each *tick* is contains a grouping of stock trades made within that hour for a symbol by one or many traders.

We're going to reshaping the data from a ``CassandraRow`` to a *pair* so that we can perform PairRDD operations on it. So the row will become a tuple or two tuples. We're aiming for this: 

```
((date_hour, ticker_symbol, trade_agent), (price, quantity))
```

In Scala types, you can also think of it as: ``((String, String, String), (Float, Int))``

Why? A PairRDD has operations like ``reduceByKey`` and ``combineByKey``. If we form the row such that the tuple will have primary key, it will allow us to combine all of the instance of `(price * quantity, quantity)` for then saving into the target table, ``ticker.tick_rollup_by_datehour``.

We'll do this in steps: `rollup1, rollup2, rollup3` 

*(each being an RDD transformation from the previous)*

```
scala> val rollup1 = tickRDD.map { r => 
     |    (
     |      (r.getString("date_hour"), r.getString("ticker_symbol"), 
     |       r.getString("trade_agent")),
     |      (r.getFloat("price"), r.getInt("quantity"))
     |    )
     | }
rollup1: org.apache.spark.rdd.RDD[((String, String, String), (Float, Int))] = MappedRDD[1] at map at <console>:61
```

What's that look like?  We can trigger the calculation to see:
```
scala> rollup1.first()
res5: ((String, String, String), (Float, Int)) = ((2015-02-13_12,PBH,TraderY),(39.37,700))
```

Instinct would say we'd want to group-by-key and then do an aggregation. In Spark, we have want to make use of the most efficient operations. For PairRDDs, we can do `combineByKey` and `reduceByKey` operations that will reduce the number of "shuffles" across the network. In general, we want to avoid using `groupByKey` if we're just going to [aggregate what we've grouped together](http://spark.apache.org/docs/1.2.0/programming-guide.html#transformations). 

So, `combineByKey` is chosen here because we want to operate on tuple in-place. 

```
scala> val rollup2 = rollup1.combineByKey(
     |        (metrics) => (metrics._1 * metrics._2, metrics._2), 
     |        (t:(Float,Int), r:(Float,Int)) => (t._1 + r._1, t._2 + r._2),
     |        (lhs:(Float,Int), rhs:(Float,Int)) => 
     |                                   (lhs._1 + rhs._1, lhs._2 + rhs._2)
     | )
rollup2: org.apache.spark.rdd.RDD[((String, String, String), (Float, Int))] = ShuffledRDD[2] at combineByKey at <console>:63
```

The transformation `combineByKey` takes the 3 arguments: 

* createCombiner
* mergeValue
* mergeCombiner

With the first argument, we're producing the first value for combining. Then, we're merging values. Finally, we're doing the combiner merge. 

There are [some examples](There are some ) out there in Python using `combineByKey` as well

The elements of the `rollup2` RDD are in the form: 

```
((String, String, String), (Float, Int))
```

To save to Cassandra, the RDD either needs to be in tuple form (with field position to column name mapping via ``SomeColumns``) or a ``case class`` (that has field names that match the CQL table). So we'll take the current tuple and map over it as translation to prepare to persist this to Cassandra.

```
scala> val rollup3 = rollup2.map { r => (r._1._1, r._1._2, r._1._3, r._2._1, r._2._2) }
rollup3: org.apache.spark.rdd.RDD[(String, String, String, Float, Int)] = MappedRDD[3] at map at <console>:65
```

We can verify that we're looking good by triggering the computation with an action (we're using ``first``):

```
scala> rollup3.first()
res6: (String, String, String, Float, Int) = (2015-02-13_10,PBH,TraderZ,5912.9995,150)
```

For known-small datasets (datasets that won't overwhelm the REPL), we can ``collect`` and then print them out for visual inspection (if desired):

```
scala> rollup3.collect.foreach(println)
(2015-02-13_10,PBH,TraderZ,5912.9995,150)
(2015-02-13_09,PBH,TraderY,27756.06,3400)
(2015-02-13_10,CL,TraderZ,30596.6,540)
(2015-02-13_10,CHD,TraderX,81235.49,3670)
....
```

Again, we can look at the schema of the target CQL table:

```
scala> :showSchema ticker tick_rollup_by_datehour
========================================
 Keyspace: ticker
========================================
 Table: tick_rollup_by_datehour
----------------------------------------
 - date_hour       : String (partition key column)
 - ticker_symbol   : String (partition key column)
 - trade_agent     : String (clustering column)
 - total_cost      : Float  
 - total_trade_amt : Float 
```

Now, we just need to provide that "positional field to column name mapping":

```
scala> rollup3.saveToCassandra("ticker", "tick_rollup_by_datehour", 
     |         SomeColumns("date_hour", "ticker_symbol", 
     |                     "trade_agent", "total_cost", "total_trade_amt"))
```

And, we can verify with `cqlsh` that we've persisted the data:

```
cqlsh:ticker> select * from tick_rollup_by_datehour 
          ... ; 

 date_hour     | ticker_symbol | trade_agent | total_cost | total_trade_amt
---------------+---------------+-------------+------------+-----------------
 2015-02-13_10 |            CL |     TraderR |      22271 |             420
 2015-02-13_10 |            CL |     TraderX |      17422 |             350
 2015-02-13_10 |            CL |     TraderY |      13947 |             300
 2015-02-13_10 |            CL |     TraderZ |      30597 |             540
 2015-02-13_13 |            CL |     TraderR |      22340 |             740
 2015-02-13_13 |            CL |     TraderX |      17491 |             600
 2015-02-13_13 |            CL |     TraderY |      14017 |             500
 2015-02-13_13 |            CL |     TraderZ |      30666 |             980
 2015-02-13_12 |           PBH |     TraderX |     5899.5 |             150
 2015-02-13_12 |           PBH |     TraderY |      27559 |             700
 2015-02-13_12 |           PBH |     TraderZ |     9837.5 |             250
 2015-02-13_13 |           PBH |     TraderR |      17698 |             450
 2015-02-13_13 |           PBH |     TraderX |      27953 |             710
 2015-02-13_13 |           PBH |     TraderY |      19680 |             500
 2015-02-13_13 |           PBH |     TraderZ |      25565 |             650
 2015-02-13_09 |           PBH |     TraderR |     9931.3 |             750
 2015-02-13_09 |           PBH |     TraderX |     6175.2 |            1050
 2015-02-13_09 |           PBH |     TraderY |      27756 |            3400
 2015-02-13_09 |           PBH |     TraderZ |     6317.5 |            3350
 2015-02-13_10 |           PBH |     TraderY |      19700 |             500
 2015-02-13_10 |           PBH |     TraderZ |       5913 |             150
 2015-02-13_11 |           PBH |     TraderX |     4009.8 |             450
 2015-02-13_11 |           PBH |     TraderY |      19690 |             500
 2015-02-13_11 |           PBH |     TraderZ |     5937.4 |             800
 2015-02-13_10 |           CHD |     TraderR |     9429.8 |            1940
 2015-02-13_10 |           CHD |     TraderX |      81235 |            3670
 2015-02-13_10 |           CHD |     TraderY |      71217 |            2290
 2015-02-13_10 |           CHD |     TraderZ |      21123 |            1600

(28 rows)
```