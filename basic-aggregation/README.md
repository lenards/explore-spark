
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


```
scala> val rollup2 = rollup1.combineByKey((metrics) => (metrics._1 * metrics._2, metrics._2), 
     |        (t:(Float,Int), r:(Float,Int)) => (t._1 + r._1, t._2 + r._2),
     |        (lhs:(Float,Int), rhs:(Float,Int)) => (lhs._1 + rhs._1, lhs._2 + rhs._2)
     | )
rollup2: org.apache.spark.rdd.RDD[((String, String, String), (Float, Int))] = ShuffledRDD[2] at combineByKey at <console>:63
```

```
scala> val rollup3 = rollup2.map { r => (r._1._1, r._1._2, r._1._3, r._2._1, r._2._2) }
rollup3: org.apache.spark.rdd.RDD[(String, String, String, Float, Int)] = MappedRDD[3] at map at <console>:65
```

```
scala> rollup3.first()
res6: (String, String, String, Float, Int) = (2015-02-13_10,PBH,TraderZ,5912.9995,150)
```

```
scala> rollup3.collect.foreach(println)
(2015-02-13_10,PBH,TraderZ,5912.9995,150)
(2015-02-13_09,PBH,TraderY,27756.06,3400)
(2015-02-13_10,CL,TraderZ,30596.6,540)
(2015-02-13_10,CHD,TraderX,81235.49,3670)
....
```

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

```
scala> rollup3.saveToCassandra("ticker", "tick_rollup_by_datehour", SomeColumns("date_hour", "ticker_symbol", 
     |   "trade_agent", "total_cost", "total_trade_amt"))
```

## A Packaged Application