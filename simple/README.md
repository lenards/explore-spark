# Simple DSE Spark Application

* File Setup
* Build
* Submit

## File Setup

You will want DataStax Enterprise running. Start it with ``-k`` if you're working with the tarball (enabling Spark with a package install is a bit different - [see here for details](http://www.datastax.com/documentation/datastax_enterprise/4.5/datastax_enterprise/spark/sparkStart.html))

Example: ``./bin/dse cassandra -k``

We're going to need to add some data to the Cassandra Filesystem (CFS) that we will be reading when running our "simple" application. To follow along with the "Quick Start" on the [Apache Spark](https://spark.apache.org/docs/latest/quick-start.html) site, we'll use the ``README`` file that is included with DataStax Enterprise (DSE) for our initial test data.

Find wherever you'd installed DSE (I'm assuming you are working locally, so either the tarball or the Mac OS X DMG is how you've installed).

From DSE Install Home Directory, just verify the file is present:
```
$ ls -lha resources/spark/README-spark.txt
-rw-r--r--  1 andrewlenards  staff   4.1K Oct  5 22:05 resources/spark/README-spark.txt
$ wc -l resources/spark/README-spark.txt
     111 resources/spark/README-spark.txt
```

We can then use the `hadoop fs` command to *put* the data in CFS:
```
$ ./bin/dse hadoop fs -put resources/spark/README-spark.txt cfs://127.0.0.1/README.txt
14/10/22 13:23:30 INFO snitch.Workload: Setting my workload to Cassandra
```

We can also verify that it is in CFS:
```
$ ./bin/dse hadoop fs -ls /
14/10/22 12:06:55 INFO snitch.Workload: Setting my workload to Cassandra
Found 3 items
-rwxrwxrwx   1 andrewlenards staff       4215 2014-10-22 10:37 /README.txt
-rwxrwxrwx   1 andrewlenards staff       4215 2014-10-22 10:33 /tmp
drwxrwxrwx   - andrewlenards staff          0 2014-10-22 10:33 /user
```

Then, we can start the *Spark Shell* with the following
```
./bin/dse spark
...
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.1.0
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_67)
Type in expressions to have them evaluated.
Type :help for more information.
Creating SparkContext...
Created spark context..
Spark context available as sc.
Type in expressions to have them evaluated.
Type :help for more information.

scala> val readme = sc.textFile("/README.txt")
readme: org.apache.spark.rdd.RDD[String] = /README.txt MappedRDD[1] at textFile at <console>:63

scala> readme.count()
res2: Long = 111
```

*(Remember that `:quit` will exit the Spark Shell)*

## Build

```
sbt/sbt package
```

The output will looking something like this:
```
Launching sbt from sbt/sbt-launch-0.13.1.jar
[info] Set current project to Simple Spark Project (in build file:/Users/andrewlenards/tmp/explore-spark/simple/)
[info] Compiling 1 Scala source to /Users/andrewlenards/tmp/explore-spark/simple/target/scala-2.10/classes...
[info] Packaging /Users/andrewlenards/tmp/explore-spark/simple/target/scala-2.10/simple-spark-project_2.10-0.1.jar ...
[info] Done packaging.
[success] Total time: 4 s, completed Oct 22, 2014 1:18:54 PM
```

## Submit

```
./bin/dse spark-submit --class "SimpleApp" \
 $PATH_TO_REPO/explore-spark/simple/target/scala-2.10/simple-spark-project_2.10-0.1.jar
```

The output for the *README* file we put in CFS should give the following output:
```
Lines with Apache: 8, Lines with Spark: 15
```
