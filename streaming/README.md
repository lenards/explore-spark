# Simple Spark Streaming Project

This project is a simple spark streaming application that reads from the HDFS-compliant Cassandra Filesystem (CFS). It creates a discretized stream from a directory within CFS and will process any new files that appear.

## Working with CFS

You'll need to use the `hadoop fs` tool via ``bin/dse`` for getting files into CFS. I'll provide examples along the way. We'll feed data into running application with this tool.

## Setup


```bash
$ ./bin/dse hadoop fs -put ~/tmp/explore-spark/streaming/sample.txt cfs://127.0.0.1/measures/00.txt
...
14/10/28 15:16:34 INFO snitch.Workload: Setting my workload to Cassandra
$ ./bin/dse hadoop fs -put ~/tmp/explore-spark/streaming/sample.txt cfs://127.0.0.1/measures/01.txt
...
14/10/28 15:16:42 INFO snitch.Workload: Setting my workload to Cassandra
$ ./bin/dse hadoop fs -put ~/tmp/explore-spark/streaming/sample.txt cfs://127.0.0.1/measures/02.txt


$ ./bin/dse hadoop fs -ls cfs://127.0.0.1/measures/
14/10/28 15:27:21 INFO snitch.Workload: Setting my workload to Cassandra
Found 4 items
-rwxrwxrwx   1 andrewlenards staff        299 2014-10-28 15:16 /measures/00.txt
-rwxrwxrwx   1 andrewlenards staff        299 2014-10-28 15:16 /measures/01.txt
-rwxrwxrwx   1 andrewlenards staff        299 2014-10-28 15:16 /measures/02.txt
-rwxrwxrwx   1 andrewlenards staff        299 2014-10-27 17:33 /measures/sample.txt


$ ./bin/dse hadoop fs -ls cfs://127.0.0.1/chkpt/
14/10/28 15:18:05 INFO snitch.Workload: Setting my workload to Cassandra
Found 16 items
drwxrwxrwx   - andrewlenards staff          0 2014-10-27 14:54 /chkpt/0baccce1-697e-4d11-bedb-205e76e5d11a
drwxrwxrwx   - andrewlenards staff          0 2014-10-27 14:39 /chkpt/3e585bca-c267-439a-beff-2acff4f9c05b
drwxrwxrwx   - andrewlenards staff          0 2014-10-27 16:23 /chkpt/7859b2a2-a9c8-4e5f-9610-c88cc3f5db42
drwxrwxrwx   - andrewlenards staff          0 2014-10-27 16:24 /chkpt/7d08b526-fcde-4a4f-b427-566da6055a2a
drwxrwxrwx   - andrewlenards staff          0 2014-10-27 14:21 /chkpt/922b180f-3eb6-4ad3-842f-eec634e0a610
-rwxrwxrwx   1 andrewlenards staff       2861 2014-10-28 15:17 /chkpt/checkpoint-1414534665000
-rwxrwxrwx   1 andrewlenards staff       2866 2014-10-28 15:17 /chkpt/checkpoint-1414534665000.bk
-rwxrwxrwx   1 andrewlenards staff       2861 2014-10-28 15:17 /chkpt/checkpoint-1414534670000
-rwxrwxrwx   1 andrewlenards staff       2868 2014-10-28 15:17 /chkpt/checkpoint-1414534670000.bk
-rwxrwxrwx   1 andrewlenards staff       2861 2014-10-28 15:17 /chkpt/checkpoint-1414534675000
-rwxrwxrwx   1 andrewlenards staff       2866 2014-10-28 15:17 /chkpt/checkpoint-1414534675000.bk
-rwxrwxrwx   1 andrewlenards staff       2861 2014-10-28 15:18 /chkpt/checkpoint-1414534680000
-rwxrwxrwx   1 andrewlenards staff       2869 2014-10-28 15:18 /chkpt/checkpoint-1414534680000.bk
-rwxrwxrwx   1 andrewlenards staff       2861 2014-10-28 15:18 /chkpt/checkpoint-1414534685000
-rwxrwxrwx   1 andrewlenards staff       2868 2014-10-28 15:18 /chkpt/checkpoint-1414534685000.bk
drwxrwxrwx   - andrewlenards staff          0 2014-10-28 15:15 /chkpt/d87d4db8-db0d-4416-b0c5-f71df379a185
```

$ ./bin/dse hadoop fs -ls cfs://127.0.0.1/measures/
objc[69468]: Class JavaLaunchHelper is implemented in both /Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home/bin/java and /Library/Java/JavaVirtualMachines/jdk1.7.0_67.jdk/Contents/Home/jre/lib/libinstrument.dylib. One of the two will be used. Which one is undefined.
14/10/28 16:46:12 INFO snitch.Workload: Setting my workload to Cassandra

### C* Schema

``schema.cql``;

```
cqlsh> SOURCE '~/tmp/explore-spark/streaming/schema.cql';
cqlsh> USE ts_data;
cqlsh:ts_data> SELECT * FROM measurements;
```

## Building

```
sbt/sbt package
...
[info] Done packaging.
[success] Total time: 5 s, completed Oct 28, 2014 3:14:09 PM
```

## Running

```
$ ./bin/dse spark-submit --class Stream ~/tmp/explore-spark/streaming/target/scala-2.10/simple-spark-streaming-project_2.10-0.1.jar cfs://127.0.0.1/chkpt/
...
14/10/28 15:15:24 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
```

You can open up http://localhost:4041/ to see the SparkUI for this application.


```
-------------------------------------------
Time: 1414534595000 ms
-------------------------------------------

-------------------------------------------
Time: 1414534600000 ms
-------------------------------------------
Measurement(1,1412360983000,86.875,F,32.221745,-110.92661)
Measurement(1,1412361583000,87.051,F,32.221745,-110.92661)
Measurement(1,1412362183000,86.995,F,32.221745,-110.92661)
Measurement(1,1412362783000,87.175,F,32.221745,-110.92661)
Measurement(1,1412363383000,86.899,F,32.221745,-110.92661)

-------------------------------------------
Time: 1414534605000 ms
-------------------------------------------
Measurement(1,1412360983000,86.875,F,32.221745,-110.92661)
Measurement(1,1412361583000,87.051,F,32.221745,-110.92661)
Measurement(1,1412362183000,86.995,F,32.221745,-110.92661)
Measurement(1,1412362783000,87.175,F,32.221745,-110.92661)
Measurement(1,1412363383000,86.899,F,32.221745,-110.92661)

-------------------------------------------
Time: 1414534610000 ms
-------------------------------------------

```