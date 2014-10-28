to get the C* RDD methods, need to import the connector:

```
import com.datastax.spark.connector._
```

configuration values

create Spark context

create Streaming context

get a dstream from socket

map your parsing function over input

map/translate anything else

foreachRDD ( rdd =>
    do stuff
)

print if you need to

saveToCassandra

call start
call await Terminate