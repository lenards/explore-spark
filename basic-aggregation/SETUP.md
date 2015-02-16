# Setup Local Environment

## Install DataStax Enterprise 

You will need to have an installed a single node instance of DataStax Enterpise or a cluster. 

You will need it to be Spark *enabled* and run it in standalone mode with Spark: 

```
$ dse cassandra -k
```

## Clone the Repo

Locally clone this repository. 

## Define the Schema

Assuming you are in the `basic-aggregation` directory of `explore-spark`:

```
$ ls 
README.md        build.sbt        sample_data2.csv schema.cql
SETUP.md         sample_data.csv  sample_data3.csv src
```

Assuming that you do not require username/password for accessing ``cqlsh``:

```
cqlsh < schema.cql
```

or 

```
$ cqlsh
Connected to andrew_lenards at 127.0.0.1:9160.
[cqlsh 4.1.1 | Cassandra 2.0.11.83 | DSE 4.6.0 | CQL spec 3.1.1 | Thrift protocol 19.39.0]
Use HELP for help.
cqlsh> SOURCE 'schema.cql'; 
```

## Insert some Sample Data

You will need to insert some sample data, provided as ``sample_data.csv``.

You can use an absolute path (replace ``/path/to/repo``, or perform this from the repo directory)

```
COPY tick (ticker_symbol, date_hour, trade_occurred, trade_agent, alpha, gamma, price, quantity) FROM '/path/to/repo/explore-spark/simple-aggregation/sample_data.csv';
```

Note, that we're using "time granularity" of one second.

```
COPY tick (ticker_symbol, date_hour, trade_occurred, trade_agent, alpha, gamma, price, quantity) FROM '/Users/andrewlenards/tmp/explore-spark/simple-aggregation/sample_data.csv';
82 rows imported in 0.105 seconds.
cqlsh:ticker> select count(*) from tick;                                                                                             
 count
-------
    79

(1 rows)
```

So we know that the granularity of our ``trade_occurred`` is causing some entries to be lost. This was cause me to consider using a ``TIMEUUID`` for this field in the future - which is going to be sorted and avoid clobbering trades. 

This is left as an exercise for the observer/reader. 

We are going to move forward without that change for now. 

