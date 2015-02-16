# Setup Local Environment

## Install DataStax Enterprise 

## Clone the Repo

## Define the Schema

Assuming you are in the `basic-aggregation` directory of `explore-spark`:

```
$ ls 
README.md        build.sbt        sample_data2.csv schema.cql
SETUP.md         sample_data.csv  sample_data3.csv src
```

```
cqlsh < schema.cql
```

or 

```

cqlsh
```

## Insert some Sample Data

```
COPY tick (ticker_symbol, date_hour, trade_occurred, trade_agent, alpha, gamma, price, quantity) FROM '/Users/andrewlenards/tmp/explore-spark/simple-aggregation/sample_data.csv';
```


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

I'm going to move forward without that change for now. 

