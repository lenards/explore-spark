CREATE KEYSPACE ticker WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE ticker;

CREATE TABLE tick (
  ticker_symbol text,
  date_hour text,
  trade_occurred timestamp,
  trade_agent text,
  alpha float,
  gamma float,
  price float,
  quantity int,
  PRIMARY KEY ((ticker_symbol, date_hour), trade_occurred, trade_agent)
) WITH CLUSTERING ORDER BY (trade_occurred DESC, trade_agent ASC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE tick_most_traded_by_date (
  date timestamp,
  total_trade_amt float,
  ticker_symbol text,
  total_cost float,
  PRIMARY KEY ((date), total_trade_amt, ticker_symbol)
) WITH CLUSTERING ORDER BY (total_trade_amt DESC, ticker_symbol ASC) AND
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

CREATE TABLE tick_rollup_by_datehour (
  date_hour text,
  ticker_symbol text,
  trade_agent text,
  total_cost float,
  total_trade_amt float,
  PRIMARY KEY ((date_hour, ticker_symbol), trade_agent)
) WITH
  bloom_filter_fp_chance=0.010000 AND
  caching='KEYS_ONLY' AND
  comment='' AND
  dclocal_read_repair_chance=0.100000 AND
  gc_grace_seconds=864000 AND
  index_interval=128 AND
  read_repair_chance=0.000000 AND
  replicate_on_write='true' AND
  populate_io_cache_on_flush='false' AND
  default_time_to_live=0 AND
  speculative_retry='99.0PERCENTILE' AND
  memtable_flush_period_in_ms=0 AND
  compaction={'class': 'SizeTieredCompactionStrategy'} AND
  compression={'sstable_compression': 'LZ4Compressor'};

