
import org.apache.spark._
import org.apache.spark.SparkContext._

import com.datastax.spark.connector._

import org.joda.time.DateTime


object TickerAgg {
    // counterpart of ticker.tick
    case class Tick(ticker_symbol: String, date_hour: String,
                    trade_occurred: DateTime, trade_agent: String,
                    alpha: Float, gamma: Float, price: Float, quantity: Int)

    // counterpart of ticker.tick_rollup_by_datehour
    case class TickRollupByDateHour(date_hour: String,
                    ticker_symbol: String, trade_agent: String,
                    total_cost: Float, total_trade_amt: Int)

    def main(args: Array[String]) {
        val sparkMasterHost = "127.0.0.1"
        val cassandraHost = "127.0.0.1"

        // Let Spark know our Cassandra node's address:
        val conf = new SparkConf(true)
                    .set("spark.cassandra.connection.host", cassandraHost)
                    .set("spark.cleaner.ttl", "3600")
                    .setMaster("local[6]") // local, standalone w/ 6 cores
                    .setAppName(getClass.getSimpleName)

        val sc = new SparkContext(conf)

        // create a base RDD from Cassandra w/ the Tick case class
        val tickRDD = sc.cassandraTable("ticker", "tick").as(Tick)

        // indicate that this should be materialized in cache
        tickRDD.cache()

        // reshape data as pairs for PairRDD, combine pairs
        val rollup = tickRDD.map { r =>
                (
                    (r.date_hour, r.ticker_symbol, r.trade_agent),
                    (r.price, r.quantity)
                )
            }.combineByKey(
                    (metrics) => (metrics._1 * metrics._2, metrics._2),
                    (t:(Float,Int), r:(Float,Int)) => (t._1 + r._1, t._2 + r._2),
                    (lhs:(Float,Int), rhs:(Float,Int)) =>
                                                (lhs._1 + rhs._1, lhs._2 + rhs._2)
            )

        // translate elements of rollup RDD into CQL-friendly form
        val tickRollup = rollup.map { r =>
                TickRollupByDateHour(r._1._1, r._1._2, r._1._3, r._2._1, r._2._2)
            }

        println("Saving to Cassandra...")

        // persist to Cassandra
        tickRollup.saveToCassandra("ticker", "tick_rollup_by_datehour")

        println("Saving complete.")

        sc.stop()
    }
}