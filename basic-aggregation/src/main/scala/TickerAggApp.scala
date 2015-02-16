
import org.apache.spark._
import org.apache.spark.SparkContext._

import com.datastax.spark.connector._

import org.joda.time.DateTime


object TickerAgg {
    case class Tick(ticker_symbol: String, date_hour: String,
                    trade_occurred: DateTime, trade_agent: String, alpha: Float,
                    gamma: Float, price: Float, quantity: Int)

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

    val tickRDD = sc.cassandraTable[Tick]("ticker", "tick").cache

    //...

    sc.stop()

}