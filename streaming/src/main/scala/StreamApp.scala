import java.util.Date
import javax.xml.bind.DatatypeConverter

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._

import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

// pulled format from OpenSensorData - but it's not super original:
//
//      http://opensensordata.net/
//
// sensor stream, date/time, value, unit, longitude, latitude
// 1, 2013-03-03T18:29:43, 86.875, F, 2.363471, 48.917536
case class Measurement(sensor_net:String, location:String, grouping: Long,
                       measured_at:Long, value:Float, label:String,
                       unit:String, long:Float, lat:Float)

// our case class will map to the following Cassandra table
//
//      CREATE TABLE measurements (
//          sensor_net text,
//          location text,
//          grouping bigint,
//          measured_at bigint,
//          value float,
//          local text,
//          unit text,
//          long float,
//          lat float,
//          PRIMARY KEY ((sensor_net, location, grouping), measured_at)
//      ) WITH CLUSTERING ORDER BY (measured_at DESC);

object Stream {
    // Turns out DatatypeConverter is the best ISO 8601 format option in JDK:
    //
    // http://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format
    def toDate(raw: String): Long = {
        return DatatypeConverter.parseDateTime(raw).getTimeInMillis()
    }

    def splitSensorInfo(full: String): Array[String] = {
        return full.split("/")
    }

    // rips apart line and returns a measurement
    def rend(line: String): Measurement = {
        val pieces = line.split(",")
        val sensorIdents = splitSensorInfo(pieces(0))
        val ts = toDate(pieces(1))
        val grp_bucket = ts / (3 * 60 * 1000) // 3-hour bucket to group measures
        return Measurement(sensorIdents(0), sensorIdents(1), ts, grp_bucket,
                           pieces(2).toFloat, sensorIdents(2), pieces(3).trim,
                           pieces(4).toFloat, pieces(5).toFloat)
    }

    def main(args: Array[String]) {
        if (args.isEmpty) {
            throw new IllegalArgumentException("Source data path required.")
        }
        val checkpointDir = args(0) //"cfs://127.0.0.1/chkpt/"

        val conf = new SparkConf(true)
            .set("spark.cassandra.connection.host", "127.0.0.1")
            .set("spark.cleaner.ttl", "3600")
            .setMaster("local[2]")
            .setAppName("Simple Spark Streaming App")

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))
        ssc.checkpoint(checkpointDir)

        sys.ShutdownHookThread {
            log.info("Gracefully stopping Spark Streaming Application")
            ssc.stop(true, true)
            log.info("Application stopped")
        }

        // setup our DStream - use a file directory as source
        val lines = ssc.textFileStream("/measures")

        // transform lines into Measurements
        val measurements = lines.map(rend)

        measurements.print()

        // we can save this back out to CFS as a Hadoop part file
        //measurements.saveAsTextFiles("/processed", "ndy")

        measurements.saveToCassandra("ts_data", "measurements")

        ssc.start()
        ssc.awaitTermination()
    }
}