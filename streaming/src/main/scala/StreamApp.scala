import java.util.Date
import javax.xml.bind.DatatypeConverter

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._



// pulled format from OpenSensorData - but it's not super original:
//
//      http://opensensordata.net/
//
// sensor stream, date/time, value, unit, longitude, latitude
// 1, 2013-03-03T18:29:43, 86.875, F, 2.363471, 48.917536
case class Measurement(sensor_net:Long, measured_at:Long, value:Float,
                       unit:String, long:Float, lat:Float)



object Stream {
    // Turns out DatatypeConverter is the best ISO 8601 format option in JDK:
    //
    // http://stackoverflow.com/questions/3914404/how-to-get-current-moment-in-iso-8601-format
    def toDate(raw: String): Long = {
        return DatatypeConverter.parseDateTime(raw).getTimeInMillis()
    }

    // rips apart line and returns a measurement
    def rend(line: String): Measurement = {
        val pieces = line.split(",")
        return Measurement(pieces(0).toLong, toDate(pieces(1)),
                           pieces(2).toFloat, pieces(3).trim,
                           pieces(4).toFloat, pieces(5).toFloat)
    }

    def rend2(arr: Array[String]): Measurement = {
        return Measurement(arr(0).toLong, toDate(arr(1)),
                           arr(2).toFloat, arr(3).trim,
                           arr(4).toFloat, arr(5).toFloat)
    }

    def main(args: Array[String]) {
        if (args.isEmpty) {
            throw new IllegalArgumentException("Source data path required.")
        }
        val checkpointDir = "cfs://127.0.0.1/chkpt/"

        val conf = new SparkConf(true)
            .set("spark.cassandra.connection.host", "127.0.0.1")
            .set("spark.cleaner.ttl", "3600")
            .setMaster("local[2]")
            .setAppName("Simple Spark Streaming App")

        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))
        ssc.checkpoint(checkpointDir)

        // setup our DStream - use a file directory as source
        val lines = ssc.textFileStream("/measures")

        // transform lines into Measurements
        val measurements = lines.map(rend)

        measurements.print()

        measurements.saveAsTextFiles("/processed", "ndy")

        ssc.start()
        ssc.awaitTermination()
    }
}