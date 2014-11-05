/* BulkMove.scala */

import org.apache.spark._
import org.apache.spark.SparkContext._
import java.util.UUID
import java.util.Date
import java.util.Calendar

import com.datastax.spark.connector._

object BulkMove {
    case class ReadingsByMonth(arrayId: UUID, arrayName: String, year: Int,
                               month: Int, sensor: String, measuredAt: Date,
                               value: Float, unit: String, location: String)

    case class ReadingsByMonthHour(arrayId: UUID, arrayName: String,
                                   monthHour: Date, sensor: String,
                                   measuredAt: Date, value: Float,
                                   unit: String, location: String)

    def convertTo(in: ReadingsByMonth): ReadingsByMonthHour = {
        // Change date to only have hours time component
        var cal = Calendar.getInstance()
        cal.setTime(in.measuredAt)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MINUTE, 0)

        return ReadingsByMonthHour(in.arrayId, in.arrayName, cal.getTime,
                                   in.sensor, in.measuredAt, in.value, in.unit,
                                   in.location)
    }

    def main(args: Array[String]) {
        val conf = new SparkConf(true)
                    .set("spark.cassandra.connection.host", "127.0.0.1")
                    .setAppName("Schema Evolution Spark App")
        val sc = new SparkContext(conf)

        val tableData = sc.cassandraTable[ReadingsByMonth]("ts_data", "readings_by_month")
        val transformed = tableData.map(convertTo)
        transformed.saveToCassandra("ts_data", "reading_by_month_hour")

        println("Done...")

        sc.stop()
    }
}