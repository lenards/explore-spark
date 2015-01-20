Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.1.0
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_67)
Type in expressions to have them evaluated.
Type :help for more information.
Creating SparkContext...
Created spark context..
Spark context available as sc.
Type in expressions to have them evaluated.
Type :help for more information.

scala> :paste
// Entering paste mode (ctrl-D to finish)

import org.apache.spark._
import org.apache.spark.SparkContext._
import java.util.UUID
import java.util.Date
import java.util.Calendar

//import javax.xml.bind.DatatypeConverter
import com.datastax.driver.core.utils.UUIDs


import com.datastax.spark.connector._


// Exiting paste mode, now interpreting.

import org.apache.spark._
import org.apache.spark.SparkContext._
import java.util.UUID
import java.util.Date
import java.util.Calendar
import com.datastax.driver.core.utils.UUIDs
import com.datastax.spark.connector._

scala> :paste
// Entering paste mode (ctrl-D to finish)

    case class ReadingsByMonth(arrayId: UUID, arrayName: String, year: Int,
                               month: Int, sensor: String, measuredAt: Date,
                               value: Float, unit: String, location: String)

    case class ReadingsByMonthHour(arrayId: UUID, arrayName: String,
                                   monthHour: Date, sensor: String,
                                   measuredAt: Date, value: Float,
                                   unit: String, location: String)

    def convertTo(in: ReadingsByMonth): ReadingsByMonthHour = {
        var cal = Calendar.getInstance()
        cal.setTime(in.measuredAt)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MINUTE, 0)
        //
        return ReadingsByMonthHour(in.arrayId, in.arrayName, cal.getTime,
                                   in.sensor, in.measuredAt, in.value, in.unit,
                                   in.location)
    }

// Exiting paste mode, now interpreting.

defined class ReadingsByMonth
defined class ReadingsByMonthHour
convertTo: (in: ReadingsByMonth)ReadingsByMonthHour

scala> val tableData = sc.cassandraTable[ReadingsByMonth]("ts_data", "readings_by_month")
tableData: com.datastax.spark.connector.rdd.CassandraRDD[ReadingsByMonth] = CassandraRDD[0] at RDD at CassandraRDD.scala:47


scala> val reading = ReadingsByMonth(UUID.fromString("867981c2-17f9-44a9-9cc3-6d70d2fe052f"), "R5-650", 2014, 11, "Freezer", new Date(), -1.7f, "C", "32, -110")
reading: ReadingsByMonth = ReadingsByMonth(867981c2-17f9-44a9-9cc3-6d70d2fe052f,R5-650,2014,11,Freezer,Tue Nov 04 10:55:21 MST 2014,-1.7,C,32, -110)

scala> reading.getClass
res2: Class[_ <: ReadingsByMonth] = class $iwC$$iwC$ReadingsByMonth

scala> val one = tableData.take(1)
one: Array[ReadingsByMonth] = Array(ReadingsByMonth(867981c2-17f9-44a9-9cc3-6d70d2fe052f,R65-011,2014,10,greenhouse,Fri Oct 31 05:40:09 MST 2014,89.3,F,32.221667, -110.926389))

scala> one.getClass
res3: Class[_ <: Array[ReadingsByMonth]] = class [L$iwC$$iwC$ReadingsByMonth;

scala> one(0).getClass
res4: Class[_ <: ReadingsByMonth] = class $iwC$$iwC$ReadingsByMonth

scala> val n1 = convertTo(reading)
<console>:84: error: type mismatch;
 found   : ReadingsByMonth
 required: ReadingsByMonth
       val n1 = convertTo(reading)
                          ^

scala> val n2 = covertTo(one(0))
<console>:82: error: not found: value covertTo
       val n2 = covertTo(one(0))
                ^

scala> val n2 = convertTo(one(0))
<console>:86: error: type mismatch;
 found   : ReadingsByMonth
 required: ReadingsByMonth
       val n2 = convertTo(one(0))
                             ^

scala> one(0).getClass == reading.getClass
res5: Boolean = true

scala> convertTo(readings).tpe
<console>:83: error: not found: value readings
              convertTo(readings).tpe
                        ^

scala> convertTo(readings).type
<console>:1: error: identifier expected but 'type' found.
       convertTo(readings).type
                           ^

scala> import scala.reflect.runtime.{universe => ru}
import scala.reflect.runtime.{universe=>ru}

scala> convertTo(readings).tpe
<console>:84: error: not found: value readings
              convertTo(readings).tpe
                        ^

scala> convertTo _
res8: ReadingsByMonth => ReadingsByMonthHour = <function1>

scala> one(0).getClass.isAssignableFrom(reading.getClass)
res9: Boolean = true

scala> reading.getClass.isAssignableFrom(one(0).getClass)
res10: Boolean = true

