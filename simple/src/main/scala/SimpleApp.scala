/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import com.datastax.spark.connector._

object SimpleApp {
    def main(args: Array[String]) {
        // We'll need to put this in CFS for it to be available at /README.txt
        val textFile = "/README.txt"
        val conf = new SparkConf().setAppName("Simple DSE Spark App")
        val sc = new SparkContext(conf)
        val numPartitions = 2

        val textData = sc.textFile(textFile, numPartitions).cache()
        val numApaches = textData.filter(line => line.contains("Apache")).count()
        val numSparks = textData.filter(line => line.contains("Spark")).count()
        println("Lines with Apache: %s, Lines with Spark: %s".format(numApaches, numSparks))
    }
}