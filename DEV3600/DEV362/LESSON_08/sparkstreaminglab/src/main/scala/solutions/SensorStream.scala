package solutions

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._


object SensorStream extends Serializable {
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double) extends Serializable

  // Function to parse line of sensor data into Sensor class
  def parseSensor(str: String): Sensor = {
    val p = str.split(",")
    Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("SensorStream").set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)

    // Create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sc, Seconds(2))

    // Parse the lines of data into sensor objects
    val textDStream = ssc.textFileStream("/user/user01/stream");
    val sensorDStream = textDStream.map(parseSensor)

    // Apply processing to each RDD in the input stream
    sensorDStream.foreachRDD { rdd =>
      if (!rdd.partitions.isEmpty) {
        val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
        println("Low pressure alert")
        alertRDD.take(2).foreach(println)
        alertRDD.saveAsTextFile("/user/user01/alertout")
      }
    }

    // Start the computation
    println("Starting streaming process")
    ssc.start()
    ssc.awaitTermination()
  }
}