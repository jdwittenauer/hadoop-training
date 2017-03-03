/*
 * 
 *  
 */

package solutions

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.SparkContext._
import org.apache.spark.sql.functions.avg
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{
  StreamingListener,
  StreamingListenerReceiverError,
  StreamingListenerReceiverStopped
}
import org.apache.spark.sql.SQLContext

object SensorStreamWindow extends Serializable {

  // schema for sensor data   
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)

  case class PumpInfo(resid: String, pumpType: String, purchaseDate: String, serviceDate: String, vendor: String, longitude: Float, lattitude: Float)

  case class Maint(resid: String, eventDate: String, technician: String, description: String)
  // function to parse line of sensor data into Sensor class
  def parseSensor(str: String): Sensor = {
    val p = str.split(",")
    Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
  }
  def parsePumpInfo(str: String): PumpInfo = {
    val p = str.split(",")
    PumpInfo(p(0), p(1), p(2), p(3), p(4), p(5).toFloat, p(6).toFloat)
  }
  def parseMaint(str: String): Maint = {
    val p = str.split(",")
    Maint(p(0), p(1), p(2), p(3))
  }
  val timeout = 10 // Terminate after N seconds
  val batchSeconds = 2 // Size of batch intervals

  def main(args: Array[String]): Unit = {
    // set up HBase Table configuration
    val sparkConf = new SparkConf().setAppName("HBaseTest").set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)

    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sc, Seconds(batchSeconds))

    // parse the lines of data into sensor objects
    val textDStream = ssc.textFileStream("/user/user01/stream")
    //ssc.checkpoint("/user/user01/check")
    val sensorDStream = textDStream.map(parseSensor)

    sensorDStream.window(Seconds(6), Seconds(2))
      .foreachRDD { rdd =>
        // There exists at least one element in RDD
        if (!rdd.partitions.isEmpty) {
           val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
          import sqlContext.implicits._
          import org.apache.spark.sql.functions._

          val sensorDF = rdd.toDF()
          // Display the top 20 rows of DataFrame
          println("sensor data")
          sensorDF.show()
          sensorDF.registerTempTable("sensor")
          val res = sqlContext.sql("SELECT resid, date, count(resid) as total FROM sensor GROUP BY resid, date")
          println("sensor count ")
          res.show
          val res2 = sqlContext.sql("SELECT resid, date, MAX(psi) as maxpsi, min(psi) as minpsi, avg(psi) as avgpsi FROM sensor GROUP BY resid,date")
          println("sensor max, min, averages ")
          res2.show
      

        }
      }

    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }


}