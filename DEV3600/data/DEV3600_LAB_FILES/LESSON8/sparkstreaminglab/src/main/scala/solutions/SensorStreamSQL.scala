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

object SensorStreamSQL extends Serializable {

  // schema for sensor data   
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double) extends Serializable

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

    val sparkConf = new SparkConf().setAppName("SensorStream")
      .set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)

    val sqlContext = SQLContextSingleton.getInstance(sc)
    import sqlContext.implicits._
    import org.apache.spark.sql.functions._

    val pumpRDD = sc.textFile("/user/user01/data/sensorvendor.csv").map(parsePumpInfo)
    println("pumpRDD take 1")
    pumpRDD.take(1).foreach(println)
    val maintRDD = sc.textFile("/user/user01/data/sensormaint.csv").map(parseMaint)
    println("maintRDD take 1")
    maintRDD.take(1)
    val maintDF = maintRDD.toDF()
    val pumpDF = pumpRDD.toDF()
    maintDF.registerTempTable("maint")
    pumpDF.registerTempTable("pump")

    // create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sc, Seconds(batchSeconds))

    // parse the lines of data into sensor objects
    val textDStream = ssc.textFileStream("/user/user01/stream");
    textDStream.print()
    val sensorDStream = textDStream.map(parseSensor)

    sensorDStream.foreachRDD { rdd =>

      // There exists at least one element in RDD
      if (!rdd.partitions.isEmpty) {
        val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
        import sqlContext.implicits._
        val sensorDF = rdd.toDF()
        // Display the top 20 rows of DataFrame
        println("sensor data")
        sensorDF.show()
        sensorDF.registerTempTable("sensor")
        val res = sqlContext.sql("SELECT resid, date,MAX(hz) as maxhz, min(hz) as minhz, avg(hz) as avghz, MAX(disp) as maxdisp, min(disp) as mindisp, avg(disp) as avgdisp, MAX(flo) as maxflo, min(flo) as minflo, avg(flo) as avgflo,MAX(sedPPM) as maxsedPPM, min(sedPPM) as minsedPPM, avg(sedPPM) as avgsedPPM, MAX(psi) as maxpsi, min(psi) as minpsi, avg(psi) as avgpsi,MAX(chlPPM) as maxchlPPM, min(chlPPM) as minchlPPM, avg(chlPPM) as avgchlPPM FROM sensor GROUP BY resid,date")
        println("sensor max, min, averages ")
        res.show
        // filter sensor data for low psi
        val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)
        println("low pressure alert ")
        alertRDD.take(1).foreach(println)
        val alertDF = alertRDD.toDF()
        alertDF.registerTempTable("alert")
        val alertpumpmaintViewDF = sqlContext.sql("select s.resid, s.date, s.psi, p.pumpType, p.purchaseDate, p.serviceDate, p.vendor, m.eventDate, m.technician, m.description from alert s join pump p on s.resid = p.resid join maint m on p.resid=m.resid")
        println("alert pump maintenance data")
        alertpumpmaintViewDF.show
        alertRDD.saveAsTextFile("/user/user01/alertout")

      }
    }

    // Start the computation
    println("start streaming")
    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }
  object SQLContextSingleton {
    @transient private var instance: SQLContext = _

    def getInstance(sparkContext: SparkContext): SQLContext = {
      if (instance == null) {
        instance = new SQLContext(sparkContext)
      }
      instance
    }
  }

}