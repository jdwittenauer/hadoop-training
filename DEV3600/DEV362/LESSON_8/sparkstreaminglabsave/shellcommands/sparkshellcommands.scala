//  SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
// this is used to implicitly convert an RDD to a DataFrame.
import sqlContext.implicits._
// Import Spark SQL data types and Row.
import org.apache.spark.sql._
import org.apache.spark.util.StatCounter

case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)

// function to parse line of sensor data into Sensor class
def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
 }

 val textRDD = sc.textFile("/user/user01/sparkstreaminglab/data/sensordata.csv")
 textRDD.take(1)
 val sensorRDD= textRDD.map(parseSensor)
 sensorRDD.take(1)

 val alertRDD = sensorRDD.filter(sensor => sensor.psi < 5.0)
 alertRDD.take(1).foreach(println)

  // transform into an RDD of (key, values) to get daily stats for psi
 val keyValueRDD=sensorRDD.map(sensor => ((sensor.resid,sensor.date),sensor.psi))
 // print out some data
 keyValueRDD.take(3).foreach(kv => println(kv))
// use StatCounter utility to get statistics for sensor psi
 val keyStatsRDD = keyValueRDD.groupByKey().mapValues(psi => StatCounter(psi))

  // print out some data
 keyStatsRDD.take(5).foreach(println)
    
 val sensorDF = sensorRDD.toDF()
 // Return the schema of this DataFrame
 sensorDF.printSchema()
 // Display the top 20 rows of DataFrame
 sensorDF.show()
 // group by the sensorid, date get average psi
 sensorDF.groupBy("resid", "date").agg(avg(sensorDF("psi"))).take(5).foreach(println)

 sensorDF.registerTempTable("sensor")

 val sensorStatDF = sqlContext.sql("SELECT resid, date,MAX(hz) as maxhz, min(hz) as minhz, avg(hz) as avghz, MAX(disp) as maxdisp, min(disp) as mindisp, avg(disp) as avgdisp, MAX(flo) as maxflo, min(flo) as minflo, avg(flo) as avgflo,MAX(sedPPM) as maxsedPPM, min(sedPPM) as minsedPPM, avg(sedPPM) as avgsedPPM, MAX(psi) as maxpsi, min(psi) as minpsi, avg(psi) as avgpsi,MAX(chlPPM) as maxchlPPM, min(chlPPM) as minchlPPM, avg(chlPPM) as avgchlPPM FROM sensor GROUP BY resid,date")
   
 sensorStatDF.printSchema()
 sensorStatDF.take(5).foreach(println)  






