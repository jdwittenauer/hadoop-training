// $ spark-shell --master local[2]

import sqlContext.implicits._
import org.apache.spark.sql._
import org.apache.spark.util.StatCounter

case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double)

// Function to parse line of sensor data into Sensor class
def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
}

// SQLContext entry point for working with structured data
val sqlContext = new org.apache.spark.sql.SQLContext(sc)

// Get sensor readings with filter criteria
val textRDD = sc.textFile("/user/user01/sparkstreaminglab/data/sensordata.csv")
val sensorRDD= textRDD.map(parseSensor)
val alertRDD = sensorRDD.filter(sensor => sensor.psi < 5.0)
alertRDD.take(1).foreach(println)

// Transform into an RDD of (key, values) to get daily stats for psi
val keyValueRDD=sensorRDD.map(sensor => ((sensor.resid, sensor.date), sensor.psi))
keyValueRDD.take(3).foreach(kv => println(kv))

// Use StatCounter utility to get statistics for sensor psi
val keyStatsRDD = keyValueRDD.groupByKey().mapValues(psi => StatCounter(psi))
keyStatsRDD.take(5).foreach(println)

// Create a DataFrame
val sensorDF = sensorRDD.toDF()
sensorDF.printSchema()
sensorDF.show()
sensorDF.groupBy("resid", "date").agg(avg(sensorDF("psi"))).take(5).foreach(println)

// Query the DataFrame
sensorDF.registerTempTable("sensor")
val sensorStatDF = sqlContext.sql("SELECT resid, date,MAX(hz) as maxhz, min(hz) as minhz, avg(hz) as avghz, MAX(disp) as maxdisp, min(disp) as mindisp, 
      avg(disp) as avgdisp, MAX(flo) as maxflo, min(flo) as minflo, avg(flo) as avgflo,MAX(sedPPM) as maxsedPPM, min(sedPPM) as minsedPPM, avg(sedPPM) as avgsedPPM, 
      MAX(psi) as maxpsi, min(psi) as minpsi, avg(psi) as avgpsi,MAX(chlPPM) as maxchlPPM, min(chlPPM) as minchlPPM, avg(chlPPM) as avgchlPPM FROM sensor GROUP BY resid,date")
sensorStatDF.printSchema()
sensorStatDF.take(5).foreach(println)
