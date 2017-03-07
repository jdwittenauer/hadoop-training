package solutions

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext


object HBaseSensorStream extends Serializable {
  final val tableName = "/user/user01/sensor"
  final val cfDataBytes = Bytes.toBytes("data")
  final val cfAlertBytes = Bytes.toBytes("alert")
  final val colHzBytes = Bytes.toBytes("hz")
  final val colDispBytes = Bytes.toBytes("disp")
  final val colFloBytes = Bytes.toBytes("flo")
  final val colSedBytes = Bytes.toBytes("sedPPM")
  final val colPsiBytes = Bytes.toBytes("psi")
  final val colChlBytes = Bytes.toBytes("chlPPM")
 
  case class Sensor(resid: String, date: String, time: String, hz: Double, disp: Double, flo: Double, sedPPM: Double, psi: Double, chlPPM: Double) extends Serializable

  object Sensor extends Serializable {
    def parseSensor(str: String): Sensor = {
      val p = str.split(",")
      Sensor(p(0), p(1), p(2), p(3).toDouble, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toDouble)
    }

    def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      val rowkey = sensor.resid + "_" + dateTime
      val put = new Put(Bytes.toBytes(rowkey))

      put.add(cfDataBytes, colHzBytes, Bytes.toBytes(sensor.hz))
      put.add(cfDataBytes, colDispBytes, Bytes.toBytes(sensor.disp))
      put.add(cfDataBytes, colFloBytes, Bytes.toBytes(sensor.flo))
      put.add(cfDataBytes, colSedBytes, Bytes.toBytes(sensor.sedPPM))
      put.add(cfDataBytes, colPsiBytes, Bytes.toBytes(sensor.psi))
      put.add(cfDataBytes, colChlBytes, Bytes.toBytes(sensor.chlPPM))

      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

    def convertToPutAlert(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val dateTime = sensor.date + " " + sensor.time
      val key = sensor.resid + "_" + dateTime
      val p = new Put(Bytes.toBytes(key))

      p.add(cfAlertBytes, colPsiBytes, Bytes.toBytes(sensor.psi))

      return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
    }
  }

  def main(args: Array[String]): Unit = {
    // Set up HBase Table configuration
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val sparkConf = new SparkConf().setAppName("HBaseSensorStream").set("spark.files.overwrite", "true")
    val sc = new SparkContext(sparkConf)

    // Create a StreamingContext, the main entry point for all streaming functionality
    val ssc = new StreamingContext(sc, Seconds(2))

    // Parse the lines of data into sensor objects
    val sensorDStream = ssc.textFileStream("/user/user01/stream").map(Sensor.parseSensor)

    sensorDStream.foreachRDD { rdd =>
      val alertRDD = rdd.filter(sensor => sensor.psi < 5.0)

      // Convert sensor data to put object and write to HBase table column family data
      rdd.map(Sensor.convertToPut).saveAsHadoopDataset(jobConfig)

      // Convert alert data to put object and write to HBase table column family alert
      alertRDD.map(Sensor.convertToPutAlert).saveAsHadoopDataset(jobConfig)
    }

    // Start the computation
    println("Starting streaming process")
    ssc.start()
    ssc.awaitTermination()
  }
}
