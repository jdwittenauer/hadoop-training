 
spark-shell --master local[2]

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import StreamingContext._
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

val ssc = new StreamingContext(sc, Seconds(2))
 
val lines = ssc.textFileStream("/user/user01/stream") 
val words = lines.flatMap(_.split(" "))
val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
wordCounts.print()
ssc.start()
ssc.awaitTermination()



