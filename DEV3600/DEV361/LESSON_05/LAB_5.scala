// Launch the interactive spark shell
// $ /opt/mapr/spark/spark-1.6.1/bin/spark-shell --master local[2]

// Imports
import sqlContext._
import sqlContext.implicits._

// Define case class to convert to data frame
case class Incidents(incidentnum:String, category:String, description:String, dayofweek:String, date:String, time:String, pddistrict:String, resolution:String, address:String, X:Float, Y:Float, pdid:String)

// Load the data
val sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(inc => inc.split(","))

// Infer the schema and register the DataFrame as a table
val sfpdCase=sfpdRDD.map(inc => Incidents(inc(0), inc(1), inc(2), inc(3), inc(4), inc(5), inc(6), inc(7), inc(8), inc(9).toFloat, inc(10).toFloat, inc(11)))
val sfpdDF=sfpdCase.toDF()
sfpdDF.registerTempTable("sfpd")
 
// Top 5 districts
val incByDist = sfpdDF.groupBy("pddistrict").count.sort($"count".desc)
val topByDistSQL = sqlContext.sql("SELECT pddistrict, count(incidentnum) AS inccount FROM sfpd GROUP BY pddistrict ORDER BY inccount DESC LIMIT 5")
topByDistSQL.show()

// What are the top ten resolutions?
val top10Res = sfpdDF.groupBy("resolution").count.sort($"count".desc)
val top10ResSQL = sqlContext.sql("SELECT resolution, count(incidentnum) AS inccount FROM sfpd GROUP BY resolution ORDER BY inccount DESC LIMIT 10")
top10ResSQL.show()

// Top 3 categories
val top3Cat = sfpdDF.groupBy("category").count.sort($"count".desc)
val top3CatSQL = sqlContext.sql("SELECT category, count(incidentnum) AS inccount FROM sfpd GROUP BY category ORDER BY inccount DESC LIMIT 3")
top3CatSQL.show()

// Save the top 10 resolutions to a JSON file
top10ResSQL.toJSON.saveAsTextFile("/user/user01/output")

// Define function to get year from a date string
def getyear(s:String): String = {
    val year = s.substring(s.lastIndexOf('/') + 1)
    year
}

// Register the function as a UDF
sqlContext.udf.register("getyear", getyear _)

// Count inc by year
val incyearSQL = sqlContext.sql("SELECT getyear(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY getyear(date) ORDER BY countbyyear DESC")
incyearSQL.show()

// Category, resolution and address of reported incidents in 2014 
val inc2014 = sqlContext.sql("SELECT category, address, resolution, date FROM sfpd WHERE getyear(date)='14'")
inc2014.show()

// Vandalism only in 2015 with address, resolution and category
val van2015 = sqlContext.sql("SELECT category, address, resolution, date FROM sfpd WHERE getyear(date)='15' AND category='VANDALISM'")
van2015.show()
