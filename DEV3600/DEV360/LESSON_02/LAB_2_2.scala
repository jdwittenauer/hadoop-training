// Launch the interactive spark shell
// $ /opt/mapr/spark/spark-1.6.1/bin/spark-shell --master local[2]

// Create a SQL context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// Define the Auctions case class
case class Auctions(auctionid:String, bid:Float, bidtime:Float, bidder:String, bidrate:Int, openbid:Float, price:Float, itemtype:String, dtl:Int)

// Load the data
val inputRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(_.split(","))

// Map the input RDD to the case class
val auctionRDD = inputRDD.map(a => Auctions(a(0), a(1).toFloat, a(2).toFloat, a(3), a(4).toInt, (5).toFloat, a(6).toFloat, a(7), a(8).toInt))

// Convert RDD to a DataFrame
val auctionDF = auctionRDD.toDF()

// Register as a temporary table with the same name
auctionDF.registerTempTable("auctionDF")

// Check the data in the DataFrame
auctionDF.show()

// See the schema of the DataFrame
auctionDF.printSchema()

// Total number of bids
val totalbids = auctionDF.count()

// Number of distinct auctions
auctionDF.select("auctionid").distinct.count()

// Number of distinct item types
auctionDF.select("itemtype").distinct.count()

// Count of bids per auction and item type
auctionDF.groupBy("itemtype", "auctionid").count().show()

// For each auction item and item type, get the min, max and average number of bids
auctionDF.groupBy("itemtype", "auctionid").count.agg(min("count"), avg("count"), max("count")).show()

// For each auction item and item type, get the min, max, and average bid
auctionDF.groupBy("itemtype", "auctionid").agg(min("bid"), max("bid"), avg("bid")).show()

// Return the count of all auctions with final price greater than 200
auctionDF.filter(auctionDF("price") > 200).count()

// Select just xbox auctions
val xboxes = sqlContext.sql("SELECT auctionid, itemtype, bid, price, openbid FROM auctionDF WHERE itemtype='xbox'")

// Compute basic statistics on price across all auctions on xboxes
xboxes.describe("price").show()

// Exit the spark shell
exit
