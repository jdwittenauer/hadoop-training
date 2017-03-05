package solutions
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql._

// Define the schema using a case class
case class Auction(auctionid: String, bid: Float, bidtime: Float, bidder: String, bidderrate: Integer, openbid: Float, price: Float, item: String, daystolive: Integer)

object AuctionsDFApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AuctionsDFApp")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val usrhome = System.getenv("HOME")
    val aucFile = usrhome.concat("/data/auctiondata.csv")
    import sqlContext.implicits._
    import sqlContext._

    // Load the data
    val auctionText = sc.textFile(aucFile)

    // Create an RDD of Auction objects 
    val auctionRDD = auctionText.map(_.split(",")).map(p => Auction(p(0), p(1).toFloat, p(2).toFloat, p(3), p(4).toInt, p(5).toFloat, p(6).toFloat, p(7), p(8).toInt))

    // Convert to a DataFrame
    val auctionDF = auctionRDD.toDF()

    // Count the number of auctions
    val count = auctionDF.select("auctionid").distinct.count()

    // Print output to the console
    println(count)
    auction.printSchema()
    auction.take(3)
    auction.write.save("json")
  }
}
