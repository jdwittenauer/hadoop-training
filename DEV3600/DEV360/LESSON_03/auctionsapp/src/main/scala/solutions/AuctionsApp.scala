package solutions
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.lang.Math

object AuctionsApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AuctionsApp")
    val sc = new SparkContext(conf)
    val usrhome = System.getenv("HOME")
    val aucFile = usrhome.concat("/data/auctiondata.csv")

    // Map input variables                
    val auctionid = 0
    val bid = 1
    val bidtime = 2
    val bidder = 3
    val bidderrate = 4
    val openbid = 5
    val price = 6
    val itemtype = 7
    val daystolive = 8

    // Load the data
    val auctionRDD = sc.textFile(aucFile).map(line => line.split(",")).cache()

    // Total number of bids across all auctions
    val totalbids = auctionRDD.count()

    // Total number of items
    val totalitems = auctionRDD.map(line => line(auctionid)).distinct().count()

    // Max, min and avg number of bids
    val bids_auctionRDD = auctionRDD.map(x => (x(auctionid), 1)).reduceByKey((x, y) => x + y)
    val maxbids = bids_auctionRDD.map(x => x._2).reduce((x, y) => Math.max(x, y))
    val minbids = bids_auctionRDD.map(x => x._2).reduce((x, y) => Math.min(x, y))
    val avgbids = totalbids / totalitems

    // Print output to the console
    println("total bids across all auctions: %s ".format(totalbids))
    println("total number of distinct items: %s ".format(totalitems))
    println("Max bids across all auctions: %s ".format(maxbids))
    println("Min bids across all auctions: %s ".format(minbids))
    println("Avg bids across all auctions: %s ".format(avgbids))
  }
}
