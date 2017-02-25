/* Simple app to inspect Auction data */
/* The following import statements are importing SparkContext, all subclasses and SparkConf*/
package exercises
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
//Will use max, min - import java.Lang.Math
import java.lang.Math

object AuctionsApp {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("AuctionsApp")
    val sc = new SparkContext(conf)
    /* MAKE SURE THAT PATH TO DATA FILE IS CORRECT */
    val usrhome = System.getenv("HOME")
    val aucFile = usrhome.concat("/data/auctiondata.csv")
    //set up indexes                

    //build the inputRDD and cache
    //val auctionRDD=

    //total number of bids across all auctions

    //total number of items (auctions)

    //RDD containing ordered pairs of auctionid,number

    //max, min and avg number of bids
    //val maxbids=
    //val minbids=
    //val avgbids=

    //print to console
    //println("total bids across all auctions: %s ".format(totalbids))
    //println("total number of distinct auctions: )
    //println("Max bids across all auctions: )
    //println("Min bids across all auctions: )
    //println("Avg bids across all auctions: )
  }
}
