// Get the current spark version
// $ ls /opt/mapr/spark

// Launch the interactive spark shell
// $ /opt/mapr/spark/spark-1.6.1/bin/spark-shell --master local[2]

// Import libraries and declare column variables
import java.lang.Math
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
val auctionRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(_.split(","))

// See the first element of the RDD
auctionRDD.first()

// First five element of the RDD
auctionRDD.take(5)

// What is the total number of bids?
auctionRDD.count()

// What is the total number of distinct items that were auctioned?
auctionRDD.map(_(auctionid)).distinct().count()

// What is the total number of item types that were auctioned?
auctionRDD.map(_(itemtype)).distinct().count()

// What is the total number of bids per item type?
auctionRDD.map(x => (x(itemtype), 1)).reduceByKey((x, y) => x + y).collect()

// What is the total number of bids per auction?
auctionRDD.map(x => (x(auctionid), 1)).reduceByKey((x, y) => x + y)

// Across all auctioned items, what is the max number of bids?
val bids_auctionRDD = auctionRDD.map(x => (x(auctionid), 1)).reduceByKey((x, y) => x + y)
bids_auctionRDD.map(x => x._2).reduce((x, y) => Math.max(x, y))

// Across all auctioned items, what is the minimum of bids?
val bids_auctionRDD = auctionRDD.map(x => (x(auctionid), 1)).reduceByKey((x, y) => x + y)
bids_auctionRDD.map(x => x._2).reduce((x, y)=>Math.min(x, y))

// What is the average bid?
val totbids = auctionRDD.count()
val totitems = auctionRDD.map(_(auctionid)).distinct().count()
totbids / totitems

// Exit the spark shell
exit
