val auctionid = 0
val bid = 1
val bidtime = 2
val bidder = 3
val bidderrate = 4
val openbid = 5
val price = 6
val itemtype = 7
val daystolive = 8


//load the data
val auctionRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(_.split(","))

//1. see the first element of the RDD
auctionRDD.first

// 2. First five element of the RDD
auctionRDD.take(5)

//3. What is the total number of bids?
val totbids = auctionRDD.count()

//totbids: Long = 10654


//4. What is the total number of distinct items that were auctioned?
val totitems = auctionRDD.map(_(auctionid)).distinct().count()

//totitems: Long = 627


//5. What is the total number of item types that were auctioned?
val itemtypes = auctionRDD.map(_(itemtype)).distinct().count()

//totitemtypes: Long = 3


//6. What is the total number of bids per item type?
val bids_itemtype = auctionRDD.map(x=>(x(itemtype),1)).reduceByKey((x,y)=>x+y).collect()

//bids_itemtype: Array[(String, Int)] = Array((palm,5917), (cartier,1953), (xbox,2784))


//7. What is the total number of bids per auction?
val bids_auctionRDD = auctionRDD.map(x=>(x(auctionid),1)).reduceByKey((x,y)=>x+y)

//For #8, 9 - if you use Math.max, etc, then impor java.lang.Math

import java.lang.Math


// 8. Across all auctioned items, what is the max number of bids?

val maxbids = bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.max(x,y))

//maxbids: Int = 75



//9. Across all auctioned items, what is the minimum of bids?
val minbids = bids_auctionRDD.map(x=>x._2).reduce((x,y)=>Math.min(x,y))

//minbids: Int = 1


//10. What is the average bid?
val avgbids = totbids/totitems
//avgbids: Long = 16
