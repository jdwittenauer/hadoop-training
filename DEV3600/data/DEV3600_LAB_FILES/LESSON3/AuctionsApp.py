# Simple App to inspect Auction data 
#The following import statements imort SparkContext, SparkConf

from pyspark import SparkContext,SparkConf

conf = SparkConf().setAppName("AuctionsApp")
sc = SparkContext(conf=conf)
# Add location of input file */
aucFile ="/user/user01/data/auctiondata.csv"
#map input variables                
auctionid = 0
bid = 1
bidtime = 2
bidder = 3
bidderrate = 4
openbid = 5
price = 6
itemtype = 7
daystolive = 8

#build the inputRDD
auctionRDD = sc.textFile(aucFile).map(lambda line:line.split(",")).cache()
#total number of bids across all auctions
totalbids=auctionRDD.count()
#total number of items (auctions)
totalitems=auctionRDD.map(lambda x:x[auctionid]).distinct().count()
#RDD containing ordered pairs of auctionid,number
bids_auctionRDD=auctionRDD.map(lambda x: (x[auctionid],1)).reduceByKey(lambda x,y:x+y)	
#max, min and avg number of bids
maxbids=bids_auctionRDD.map(lambda x:x[bid]).reduce(max)
minbids=bids_auctionRDD.map(lambda x:x[bid]).reduce(min)
avgbids=totalbids/totalitems
print "total bids across all auctions: %d " %(totalbids)
print "total number of distinct items: %d " %(totalitems)
print "Max bids across all auctions: %d " %(maxbids)
print "Min bids across all auctions: %d " %(minbids)
print "Avg bids across all auctions: %d " %(avgbids)
print "DONE"                                                               
