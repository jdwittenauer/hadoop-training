from pyspark import SparkContext,SparkConf

# Define variables
conf = SparkConf().setAppName("auctions_app")
sc = SparkContext(conf=conf)

# Map input variables                
auctionid = 0
bid = 1
bidtime = 2
bidder = 3
bidderrate = 4
openbid = 5
price = 6
itemtype = 7
daystolive = 8

# Load the data
auctionRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(lambda line: line.split(",")).cache()

# Total number of bids across all auctions
totalbids = auctionRDD.count()

# Total number of items
totalitems = auctionRDD.map(lambda x: x[auctionid]).distinct().count()

# Max, min and avg number of bids
bids_auctionRDD = auctionRDD.map(lambda x: (x[auctionid], 1)).reduceByKey(lambda x, y: x + y)
maxbids = bids_auctionRDD.map(lambda x: x[bid]).reduce(max)
minbids = bids_auctionRDD.map(lambda x: x[bid]).reduce(min)
avgbids = totalbids / totalitems

# Print output to the console
print "Total bids across all auctions: %d " %(totalbids)
print "Total number of distinct items: %d " %(totalitems)
print "Max bids across all auctions: %d " %(maxbids)
print "Min bids across all auctions: %d " %(minbids)
print "Avg bids across all auctions: %d " %(avgbids)
print "DONE"                                                         
