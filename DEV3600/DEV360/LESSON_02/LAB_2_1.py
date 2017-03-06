# Launch the interactive pyspark shell
# $ /opt/mapr/spark/spark-1.6.1/bin/pyspark

# Declare column variables
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
auctionRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(lambda line: line.split(","))

# See the first element of the RDD
auctionRDD.first()

# First five element of the RDD
auctionRDD.take(5)

# What is the total number of bids?
auctionRDD.count()

# What is the total number of distinct items that were auctioned?
auctionRDD.map(lambda line: line[auctionid]).distinct().count()

# What is the total number of item types that were auctioned?
auctionRDD.map(lambda line: line[itemtype]).distinct().count()

# What is the total number of bids per item type?
auctionRDD.map(lambda x: (x[itemtype], 1)).reduceByKey(lambda x, y: x + y).collect()

# What is the total number of bids per auction?
auctionRDD.map(lambda x: (x[auctionid], 1)).reduceByKey(lambda x, y: x + y).collect()

# Across all auctioned items, what is the max number of bids?
bids_auctionRDD = auctionRDD.map(lambda x: (x[auctionid], 1)).reduceByKey(lambda x, y: x + y)
bids_auctionRDD.map(lambda x: x[bid]).reduce(max)

# Across all auctioned items, what is the minimum of bids?
bids_auctionRDD = auctionRDD.map(lambda x: (x[auctionid], 1)).reduceByKey(lambda x, y: x + y)
bids_auctionRDD.map(lambda x: x[bid]).reduce(min)

# What is the average bid?
totbids = auctionRDD.count()
totitems = auctionRDD.map(lambda line: line[auctionid]).distinct().count()
totbids / totitems

# Exit the pyspark shell
exit()
