# Launch the interactive pyspark shell
# $ /opt/mapr/spark/spark-1.6.1/bin/pyspark

# Create a SQL context
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlContext = SQLContext(sc)

# Load the data
inputRDD = sc.textFile("/user/user01/data/auctiondata.csv").map(lambda line: line.split(","))

# Map the input RDD to the case class
val auctionRDD = inputRDD.map(lambda p: Row(auctionid=p[0], bid=float(p[1]), bidtime=float(p[2]), bidder=p[3], bidrate=int(p[4]), openbid=float(p[5]), price=float(p[6]), itemtype=p[7], dtl=int(p[8])))

# Convert RDD to a DataFrame
auctionDF = sqlContext.createDataFrame(auctionRDD)

# Register as a temporary table with the same name
auctionDF.registerTempTable("auctions")

# Check the data in the DataFrame
auctionDF.show()

# See the schema of the DataFrame
auctionDF.printSchema()

# Total number of bids
totalbids = auctionDF.count()

# Number of distinct auctions
auctionDF.select("auctionid").distinct.count()

# Number of distinct item types
auctionDF.select("itemtype").distinct.count()

# Count of bids per auction and item type
auctionDF.groupBy("itemtype","auctionid").count().show()

# For each auction item and item type, get the min, max and average number of bids
auctionDF.groupBy("itemtype","auctionid").count().agg(func.min("count"), func.max("count"), func.avg("count")).show()

# For each auction item and item type, get the min, max, and average bid
auctionDF.groupBy("itemtype", "auctionid").agg(func.min("bid"), func.max("bid"), func.avg("bid")).show()

# Return the count of all auctions with final price greater than 200
auctionDF.filter(auctiondf.price > 200).count()

# Select just xbox auctions
xboxes = sqlContext.sql("SELECT auctionid, itemtype, bid, price, openbid FROM auctions WHERE itemtype = 'xbox'")

# Compute basic statistics on price across all auctions on xboxes
xboxes.describe("price").show()

# Exit the pyspark shell
exit()
