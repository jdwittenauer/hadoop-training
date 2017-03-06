# Launch the interactive pyspark shell
# $ /opt/mapr/spark/spark-1.6.1/bin/pyspark

# Map input variables
IncidntNum = 0
Category = 1
Descript = 2
DayOfWeek = 3
Date = 4
Time = 5
PdDistrict = 6
Resolution = 7
Address = 8
X = 9
Y = 10
PdId = 11

# Load SFPD data into an RDD
sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(lambda line: line.split(","))

# How do you see the first element of the inputRDD?
sfpdRDD.first()

# What do you use to see the first 5 elements of the RDD?
sfpdRDD.take(5)

# What is the total number of incidents?
sfpdRDD.count()

# What is the total number of distinct resolutions?
sfpdRDD.map(lambda inc: inc[Resolution]).distinct().count()

# List all the districts
sfpdRDD.map(lambda inc: inc[PdDistrict]).distinct().collect()

# Which five districts have the highest incidents?
sfpdRDD.map(lambda incident: (incident[PdDistrict], 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey(False).take(5)

# Which five addresses have the highest number of incidents?
sfpdRDD.map(lambda incident: (incident[Address], 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey(False).take(5)

# What are the top three catefories of incidents?
sfpdRDD.map(lambda incident: (incident[Category], 1)).reduceByKey(lambda x, y: x + y).map(lambda x: (x[1], x[0])).sortByKey(False).take(3)

# What is the count of incidents by district?
sfpdRDD.map(lambda incident: (incident[PdDistrict], 1)).countByKey()

# Load two datasets into separate pairRDDs with “address” being the key
catAdd = sc.textFile("/path/to/file/J_AddCat.csv").map(lambda x: x.split(",")).map(lambda x: (x[1], x[0]))
distAdd = sc.textFile("/path/to/file/J_AddDist.csv").map(lambda x: x.split(",")).map(lambda x: (x[1], x[0]))

# List the incident category and district for those addresses that have both category and district information
catJdist = catAdd.join(distAdd)
catJDist.collect()
catJdist.count()
catJdist.take(10)

# List the incident category and district for all addresses irrespective of whether each address has category and district information
catJdist1 = catAdd.leftOuterJoin(distAdd)
catJdist1.collect()
catJdist.count()

# List the incident district and category for all addresses irrespective of whether each address has category and district information
catJdist2 = catAdd.rightOuterJoin(distAdd)
catJdist2.collect()
catJdist2.count()

# How many partitions are there in the sfpdRDD?
sfpdRDD.getNumPartitions()

# How do you find the type of partitioner?
sfpdRDD.partitioner

# Create a pair RDD for incidents by district
incByDists = sfpdRDD.map(lambda incident: (incident[PdDistrict], 1)).reduceByKey(lambda x, y: x + y)

# How many partitions does this have?
incByDists.partitions.size()

# What is the type of partitioner?
incByDists.partitioner

# Now add a map transformation
inc_map = incByDists.map(lambda x: (x[1], x[0]))

# Is there a change in the size?
inc_map.partitions.size()

# What about the type of partitioner?
inc_map.partitioner

# Add sort by key
inc_sort = inc_map.sortByKey(false)
inc_sort.partitioner

# Add group by key
inc_group = sfpdRDD.map(lambda incident: (incident[PdDistrict], 1)).groupByKey()
inc_group.partitioner

# Specify partition size in the transformation
incByDists = sfpdRDD.map(lambda incident: (incident[PdDistrict], 1)).reduceByKey(lambda x, y: x + y, 10)
incByDists.partitions.size()

# Create pairRDDs
catAdd = sc.textFile("/user/user01/data/J_AddCat.csv").map(lambda x: x.split(",")).map(lambda x: (x[1], x[0]))
distAdd = sc.textFile("/user/user01/data/J_AddDist.csv").map(lambda x: x.split(",")).map(lambda x: (x[1], x[0]))

# Join and specify partitions, then check the number of partitions and the partitioner
catJdist = catAdd.join(distAdd, 8)
catJdist.partitions.size()
catjDist.partitioner
