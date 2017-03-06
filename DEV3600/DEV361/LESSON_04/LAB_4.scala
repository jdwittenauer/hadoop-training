// Launch the interactive spark shell
// $ /opt/mapr/spark/spark-1.6.1/bin/spark-shell --master local[2]

// Map input variables
val IncidntNum = 0
val Category = 1
val Descript = 2
val DayOfWeek = 3
val Date = 4
val Time = 5
val PdDistrict = 6
val Resolution = 7
val Address = 8
val X = 9
val Y = 10
val PdId = 11

// Load SFPD data into an RDD
val sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(line => line.split(","))

// How do you see the first element of the inputRDD?
sfpdRDD.first()

// What do you use to see the first 5 elements of the RDD?
sfpdRDD.take(5)

// What is the total number of incidents?
sfpdRDD.count()

// What is the total number of distinct resolutions?
sfpdRDD.map(inc => inc(Resolution)).distinct.count()

// List all the districts
sfpdRDD.map(inc => inc(PdDistrict)).distinct().collect()

// Which five districts have the highest incidents?
sfpdRDD.map(incident => (incident(PdDistrict), 1)).reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).take(5)

// Which five addresses have the highest number of incidents?
sfpdRDD.map(incident => (incident(Address), 1)).reduceByKey((x, y) => x + y).map(x => (x._2, x._1)).sortByKey(false).take(5)

// What are the top three catefories of incidents?
sfpdRDD.map(incident => (incident(Category), 1)).reduceByKey((x , y) => x + y).map(x => (x._2, x._1)).sortByKey(false).take(3)

// What is the count of incidents by district?
sfpdRDD.map(incident => (incident(PdDistrict), 1)).countByKey()

// Load two datasets into separate pairRDDs with “address” being the key
val catAdd = sc.textFile("/user/user01/data/J_AddCat.csv").map(x => x.split(",")).map(x => (x(1), x(0)))
val distAdd = sc.textFile("/user/user01/data/J_AddDist.csv").map(x => x.split(",")).map(x => (x(1), x(0)))

// List the incident category and district for those addresses that have both category and district information
val catJdist = catAdd.join(distAdd)
catJdist.collect()
catJdist.count()
catJdist.take(10)

// List the incident category and district for all addresses irrespective of whether each address has category and district information
val catJdist1 = catAdd.leftOuterJoin(distAdd)
catJdist1.collect()
catJdist.count()

// List the incident district and category for all addresses irrespective of whether each address has category and district information
val catJdist2 = catAdd.rightOuterJoin(distAdd)
catJdist2.collect()
catJdist2.count()

// How many partitions are there in the sfpdRDD?
sfpdRDD.partitions.size()

// How do you find the type of partitioner?
sfpdRDD.partitioner

// Create a pair RDD for incidents by district
val incByDists = sfpdRDD.map(incident => (incident(PdDistrict), 1)).reduceByKey((x, y) => x + y)

// How many partitions does this have?
incByDists.partitions.size()

// What is the type of partitioner?
incByDists.partitioner

// Now add a map transformation
val inc_map = incByDists.map(x => (x._2, x._1))

// Is there a change in the size?
inc_map.partitions.size()

// What about the type of partitioner?
inc_map.partitioner

// Add sort by key
val inc_sort = inc_map.sortByKey(false)
inc_sort.partitioner

// Add group by key
val inc_group = sfpdRDD.map(incident => (incident(PdDistrict), 1)).groupByKey()
inc_group.partitioner

// Specify partition size in the transformation
val incByDists = sfpdRDD.map(incident => (incident(PdDistrict), 1)).reduceByKey((x , y) => x + y, 10)
incByDists.partitions.size()

// Create pairRDDs
val catAdd = sc.textFile("/user/user01/data/J_AddCat.csv").map(x => x.split(",")).map(x => (x(1), x(0)))
val distAdd = sc.textFile("/user/user01/data/J_AddDist.csv").map(x => x.split(",")).map(x => (x(1), x(0)))

// Join and specify partitions, then check the number of partitions and the partitioner
val catJdist = catAdd.join(distAdd, 8)
catJdist.partitions.size()
catjDist.partitioner
