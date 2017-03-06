# Launch the interactive pyspark shell
# $ /opt/mapr/spark/spark-1.6.1/bin/pyspark

# Imports
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
sqlContext = SQLContext(sc)

# Load the data
sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(lambda inc: inc.split(","))

# Infer the schema and register the DataFrame as a table
sfpdSchema=sfpdRDD.map(lambda inc: Row(incidentnum=inc[0], category=inc[1], description=inc[2], dayofweek=inc[3], date=inc[4], time=inc[5], pddistrict=inc[6], resolution=inc[7], address=inc[8], X=float(inc[9]), Y=float(inc[10]), pdid=inc[11]))
sfpdDF=sqlContext.createDataFrame(sfpdSchema)
sfpdDF.registerTempTable("sfpd")

# Top 5 districts
incByDist = sfpdDF.groupBy("pddistrict").count().sort(func.desc("count"))                
topByDistSQL = sqlContext.sql("SELECT pddistrict, count(incidentnum) AS inccount FROM sfpd GROUP BY pddistrict ORDER BY inccount DESC LIMIT 5")
topByDistSQL.show()

# What are the top ten resolutions?
top10Res = sfpdDF.groupBy("resolution").count().sort(func.desc("count"))
top10ResSQL = sqlContext.sql("SELECT resolution, count(incidentnum) AS inccount FROM sfpd GROUP BY resolution ORDER BY inccount DESC LIMIT 10")
top10ResSQL.show()

# Top 3 categories
top3Cat = sfpdDF.groupBy("category").count().sort(func.desc("count"))
top3CatSQL=sqlContext.sql("SELECT category, count(incidentnum) AS inccount FROM sfpd GROUP BY category ORDER BY inccount DESC LIMIT 3")
top3CatSQL.show()

# Save the top 10 resolutions to a JSON file
top10ResSQL.toJSON.saveAsTextFile("/user/user01/output")

# Define and register UDF to get year from a date string
sqlContext.registerFunction("getyear", lambda x: x[-2:])

# Count inc by year
incyearSQL = sqlContext.sql("SELECT getyear(date), count(incidentnum) AS countbyyear FROM sfpd GROUP BY getyear(date) ORDER BY countbyyear DESC")
incyearSQL.show()

# Category, resolution and address of reported incidents in 2014 
inc2014 = sqlContext.sql("SELECT category, address, resolution, date FROM sfpd WHERE getyear(date)='14'")
inc2014.show()

# Vandalism only in 2016 with address, resolution and category
van2015 = sqlContext.sql("SELECT category, address, resolution, date FROM sfpd WHERE getyear(date)='15' AND category='VANDALISM'")
van2015.show()
