#      1. Loads sfpd.csv
#      2. Creates a DataFrame (inferred schema by reflection)
#      3. Registers the DataFrame as a table
#      4. Prints the top three categories to the console
#      5. Finds the address, resolution, date and time for burglaries in 2015
#      6. Saves this to a JSON file in a folder /<user home>/appoutput

from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext, Row
import pyspark.sql.functions as func
conf = SparkConf().setAppName("ResolutionApp")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)


#1. Create input RDD
sfpdRDD = sc.textFile("/user/user01/data/sfpd.csv").map(lambda inc: inc.split(","))

#2. Infer the schema, and register the DataFrame as a table.
sfpdSchema=sfpdRDD.map(lambda inc: Row(incidentnum=inc[0],category=inc[1], description=inc[2],dayofweek=inc[3],date=inc[4],time=inc[5],pddistrict=inc[6],resolution=inc[7],address=inc[8],X=float(inc[9]),Y=float(inc[10]), pdid=inc[11]))

sfpdDF=sqlContext.createDataFrame(sfpdSchema)
sfpdDF.cache()

#3. register as table
sfpdDF.registerTempTable("sfpd")

#4. Top 3 categories
top3Cat = sfpdDF.groupBy("category").count().sort(func.desc("count"))
top3Cat.show(3)
top3CatSQL=sqlContext.sql("SELECT category, count(incidentnum) AS inccount FROM sfpd GROUP BY category ORDER BY inccount DESC LIMIT 3")
top3CatSQL.show()                                                            
                                                                                 
#5. register the function as a udf 
sqlContext.registerFunction("getyear",lambda x:x[-2:])
burg2015 = sqlContext.sql("SELECT address,resolution, date, time FROM sfpd WHERE getyear(date)='15' AND category='BURGLARY'")
burg2015.show(5)
burg2015.save("/user/user01/appoutput","json","overwrite")
