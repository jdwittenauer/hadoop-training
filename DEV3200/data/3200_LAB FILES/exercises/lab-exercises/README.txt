Commands to run labs:

Step 1: First compile the project: Select project 'lab-exercises' -> Run As -> Maven Install

Step 2: Copy the lab-exercises-1.0.jar to the cluster
	
	
To run the admin lab:
java -cp `hbase classpath`:./lab-exercises-1.0.jar adminApi.LabAdminAPI  setup

java -cp `hbase classpath`:./lab-exercises-1.0.jar adminApi.LabAdminAPISolution setupmaxversions

java -cp `hbase classpath`:./lab-exercises-1.0.jar adminApi.LabAdminAPISolution presplit

java -cp `hbase classpath`:./lab-exercises-1.0.jar adminApi.LabAdminAPISolution listtables

To run the bulk load lab:

java -cp `hbase classpath`:./lab-exercises-1.0.jar bulkload.BulkLoadMapReduce /user/user01/hly_temp /user/user01/input/ /user/user01/output/
	

export LD_LIBRARY_PATH=/opt/mapr/hadoop/hadoop-0.20.2/lib/native/Linux-amd64-64
    