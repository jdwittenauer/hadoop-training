Commands to run labs:

Step 1: First compile the project: Select project 'schemadesignsolution' -> Run As -> Maven Install
Step 2: Copy the lab-exercise-filter-1.0.jar to the cluster

To run the lab to create the tables and add data, note this will delete an existing table:
    java -cp `hbase classpath`:./lab-exercises-filter-solution-1.0.jar filter.CreateTable tall

To run the lab to filter trades with a prefix:
    java -cp `hbase classpath`:./lab-exercises-filter-solution-1.0.jar filter.FilterTradesTall prefix

To run the lab to filter trades with a list:
    java -cp `hbase classpath`:./lab-exercises-filter-solution-1.0.jar filter.FilterTradesTall list 
