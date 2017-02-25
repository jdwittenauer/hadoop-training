Commands to run labs:

Step 0: replace "user01" with your user in the TradeDAOFlat.java 
	public static String tablePath = "/user/user01/trades_flat"
	replace "user01" with your user in the  TradeDAOTall.java
	public static String tablePath = "/user/user01/trades_tall"

Step 1: First compile the project: Select project 'schemadesign' -> Run As -> Maven Install

Step 2: Copy the schemadesign-1.0.jar to the cluster

To run the  lab to create the flat tables and add data:
java -cp `hbase classpath`:./schemadesign-1.0.jar schemadesign.CreateTable flat ./500trades.txt 

To run the  lab to read trades for  amazon:
 java -cp `hbase classpath`:./schemadesign-1.0.jar schemadesign.LookupTrades flat AMZN



use the hbase shell   (substitute your userid) to delete a table
		disable '/user/user01/trades_flat'
		drop '/user/user01/trades_flat'
		
To run the  lab to create the tall tables and add data:
 java -cp `hbase classpath`:./schemadesign-1.0.jar schemadesign.CreateTable tall ./500trades.txt 
		
To run the  lab to read trades for  amazon:		
java -cp `hbase classpath`:./schemadesign-1.0.jar \
    schemadesign.LookupTrades tall AMZN 20131021 20131022
		
