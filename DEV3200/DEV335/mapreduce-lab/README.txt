Commands to run labs:

Step 0: replace "user01" with your user in the StockDriver.java 


Step 1: First compile the project: Select project  -> Run As -> Maven Install

Step 2: Copy the mapreduce-lab-1.0.jar to the cluster
   scp mapreduce-lab-1.0.jar userXX@host:


FLAT WIDE    
Run a map only  program on an M7 HBase table:   
java -cp `hbase classpath`:./mapreduce-lab-1.0.jar mapreducelab.flatwide.StockDriver ~/OUT2   

to see the results type
cat ~/OUT2/part-r-00000
you should see results like 
GOOG_20131025   6155
GOOG_20131025   5095


Run a map-reduce program on an M7 HBase table:
finish the todos 
java -cp `hbase classpath`:./mapreduce-lab-1.0.jar mapreducelab.flatwide.StockDriver ~/OUT3
	
or Run the solution : 
java -cp `hbase classpath`:./mapreduce-lab-1.0.jar mapreducesolution.flatwide.StockDriver ~/OUT3	

to see results : in hbase shell type
scan ‘/user/user01/trades_flat’
you should see summaries like this for each symbol
 AMZN_20131022        column=stats:count, timestamp=1385137969400, value=34
 AMZN_20131022        column=stats:max, timestamp=1385137969400, value=99.87
 AMZN_20131022        column=stats:mean, timestamp=1385137969400, value=75.91
 AMZN_20131022        column=stats:min, timestamp=1385137969400, value=50.04


TALL NARROW
Run a map only  program on an M7 HBase table: 
java -cp `hbase classpath`:./mapreduce-lab-1.0.jar mapreducelab.tallnarrow.StockDriver ~/OUT2

to see the results type
cat ~/OUT2/part-r-00000

you should see results like this
GOOG    6529
GOOG    6108


java -cp `hbase classpath`:./mapreduce-lab-1.0.jar mapreducesolution.tallnarrow.StockDriver ~/OUT3

to see results : in hbase shell type
scan '/user/user01/trades_tall'
you should see summaries like this for each symbol

 CSCO                 column=CF1:count, timestamp=1385138594695, value=165
 CSCO                 column=CF1:max, timestamp=1385138594695, value=99.97
 CSCO                 column=CF1:mean, timestamp=1385138594695, value=76.88
 CSCO                 column=CF1:min, timestamp=1385138594695, value=50.22
	



 		
