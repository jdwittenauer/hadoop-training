Commands to run labs:

Step 1: First compile the project: Select project  -> Run As -> Maven Install
Step 2: Copy the mapreduce-lab-1.0.jar to the cluster

FLAT WIDE    

Run the solution: 
java -cp `hbase classpath`:./mapreduce-lab-1.0.jar mapreducesolution.flatwide.StockDriver ~/OUT3	

to see results in hbase shell type
scan ‘/user/user01/trades_flat’

TALL NARROW

Run the solution: 
java -cp `hbase classpath`:./mapreduce-lab-1.0.jar mapreducesolution.tallnarrow.StockDriver ~/OUT3

to see results in hbase shell type
scan '/user/user01/trades_tall'