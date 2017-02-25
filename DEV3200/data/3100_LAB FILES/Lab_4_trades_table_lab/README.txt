#  Lab

use scp to Copy the Lab_4_schema_import_lab.zip  to the aws cluster 
or sandbox in your home folder 

for example if using virtual box : 
scp -P 2222 downloads/Lab_4_schema_import_lab.zip user01@127.0.0.1:/user/user01/.

Log into the sandbox via SSH or your VM window
    
if you are using the cluster change user01  to your user

## Exercise 1

To run the  lab to create the tables and add data:
java -cp `hbase classpath`:./schemadesignsolution-1.0.jar schemadesign.CreateTable tall ./500trades.txt 

java -cp `hbase classpath`:./schemadesignsolution-1.0.jar schemadesign.CreateTable flat ./500trades.txt 

 
To run the  lab to read trades for  amazon:
java -cp `hbase classpath`:./schemadesignsolution-1.0.jar schemadesign.LookupTrades tall AMZN 20131021 20131022
java -cp `hbase classpath`:./schemadesignsolution-1.0.jar schemadesign.LookupTrades flat AMZN 20131021 20131022

try out some queries in HBase shell
https://learnhbase.wordpress.com/2013/03/02/hbase-shell-commands/

Scan one row using the HBase shell 
scan '/user/user01/trades_flat', {LIMIT => 1}
  
scan '/user/user01/trades_tall', {LIMIT => 1}

