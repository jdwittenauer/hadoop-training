# Lab Guide
##Overview

##Lab Procedure

* Obtain Sandbox VM (if you don't already have it) 

### Pre-req's

* Laptop with at least 8GB of RAM
* 20GB or more free space on your harddrive (ideally SSD or Hybrid)
* VMware Fusion  or Virtualbox 
* MapR Sandbox downloaded

###Importing/starting up

First, load the sandbox into your vmware or virtualbox.  

###Logging in

Once the sandbox has been powered up, it will initiate a startup sequence which may take several minutes.  Your sandbox will be ready to login to when you see a screen similar to:

![Startup success

> ***Note*** If you see a message that not all services were able to start, try restarting the virtual machine.  

Note the URL will contain an IP address local to your machine.  You'll be using this IP to SSH to 

On a Windows machine, download and install a terminal emulation client such as Putty.  On a Mac or Linux machine, simply open a terminal window and SSH.

The login for the CLI is `user01`, the password is  `user01` .


###Create HIVE external table

 HIVE's external table functionality allows you to create a table which sources its data from an existing HBase table.   
 All commands below are run on a terminal session, connected to the Sandbox.
 If you have not already created and loaded the HBase tables from lab 4, go to that lab and do that first. 

1.	
examine (edit if you are not using user01) a text file, which will be used when creating the table:

cat create_ext_hive_table.hql

	
		CREATE EXTERNAL TABLE flighttable(key string,aircraftdelay FLOAT,arrdelay FLOAT,carrierdelay FLOAT,depdelay FLOAT,weatherdelay float,cncl FLOAT,cnclcode string, tailnum string, airtime FLOAT, distance FLOAT, elaptime FLOAT,arrtime int, deptime int, dom int, dow int, month int)
		STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,delay:aircraftdelay,delay:arrdelay,delay:carrierdelay,delay:depdelay,delay:weatherdelay,info:cncl,info:cnclcode,info:tailnum,stats:airtime,stats:distance,stats:elaptime,timing:arrtime,timing:deptime,timing:dom,timing:dow,timing:month")TBLPROPERTIES("hbase.table.name" = "/user/user01/tables/airline");

	
2. Execute the query:

hive -f /user/user01/create_ext_hive_table.hql

	This may take 1-2 minutes, and the result should be similar to:
	
		OK
		Time taken: 27.06 seconds

3.  Verify table existence:

		hive -e "show tables;"
	Which should now show:
	
		OK
		flighttable
		Time taken: 11.865 seconds, Fetched: 1 row(s)

4.  you should now be able to query it using the HIVE managed HBase table:

	hive -e "select * from flighttable LIMIT 2;"
	This should return:
	
    "HIVE limit w result")

	Note that many rows have NULL values for some columns.  This is expected, since canceled flights won't have an 'elapsedtime', and typically not all delay types are used for a single delayed flight.  
	
### what do we want to ask of this?

Think now for a moment what sorts of questions you would like to ask of this data.  You can then transform those questions into queries which can be executed against the data, using HIVE which will run map reduce jobs.  Some example questions:

* worst delay for an airline? (by # of late flights..minutes late..etc)
* worst airport?
* most common cancelation reasons?

### Query

Some example queries have been placed into the lab files queries.txt and files ending with .hql:

You are welcome to use them, or come up with your own.  If you like, you can scp the hql files to your sandbox or aws:

	scp *.hql root@192.168.6.132:/user/user01
	
#### in HIVE

Before you execute any queries in HIVE, its important to understand what the query will do when it runs. More specifically, how many mapReduce jobs will be run?  A simple way to do this is to slightly modify the contents of your query, merely insert the word explain at the beginning of the statement, so it is the first word on the first line. For Example:

EXPLAIN select count(*) as cancellations,cnclcode
from flighttable 
where cncl=1 
group by cnclcode
order by cancellations asc limit 100;

Now run the query. The output will be long and verbose, showing you details about each stage, column, and variable used to execute the query.  One item that is important to look for are the number of times you see "Map Reduce" mentioned. 

Which in this case yields 2 lines.  This effectively means that 2 MR jobs will run if this query were to actually execute. 

Now remove the 'EXPLAIN' command from the beginning of the query and run it.
Here is the (shortened) output 
Total jobs = 2
MapReduce Jobs Launched:
Job 0: Map: 1  Reduce: 1   Cumulative CPU: 13.3 sec   MAPRFS Read: 0 MAPRFS Write: 0 SUCCESS
Job 1: Map: 1  Reduce: 1   Cumulative CPU: 1.52 sec   MAPRFS Read: 0 MAPRFS Write: 0 SUCCESS
Total MapReduce CPU Time Spent: 14 seconds 820 msec
OK
4598    C
7146    A
19108   B

try running additional queries that are in queries.txt
