#  Lab



scp  the lab zip file  to your sandbox or aws cluster in your home  ( /user/user01 ) folder 
as described in the connecting to the sandbox document

for example if using virtual box : 
scp -P 2222 downloads/Lab_4_schema_import_lab.zip user01@127.0.0.1:/user/user01/.

Log into the sandbox or aws cluster via SSH 

unzip Lab_4_schema_import_lab.zip
    
if you are using aws with a different userid  change /user/user01 in all of the scripts to your userid home folder

## Exercise 1



Let's start by loading some data into a table.  We are going to use a modified version of the HBase utility, ImportTsv, which imports CSV files like our sample airline data set.

In your home directory, make all shell scripts executable. Create two sub-directories - data and tables.   Move the ontime.csv file to the data subdirectory:
$ chmod +x *.sh 
$ mkdir data
$ mkdir tables
$ mv ontime.csv data/.

Run the script to import data:
$ ./import1.sh


Launch the HBase shell and use the count command to count the number of rows in  the table : 
$hbase shell
hbase(main):003:0> count '/user/user01/tables/airline'
31 row(s) in 0.0560 seconds

 you see that it only has 31 rows.  This is the problem with ImportTsv as it stands today, because it doesn't use composite keys, and there are only 31 dates in the file of more than 471,000 flights.  Each of the column values was overwritten for the next flight, because the rowkey is not unique.

try out some queries in HBase shell
https://learnhbase.wordpress.com/2013/03/02/hbase-shell-commands/

Scan one row using the HBase shell 
scan '/user/user01/tables/airline', {LIMIT => 1}

scan '/user/user01/tables/airline'

scan  '/user/user01/tables/airline',  { STARTROW => '2014'}
scan  '/user/user01/tables/airline',  { STARTROW => '2014-01-20', STOPROW => '2014-01-21'}

## Exercise 2

Luckily, we have gone ahead and [patched ImportTsv](https://github.com/boorad/CompositeKeyImportTsv) to handle composite keys, as requested in [HBASE-5339](https://issues.apache.org/jira/browse/HBASE-5339). 

View and then execute the import2.sh script

	more import2.sh
	./import2.sh

Note that in the import script, we specify five different fields as our composite rowkey.

	HBASE_ROW_KEY_x
  	1 = flight date
  	2 = carrier
  	3 = flight number
  	4 = origin
  	5 = destination

Take a look at the results of the table load in the HBase shell

    count '/user/user01/tables/airline'

    scan '/user/user01/tables/airline', {LIMIT => 1}

    scan  '/user/user01/tables/airline',  { STARTROW => '2014-01-20', STOPROW => '2014-01-21'}

    scan  '/user/user01/tables/airline',  { STARTROW => '2014-01-16-AA',  STOPROW => '2014-01-16-AB'}


## Exercise 3

Execute the import3.sh script

    more import3.sh
    ./import3.sh

Take a look at the results of the table load in the HBase shell

    > scan '/user/user01/tables/airline', {LIMIT => 5}


We have a composite key, with Carrier as the first field.  This will be better for queries that scan or filter by Carrier.

Try out some scans 
scan '/user/user01/tables/airline', FILTER=>"ValueFilter(=,'binary:239.00')"

scan '/user/user01/tables/airline', {FILTER => "SingleColumnValueFilter('cf1','cncl', =,'binary:1.00')"}

scan '/user/user01/tables/airline', FILTER=>"ColumnPrefixFilter('cnc')"

scan  '/user/user01/tables/airline',  { STARTROW => 'AA-1-2014-01-01-JFK-LAX', STOPROW => 'AA-10', FILTER=>"ColumnPrefixFilter('cnc')"}

scan '/user/user01/tables/airline', {COLUMNS => ['cf1:carrierdelay'],FILTER => "(SingleColumnValueFilter('cf1','carrierdelay',=,'regexstring:^[2-9]{3}')"}

You can also pass a script file to the hbase shell. Exit the shell and look at the file scan4, then Scan to find flights with AA and  JFK in the row key

   cat scan4
   hbase shell < scan4



## Exercise 4

We now want to explore how adding column families to your table helps with performance.  On disk, column families and their tablets are separate files.  If your query only needs to read from a certain group of columns, and those columns are in a column family that is isolated from others, then reading operations will only be performed on those tablets/files, saving iops.

Notice we have 4 column families, and if a query doesn't use any of the columns in the 'delay' column family, it won't be read from disk.

    less import4.sh
    ./import4.sh

Take a look at the results of the table load in the HBase shell

  describe '/user/user01/tables/airline'
  scan '/user/user01/tables/airline', {LIMIT => 5}

  scan   '/user/user01/tables/airline',  {COLUMNS=>['stats'], STARTROW => 'AA', LIMIT => 5}
  scan   '/user/user01/tables/airline',  {COLUMNS=>['delay'], STARTROW => 'AA', LIMIT => 5}


## Credits
The dataset was gathered from [here](https://explore.data.gov/download/ar4r-an9z/DATAEXTRACTION) 

