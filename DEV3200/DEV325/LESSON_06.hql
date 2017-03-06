-- Launch the hive shell from ssh
-- $ hive

-- Create an external table to give hive access to the hbase airline table
CREATE EXTERNAL TABLE flighttable(key string, aircraftdelay FLOAT, arrdelay FLOAT, carrierdelay FLOAT, depdelay FLOAT, weatherdelay float, cncl FLOAT, cnclcode string, tailnum string, airtime FLOAT, distance FLOAT, elaptime FLOAT, arrtime int, deptime int, dom int, dow int, month int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key, delay:aircraftdelay, delay:arrdelay, delay:carrierdelay, delay:depdelay, delay:weatherdelay, info:cncl, info:cnclcode, info:tailnum, stats:airtime, stats:distance, stats:elaptime, timing:arrtime, timing:deptime, timing:dom, timing:dow, timing:month")
TBLPROPERTIES("hbase.table.name" = "/user/user01/tables/airline");

-- Use explain keyword to see what mapreduce jobs will be run
EXPLAIN select count(*) as cancellations, cnclcode from flighttable  where cncl=1  group by cnclcode order by cancellations asc limit 100;

-- Count of cancellations by cancelcode 
select count(*) as cancellations, cnclcode
from flighttable 
where cncl=1 
group by cnclcode
order by cancellations asc limit 100;

-- Count of cancellations for american airlines
select count(*) as cancellations 
from flighttable 
where cncl=1 and key like "AA%";

-- Longest delay
select key, a.arrdelay, a.elaptime, a.airtime, a.distance 
from flighttable a
order by a.arrdelay desc limit 20;  

-- Fastest airspeed
select key, a.arrdelay, a.elaptime, a.airtime, a.distance, ((a.distance / a.airtime) * 60) as airspeed
from flighttable a
order by airspeed desc limit 20;

-- Longest delay for United
select max(arrdelay)
from flighttable 
where key like "UA%";

-- Longest carrier delay
select carrierdelay, key
from flighttable 
order by carrierdelay desc limit 20;

-- Max carrier delay for delta
select max(a.carrierdelay) as CarrierDelayMinutes 
from flighttable a
where  key like "DL%";

-- Create an external table to give hive access to the hbase trades table
CREATE EXTERNAL TABLE trades(key string, price bigint, vol bigint)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "CF1:price#b,CF1:vol#b")
TBLPROPERTIES("hbase.table.name" = "/user/user01/trades_tall");

-- Run some queries on the new external table
describe trades;
select * from trades;
select * from trades where key like "GOOG%";
select avg(price) from trades where key like "GOOG%"
