CREATE EXTERNAL TABLE flighttable(key string,aircraftdelay FLOAT,arrdelay FLOAT,carrierdelay FLOAT,depdelay FLOAT,weatherdelay float,cncl FLOAT,cnclcode string, tailnum string, airtime FLOAT, distance FLOAT, elaptime FLOAT,arrtime int, deptime int, dom int, dow int, month int)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,delay:aircraftdelay,delay:arrdelay,delay:carrierdelay,delay:depdelay,delay:weatherdelay,info:cncl,info:cnclcode,info:tailnum,stats:airtime,stats:distance,stats:elaptime,timing:arrtime,timing:deptime,timing:dom,timing:dow,timing:month")
TBLPROPERTIES("hbase.table.name" = "/user/user01/tables/airline");

