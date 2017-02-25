ME="user01"

TABLE="/user/$ME/tables/airline"
FILE="ontime.csv"


# delete table
maprcli table delete -path $TABLE

# create table
maprcli table create -path $TABLE
maprcli table cf create -path $TABLE -cfname timing
maprcli table cf create -path $TABLE -cfname info
maprcli table cf create -path $TABLE -cfname stats
maprcli table cf create -path $TABLE -cfname delay

# HBASE_ROW_KEY_x
#   1 = carrier
#   2 = flight number
#   3 = flight date
#   4 = origin
#   5 = destination

# run m/r import job
java -cp `hbase classpath`:./hbaseimport.jar org.apache.hadoop.hbase.mapreduce.CompositeKeyImportTsv \
    -Dimporttsv.separator=, \
    -Dimporttsv.columns=timing:year,timing:qtr,timing:month,timing:dom,timing:dow,HBASE_ROW_KEY_3,HBASE_ROW_KEY_1,info:tailnum,HBASE_ROW_KEY_2,HBASE_ROW_KEY_4,HBASE_ROW_KEY_5,timing:deptime,delay:depdelay,timing:arrtime,delay:arrdelay,info:cncl,info:cnclcode,stats:elaptime,stats:airtime,stats:distance,delay:carrierdelay,delay:weatherdelay,delay:nasdelay,delay:securitydelay,delay:aircraftdelay,info:dummy \
    -Dlog4j.configuration=/opt/mapr/hadoop/hadoop-0.20.2/conf/log4j.properties \
    $TABLE \
    /user/$ME/data/$FILE
