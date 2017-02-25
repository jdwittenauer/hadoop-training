ME="user01"

TABLE="/user/$ME/tables/airline"
FILE="ontime.csv"

CF="cf1"

# delete table
maprcli table delete -path $TABLE

# create table
maprcli table create -path $TABLE
maprcli table cf create -path $TABLE -cfname $CF

# HBASE_ROW_KEY_x
#   1 = flight date
#   2 = carrier
#   3 = flight number
#   4 = origin
#   5 = destination

# run m/r import job
java -cp `hbase classpath`:./hbaseimport.jar org.apache.hadoop.hbase.mapreduce.CompositeKeyImportTsv \
    -Dimporttsv.separator=, \
    -Dimporttsv.columns=$CF:year,$CF:qtr,$CF:month,$CF:dom,$CF:dow,HBASE_ROW_KEY_1,HBASE_ROW_KEY_2,$CF:tailnum,HBASE_ROW_KEY_3,HBASE_ROW_KEY_4,HBASE_ROW_KEY_5,$CF:deptime,$CF:depdelay,$CF:arrtime,$CF:arrdelay,$CF:cncl,$CF:cnclcode,$CF:elaptime,$CF:airtime,$CF:distance,$CF:carrierdelay,$CF:weatherdelay,$CF:nasdelay,$CF:securitydelay,$CF:aircraftdelay,$CF:dummy \
    -Dlog4j.configuration=/opt/mapr/hadoop/hadoop-0.20.2/conf/log4j.properties \
    $TABLE \
    /user/$ME/data/$FILE
