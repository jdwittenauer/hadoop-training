ME="user01"

TABLE="/user/$ME/tables/airline"
FILE="ontime.csv"

CF="cf1"

# delete table
maprcli table delete -path $TABLE

# create table
maprcli table create -path $TABLE
maprcli table cf create -path $TABLE -cfname $CF

# run m/r import job
java -cp `hbase classpath`:./hbaseimport.jar org.apache.hadoop.hbase.mapreduce.CompositeKeyImportTsv \
    -Dimporttsv.separator=, \
    -Dimporttsv.columns=$CF:year,$CF:qtr,$CF:month,$CF:dom,$CF:dow,HBASE_ROW_KEY,$CF:carrier,$CF:tailnum,$CF:flightnumber,$CF:origin,$CF:dest,$CF:deptime,$CF:depdelay,$CF:arrtime,$CF:arrdelay,$CF:cncl,$CF:cnclcode,$CF:elaptime,$CF:airtime,$CF:distance,$CF:carrierdelay,$CF:weatherdelay,$CF:nasdelay,$CF:securitydelay,$CF:aircraftdelay,$CF:dummy \
    $TABLE \
    /user/$ME/data/$FILE
