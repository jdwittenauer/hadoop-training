
Create an hbase table to write to:
launch the hbase shell
$hbase shell

create '/user/user01/sensor', {NAME=>'data'}, {NAME=>'alert'}, {NAME=>'stats'}

Commands to run labs:

Step 1: First compile the project on eclipse: Select project  -> Run As -> Maven Install

Step 2: use scp to copy the sparkstreaminglab-1.0.jar to the mapr sandbox or cluster

scp  sparkstreaminglab-1.0.jar user01@ipaddress:/user/user01/.
if you are using virtualbox:
scp -P 2222 sparkstreaminglab-1.0.jar user01@127.0.0.1:/user/user01/.

To run the  streaming:

Step 3: start the streaming app
 
spark-submit --class solutions.SensorStream --master yarn sparkstreaminglab-1.0.jar

Step 4: copy the streaming data file to the stream directory
cp sensordata.csv  /user/user01/stream/.

Step 5: run another example

spark-submit --class solutions.SensorStreamSQL --master yarn sparkstreaminglab-1.0.jar
spark-submit --class solutions.SensorStreamWindow --master yarn sparkstreaminglab-1.0.jar
spark-submit --class solutions.HBaseSensorStream --master yarn --driver-memory 256m sparkstreaminglab-1.0.jar




