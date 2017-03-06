Create an hbase table to write to:
$ hbase shell
> create '/user/user01/sensor', {NAME=>'data'}, {NAME=>'alert'}, {NAME=>'stats'}

Commands to run labs:

Step 1: First compile the project on eclipse: Select project -> Run As -> Maven Install
Step 2: scp sparkstreaminglab-1.0.jar user01@ipaddress:/user/user01/.
Step 3: spark-submit --class solutions.SensorStream --master yarn sparkstreaminglab-1.0.jar
Step 4: cp sensordata.csv  /user/user01/stream/.
Step 5: Run other examples:
            spark-submit --class solutions.SensorStreamSQL --master yarn sparkstreaminglab-1.0.jar
            spark-submit --class solutions.SensorStreamWindow --master yarn sparkstreaminglab-1.0.jar
            spark-submit --class solutions.HBaseSensorStream --master yarn --driver-memory 256m sparkstreaminglab-1.0.jar
