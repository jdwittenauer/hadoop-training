#!/bin/bash
export HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=`hbase classpath`
export HADOOP_CLASSPATH=$CLASSPATH

javac -d classes VoterHbaseMapper.java
javac -d classes VoterHbaseReducer.java
jar -cvf VoterHbase.jar -C classes/ .
javac -classpath $CLASSPATH:VoterHbase.jar -d classes VoterHbaseDriver.java
jar -uvf VoterHbase.jar -C classes/ .
