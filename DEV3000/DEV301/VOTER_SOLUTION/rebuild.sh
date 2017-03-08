#!/bin/bash

USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.5.1
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*export HADOOP_CLASSPATH=$CLASSPATH


javac -d classes VoterMapper.java
javac -d classes VoterReducer.java
jar -cvf Voter.jar -C classes/ .
javac -classpath $CLASSPATH:Voter.jar -d classes VoterDriver.java
jar -uvf Voter.jar -C classes/ .
javac -classpath $CLASSPATH:Voter.jar -d classes VoterTest.java
jar -uvf Voter.jar -C classes/ .
