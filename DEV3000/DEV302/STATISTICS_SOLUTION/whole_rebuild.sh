#!/bin/bash

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.7.0
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*export HADOOP_CLASSPATH=$CLASSPATH


javac -d classes UniversityMapper.java
javac -d classes UniversityReducer.java
jar -cvf WholeJob.jar -C classes/ .

javac -d classes StatsMapper.java
javac -d classes StatsReducer.java
jar -uvf WholeJob.jar -C classes/ .

javac -classpath $CLASSPATH:WholeJob.jar -d classes WholeJobDriver.java
jar -uvf WholeJob.jar -C classes/ .
