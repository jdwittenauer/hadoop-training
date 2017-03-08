#!/bin/bash
export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.5.1
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*
export HADOOP_CLASSPATH=$CLASSPATH
export HADOOP_HOME=/opt/mapr/hadoop/hadoop-0.20.2

javac -d classes UniversityMapper.java
javac -d classes UniversityReducer.java
jar -cvf University.jar -C classes/ .
javac -classpath $CLASSPATH:University.jar -d classes UniversityDriver.java
jar -cvf University.jar -C classes/ .
