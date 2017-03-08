#!/bin/bash

USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.5.1
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*:$HADOOP_HOME/*:/user/$USER/5/VOTER_SOLUTION/*
export HADOOP_CLASSPATH=$CLASSPATH

rm -rf /user/$USER/5/VOTER_SOLUTION/OUT

ARGS=$1

hadoop jar Voter.jar Voter.VoterDriver $ARGS /user/$USER/5/VOTER_SOLUTION/DATA/myvoter.csv /user/$USER/5/VOTER_SOLUTION/OUT

