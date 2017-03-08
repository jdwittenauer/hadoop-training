#!/bin/bash
USER=`whoami`

export HADOOP_HOME=/opt/mapr/hadoop/hadoop-2.5.1
export LD_LIBRARY_PATH=$HADOOP_HOME/lib/native/Linux-amd64-64
export CLASSPATH=$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/mapreduce/lib/*:$HADOOP_HOME/share/hadoop/common/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/tools/lib/*:$HADOOP_HOME/share/hadoop/yarn/*:$HADOOP_HOME/share/hadoop/yarn/lib/*
export HADOOP_CLASSPATH=$CLASSPATH

if [ $# -ne 2 ]
then
   echo "usage $0 <map|reduce> <testfile>"
   exit 1
fi

if [ ! -f "$2" ]
then
   echo "file $2 does not exist -- exiting"
   exit 1
fi

hadoop jar Voter.jar Voter.VoterTest $1 $2
