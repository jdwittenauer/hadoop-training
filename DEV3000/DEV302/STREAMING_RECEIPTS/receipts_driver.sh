#!/usr/bin/env bash
USER=`whoami`
# 1) test map script
echo -e "1901 588 525 63 588 525 63" | ./receipts_mapper.sh | od -c
# 2) test reduce script
echo -e "summary\t1901_63" | ./receipts_reducer.sh | od -c
# 3) map/reduce on Hadoop
export JOBHOME=/user/$USER/9/STREAMING_RECEIPTS
export CONTRIB=/opt/mapr/hadoop/hadoop-2.5.1/share/hadoop/tools/lib
export STREAMINGJAR=hadoop-streaming-2.5.1-mapr-1503.jar
export THEJARFILE=$CONTRIB/$STREAMINGJAR
rm -rf $JOBHOME/OUT
hadoop jar $THEJARFILE \
  -mapper 'receipts_mapper.sh' \
  -file receipts_mapper.sh \
  -reducer 'receipts_reducer.sh' \
  -file receipts_reducer.sh \
  -input  $JOBHOME/DATA/receipts.txt  \
  -output  $JOBHOME/OUT
