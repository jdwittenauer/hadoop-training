#!/bin/bash
while read record
do
   year=`echo $record | awk  '{print $1}'`
   delta=`echo $record | awk  '{print $4}'`
   printf "summary\t%s_%s\n" "$year" "$delta"
done

