#!/bin/bash
count=0
sum=0
max=-2147483647
min=2147483647
minyear=""
maxyear=""
while read line
do
   value=`echo $line | awk '{print $2}'`
   if [ -n "$value" ]
   then
      year=`echo $value | awk -F_ '{print $1}'`
      delta=`echo $value | awk -F_ '{print $2}'`
   fi
   if [ $delta -lt $min ]
   then
      min=$delta
      minyear=$year
   elif [ $delta -gt $max ]
   then
      max=$delta
      maxyear=$year
   fi
   
   count=$(( count + 1 )) 
   sum=$(( sum + delta ))
done

mean=$(( sum / count ))

printf "min year is %s\n" "$minyear"
printf "min value is %s\n" "$min"
printf "max year is %s\n" "$maxyear"
printf "max value is %s\n" "$max"
printf "sum is %s\n" "$sum"
printf "count is %s\n" "$count"
printf "mean is %d\n" "$mean"

