#!/bin/bash

# open file in xls

# save file in unicode

# copy file to cluster

# convert unicode (utf-16) to utf-8
iconv -f UTF-16 -t UTF-8 receipts_outlays_surplus_fed.txt > receipts_outlays_surplus_fed-utf8.txt

sed '1,$ s/\"//g' receipts_outlays_surplus_fed-utf8.txt > receipts-temp.txt

sed '1,$ s/\,//g' receipts-temp.txt > receipts-temp2.txt

# remove lines with bad values in 1st and 3rd fields
