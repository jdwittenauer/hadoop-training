#!/bin/bash

rm -rf ~/DISTRIBUTED_CACHE/UNIV_OUT
ARGS=$1

echo "args are $ARGS"

#hadoop jar University.jar University.UniversityDriver $ARGS ~/DISTRIBUTED_CACHE/DATA/university.txt ~/DISTRIBUTED_CACHE/UNIV_OUT
hadoop jar University.jar University.UniversityDriver -D var1="verbal" -D var2="math"  ~/DISTRIBUTED_CACHE/DATA/university.txt ~/DISTRIBUTED_CACHE/UNIV_OUT

