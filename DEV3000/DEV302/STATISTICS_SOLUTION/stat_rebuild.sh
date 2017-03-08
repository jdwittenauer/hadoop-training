#!/bin/bash
javac -d classes StatsMapper.java
javac -d classes StatsReducer.java
jar -cvf Stats.jar -C classes/ .
javac -classpath $CLASSPATH:Stats.jar -d classes StatsDriver.java
jar -cvf Stats.jar -C classes/ .
