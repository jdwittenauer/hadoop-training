#!/bin/bash
javac -d classes UniversityMapper.java
javac -d classes UniversityReducer.java
jar -cvf University.jar -C classes/ .
javac -classpath $CLASSPATH:University.jar -d classes UniversityDriver.java
jar -cvf University.jar -C classes/ .
