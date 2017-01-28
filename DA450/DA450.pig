-- Launch pig shell from ssh
-- $ pig

-- List available commands
help

-- Load data into a relation
TEMPERATURE = LOAD '/user/user01/da450/raw_temperature.csv'
USING PigStorage(',') AS
(station:chararray, year:int, jan:float, feb:float, mar:float, apr: float, may: float, jun:float, jul:float, aug:float, sep:float, oct:float, nov:float, dec:float);

-- Output the contents of a relation
DUMP TEMPERATURE;

-- Display the schema of a relation
DESCRIBE TEMPERATURE;

-- Display the schema and a sample of data from a relation
ILLUSTRATE TEMPERATURE;

-- Example using "foreach" to generate new data sets
JAN = FOREACH TEMPERATURE GENERATE station, year, 'jan' AS month, jan AS celsius;
FEB = FOREACH TEMPERATURE GENERATE station, year, 'feb' AS month, feb AS celsius;
MAR = FOREACH TEMPERATURE GENERATE station, year, 'mar' AS month, mar AS celsius;

-- Example using union statement
TEMPERATURE2 = UNION JAN, FEB, MAR;

-- Use "limit" to include only first N rows
TEMPERATURE3 = LIMIT TEMPERATURE2 10;

-- Write a data set to HDFS
STORE TEMPERATURE2 INTO '/user/user01/da450/temperature' USING PigStorage(',');

-- Examine the output of the store command
fs -ls /user/user01/da450/temperature
cat /user/user01/da450/temperature/part-m-00000

-- Merge output files together into a single CSV file
fs -getmerge /user/user01/da450/temperature* ./temperature.csv

-- Reassign relation to new data set
TEMPERATURE = LOAD '/user/user01/temperature.csv' USING PigStorage(',') as (station:chararray, year:int, month:chararray, celsius:float);

-- Apply transform to create new column
TEMPERATURE2 = FOREACH TEMPERATURE GENERATE station, year, month, celsius, ROUND(celsius * 1.8 + 32) AS fahrenheit:float;

-- Filter relation based on criteria
SPTEMP = FILTER TEMPERATURE2 BY station == 'Clean_Air';

-- Exit the pig shell
quit
