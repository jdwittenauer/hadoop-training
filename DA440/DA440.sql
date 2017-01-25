-- Launch hive shell from ssh
-- $ hive

-- List available functions
SHOW FUNCTIONS;

-- Create a new database
CREATE DATABASE user01;

-- List available databases
SHOW DATABASES;

-- Create a new table in the user01 database
CREATE TABLE user01.location (
    station STRING COMMENT 'Station Name',
    latitude INT COMMENT 'Degrees South',
    longitude INT COMMENT 'Degrees West'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Create a new partitioned table in the user01 database
CREATE TABLE user01.windspeed (
    year INT COMMENT 'Year',
    month STRING COMMENT 'Month',
    knots FLOAT COMMENT 'wind speed'
)
PARTITIONED BY (
    station STRING COMMENT 'Station Name'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- Create a new external table in the user01 database
CREATE EXTERNAL TABLE user01.temperature (
    station STRING COMMENT 'Station Name',
    year INT COMMENT 'Year',
    month STRING COMMENT 'Month',
    celsius FLOAT COMMENT 'degrees celsius'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '/user/user01/da440/temperature';

-- List available tables in user01
SHOW TABLES IN user01;

-- Show schema information for a table
DESCRIBE user01.location;

-- Drop a table
DROP TABLE user01.location;

-- Alter a column in a table
ALTER TABLE user01.location CHANGE COLUMN long longitude INT;

-- Simple select statement
SELECT * FROM user01.temperature LIMIT 10;

-- Load data from a file directly into an existing table
LOAD DATA LOCAL INPATH '/user/user01/da440/location.csv'
INTO TABLE user01.location;

-- Load data from several files directly into an existing table
LOAD DATA LOCAL INPATH '/user/user01/da440/wind_Adelaide.csv'
INTO TABLE user01.windspeed PARTITION(station = 'Adelaide');
LOAD DATA LOCAL INPATH '/user/user01/da440/wind_Faraday.csv'
INTO TABLE user01.windspeed PARTITION(station = 'Faraday');
LOAD DATA LOCAL INPATH '/user/user01/da440/wind_Halley.csv'
INTO TABLE user01.windspeed PARTITION(station = 'Halley');

-- Set the current database (avoids dot notation)
USE user01;

-- Simple select examples
SELECT * FROM temperature LIMIT 20;
SELECT * FROM temperature WHERE month = 'jan' AND year = 1970;
SELECT count(*) FROM temperature WHERE celsius IS NOT NULL;
SELECT avg(celsius) FROM temperature WHERE year=1970;

-- Join statement example
SELECT w.station, w.year, w.month, w.knots, l.latitude, l.longitude
FROM windspeed w
JOIN location l ON w.station = l.station
WHERE w.year > 2012 AND w.knots > 15 AND w.knots IS NOT NULL;

-- Create table via join
CREATE TABLE weather AS
SELECT t.station, t.year, t.month, t.celsius, round(t.celsius * 1.8 + 32, 2) AS fahrenheit, w.knots, round(w.knots * 1.852, 2) AS kph, l.latitude, l.longitude
FROM windspeed w
JOIN location l ON w.station = l.station
JOIN temperature t ON w.station = t.station AND w.month = t.month AND w.year = t.year;

-- Leave hive shell
QUIT;

-- Dump query results to file from ssh
-- $ hive -e 'SELECT * FROM user01.weather' > /user/user01/weather.csv
