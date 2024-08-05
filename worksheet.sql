// weather stage -> s3://snowflake-workshop-lab/weather-nyc
// trips stage -> s3://snowflake-workshop-lab/citibike-trips-csv/trips_2013_1_6_0.csv.gz

create or replace TABLE CITIBIKE.PUBLIC.TRIPS (
	TRIPDURATION NUMBER(38,0),
	STARTTIME TIMESTAMP_NTZ(9),
	ENDTIME TIMESTAMP_NTZ(9),
	START_STATION_ID NUMBER(38,0),
	START_STATION_NAME VARCHAR(16777216),
	START_STATION_LATITUDE FLOAT,
	START_STATION_LONGITUDE FLOAT,
	END_STATION_ID NUMBER(38,0),
	END_STATION_NAME VARCHAR(16777216),
	END_STATION_LATITUDE FLOAT,
	END_STATION_LONGITUDE FLOAT,
	BIKEID NUMBER(38,0),
	MEMBERSHIP_TYPE VARCHAR(16777216),
	USERTYPE VARCHAR(16777216),
	BIRTH_YEAR NUMBER(38,0),
	GENDER NUMBER(38,0)
);

truncate table trips

// check the contents 
list @citibike_trips;


// create the csv format for parsing records from s3
CREATE OR REPLACE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = ','
  RECORD_DELIMITER = '\n'
  SKIP_HEADER = 0
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL')  -- Treat empty strings and the string 'NULL' as NULL values
  TRIM_SPACE = FALSE
  ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  ESCAPE = NONE
  ESCAPE_UNENCLOSED_FIELD = '\\'
  DATE_FORMAT = 'AUTO'
  TIMESTAMP_FORMAT = 'AUTO';	

// load the data
copy into trips from @citibike_trips
file_format = my_csv_format;

select * from trips limit 20;


select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration(mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance(km)"
from trips
group by 1 order by 1


// which month is busiest
select dayofweek(starttime) as "dayofweek",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

// create a clone
create table trips_dev clone trips; 


// create database
create database weather; 
use database weather;
use schema public;

// create a new table;
create table json_weather_data (v variant);

// create a new stage
create stage nyc_weather 
url = 's3://snowflake-workshop-lab/weather-nyc'

list @nyc_weather;

copy into json_weather_data 
from @nyc_weather
file_format = (type=json);


select * from json_weather_data limit 5; 

create view json_weather_data_view as 
select 
v:time::timestamp as observation_time, 
v:city.id::int as city_id,
v:city.name::string as city_name, 
v:city.country::string as country,
v:city.coord.lat::float as city_lat,
v:city.coord.lon::float as city_lon,
v:clouds.all::int as clouds,
(v:main.temp::float)-273.15 as temp_avg,
(v:main.temp_min::float)-273.15 as temp_min,
(v:main.temp_max::float)-273.15 as temp_max,
v:weather[0].main::string as weather,
v:weather[0].description::string as weather_desc,
v:weather[0].icon::string as weather_icon,
v:wind.deg::float as wind_dir, 
v:wind.speed::float as wind_speed,
from json_weather_data
where city_id = 5128638

select * from json_weather_data_view 
where date_trunc('month', observation_time) = '2018-01-01'
limit 20;


select weather as conditions, count(*) as num_trips
from citibike.public.trips left outer join json_weather_data_view
-- on date_trunc('day', observation_time) = date_trunc('day', starttime)
where conditions is not null
group by 1 order by 2


drop table json_weather_data
select * from json_weather_data

undrop table json_weather_data

use database citibike;
use schema public;

update trips set start_station_name = 'oops';

select start_station_name as "station", count(*) as "rides" 
from trips
group by 1 order by 2 desc


set query_id = 
(select query_id from table(information_schema.query_history_by_session (result_limit => 100))
where query_text like 'update%' order by start_time limit 1);

create or replace table trips as (
    select * from trips before (statement => $query_id)
);
