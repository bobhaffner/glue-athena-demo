
-- Drop table glue_athena_demo.nyc_taxi_big_parquet;

CREATE external TABLE IF NOT EXISTS glue_athena_demo.nyc_taxi_big_parquet
(
  VendorID int,
  tpep_pickup_datetime string,
  tpep_dropoff_datetime string,
  passenger_count int,
  trip_distance double,
  RatecodeID int,
  store_and_fwd_flag string,
  PULocationID int,
  DOLocationID int,
  payment_type int,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double
)
PARTITIONED BY (month int)
STORED AS PARQUET
LOCATION 's3://ids-glue-athena-demo/nyc_taxi_big_parquet/'
tblproperties ("parquet.compress"="GZIP");

MSCK REPAIR TABLE glue_athena_demo.nyc_taxi_big_parquet;


CREATE external TABLE IF NOT EXISTS glue_athena_demo.nyc_taxi_big_csv
(
  VendorID int,
  tpep_pickup_datetime string,
  tpep_dropoff_datetime string,
  passenger_count int,
  trip_distance double,
  RatecodeID int,
  store_and_fwd_flag string,
  PULocationID int,
  DOLocationID int,
  payment_type int,
  fare_amount double,
  extra double,
  mta_tax double,
  tip_amount double,
  tolls_amount double,
  improvement_surcharge double,
  total_amount double
)
PARTITIONED BY (month int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 's3://ids-glue-athena-demo/nyc_taxi_big_csv/'
TBLPROPERTIES ('skip.header.line.count'='1');


MSCK REPAIR TABLE glue_athena_demo.nyc_taxi_big_csv;



select * from glue_athena_demo.nyc_taxi_big_csv limit 10;

Select max(passenger_count) as max_passengers
from glue_athena_demo.nyc_taxi_big_parquet;



DESCRIBE glue_athena_demo.nyc_taxi_parquet_partitioned;
DESCRIBE glue_athena_demo.big_parquet



















-- CREATE DATABASE us_zips

DROP TABLE us_zips.zip_points

CREATE external TABLE IF NOT EXISTS us_zips.zip_points
(
 GEOID string,
 ALAND int,
 AWATER int,
 ALAND_SQMI double,
 AWATER_SQMI double,
 INTPTLAT double,
 INTPTLONG double
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE LOCATION 's3://athena-spatial/zip_points/'
TBLPROPERTIES ('skip.header.line.count'='1');


DROP TABLE us_zips.big_square

CREATE external TABLE IF NOT EXISTS us_zips.big_square
  (
  wkt string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://athena-spatial/zip_polys/';



DROP TABLE us_zips.zip_polys;


CREATE external TABLE IF NOT EXISTS us_zips.zip_polys
  (
  wkt VARCHAR(65535),
  geo_id string,
  state bigint,
  name string,
  lsad string,
  censusarea double
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://athena-spatial/zip_polys/'
TBLPROPERTIES ('skip.header.line.count'='1');



SELECT count(GEOID)
FROM us_zips.zip_points
WHERE GEOID LIKE '681%'


SELECT COUNT(*) as cnt
FROM us_zips.zip_points





CREATE external TABLE IF NOT EXISTS us_zips.zip_polys
 (
 GEO_ID string,
 STATE int,
 NAME string,
 LSAD string,
 CENSUSAREA double,
 geometry binary
 )
ROW FORMAT SERDE 'com.esri.hadoop.hive.serde.JsonSerde'
WITH SERDEPROPERTIES ( 'ignore.malformed.json' = 'true')
STORED AS INPUTFORMAT 'com.esri.json.hadoop.EnclosedJsonInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://athena-spatial/zip_polys/';



SELECT COUNT(*) as cnt
FROM us_zips.zip_polys;


SELECT COUNT(*) cnt
FROM us_zips.zip_points as z
WHERE ST_CONTAINS (ST_GEOMETRY_FROM_TEXT('POLYGON ((-104.58984375 39.198205348894795, -94.306640625 39.198205348894795, -94.306640625 42.8115217450979, -104.58984375 42.8115217450979, -104.58984375 39.198205348894795))'),
ST_POINT(z.intptlong, z.INTPTLAT))



Select ST_GEOMETRY_FROM_TEXT('POLYGON ((-104.58984375 39.198205348894795, -94.306640625 39.198205348894795, -94.306640625 42.8115217450979, -104.58984375 42.8115217450979, -104.58984375 39.198205348894795))')

SELECT ST_GEOMETRY_FROM_TEXT(wkt)
FROM us_zips.big_square
LIMIT 1;

Select ST_GEOMETRY_FROM_TEXT(wkt) from us_zips.big_square

SELECT ST_POINT(intptlong, INTPTLAT)
FROM us_zips.zip_points
LIMIT 1;

SELECT ST_GeomFromText('POINT(-71.064544 42.28787)');



