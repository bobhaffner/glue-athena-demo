
-- tab-delimited list of zip code centroids
CREATE external TABLE IF NOT EXISTS my_db.zip_points
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
STORED AS TEXTFILE LOCATION 's3://my-bucket/zip_points/'
TBLPROPERTIES ('skip.header.line.count'='1');

SELECT  geoid,
        intptlong,
        intptlat
FROM my_db.zip_points
LIMIT 5;


drop table glue_athena_demo.state_polys;

-- pipe-delimited WKT file of the 50 US states and their polygons
CREATE external TABLE IF NOT EXISTS my_db.state_polys
(
 state string,
 geom string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE LOCATION 's3://my-bucket/state_polys/';

SELECT  state,
        geom
FROM my_db.state_polys
LIMIT 5;

SELECT sp.state, zp.geoid
FROM glue_athena_demo.zip_points as zp
CROSS JOIN glue_athena_demo.state_polys as sp
WHERE ST_CONTAINS (ST_POLYGON(sp.geom), ST_POINT(zp.intptlong, zp.intptlat))
LIMIT 5;










SELECT  sp.state,
        COUNT(*) cnt
FROM my_db.zip_points as zp
CROSS JOIN my_db.state_polys as sp
WHERE ST_CONTAINS (ST_POLYGON(sp.geom), ST_POINT(zp.intptlong, zp.intptlat))
GROUP BY sp.state
LIMIT 5;

select * from glue_athena_demo.zip_points
-- Partition example
SELECT count(*)
FROM nyc_taxi_csv
WHERE pickup_datetime >= '2016-01-01 00:00:00'
        AND pickup_datetime <= '2016-01-31 23:59:59'


SELECT count(*)
FROM nyc_taxi_csv_partitioned
WHERE month = '1'


select count(*) as cnt from glue_athena_demo.nyc_taxi_parquet

-- spatial query
Select count(*)
FROM glue_athena_demo.nyc_taxi_parquet
WHERE ST_CONTAINS(ST_GEOMETRY_FROM_TEXT('POLYGON ((-73.95858764648438 40.80120581546625, -73.98399353027344 40.7670866323236, -73.97266387939453 40.76299115251035, -73.94811630249022 40.79756727106044, -73.95858764648438 40.80120581546625))'),
    ST_Point(pickup_longitude, pickup_latitude))

-- spatial query
Select count(*)
FROM glue_athena_demo.nyc_taxi_parquet
WHERE ST_CONTAINS(ST_POLYGON('POLYGON ((-73.95858764648438 40.80120581546625, -73.98399353027344 40.7670866323236, -73.97266387939453 40.76299115251035, -73.94811630249022 40.79756727106044, -73.95858764648438 40.80120581546625))'),
                  ST_Point(pickup_longitude, pickup_latitude))




SHOW CREATE TABLE glue_athena_demo.nyc_taxi_parquet;

