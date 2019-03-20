-- Create table CSV

CREATE external TABLE IF NOT EXISTS glue_athena_demo.nyc_taxi_csv
(
 id string,
 vendor_id int,
 pickup_datetime timestamp,
 dropoff_datetime timestamp,
 passenger_count int,
 pickup_longitude double,
 pickup_latitude double,
 dropoff_longitude double,
 dropoff_latitude double,
 store_and_fwd_flag string,
 trip_duration int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE LOCATION 's3://ids-glue-athena-demo/csv/'
TBLPROPERTIES ('skip.header.line.count'='1');