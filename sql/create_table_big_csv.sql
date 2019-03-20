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


