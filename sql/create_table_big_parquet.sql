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