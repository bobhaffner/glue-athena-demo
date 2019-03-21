CREATE EXTERNAL TABLE glue_athena_demo.nyc_taxi_parquet_partitioned(
  trip_duration int, 
  pickup_latitude double, 
  pickup_longitude double, 
  dropoff_datetime timestamp, 
  dropoff_longitude double, 
  vendor_id int, 
  dropoff_latitude double, 
  pickup_datetime timestamp, 
  store_and_fwd_flag string, 
  id string, 
  passenger_count int)
PARTITIONED BY ( 
  month string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://ids-glue-athena-demo/nyc_taxi_parquet_partitioned/'
  
-- Loading the partitions into the table
MSCK REPAIR TABLE glue_athena_demo.nyc_taxi_parquet_partitioned