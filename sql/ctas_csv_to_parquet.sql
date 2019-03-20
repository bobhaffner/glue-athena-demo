-- Create Table as Select
CREATE TABLE nyc_taxi_parquet
WITH (
      external_location = 's3://ids-glue-athena-demo/nyc_taxi_parquet/',
      format = 'parquet')
AS SELECT * 
FROM nyc_taxi_csv;