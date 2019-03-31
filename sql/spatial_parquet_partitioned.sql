Select *
FROM glue_athena_demo.nyc_taxi_parquet_partitioned
WHERE ST_CONTAINS(
                  ST_POLYGON(''),
                  ST_POINT(pickup_longitude, pickup_latitude)
                 )
AND month = '01'