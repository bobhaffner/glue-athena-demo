SELECT vendor_id, AVG(trip_duration) / 60 as avg_duration_mins
FROM nyc_taxi_parquet_partitioned
WHERE month  = '01'
GROUP BY vendor_id 