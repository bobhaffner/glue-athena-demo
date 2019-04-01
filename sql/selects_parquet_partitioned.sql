SELECT max(passenger_count) as max_passenger_count
FROM nyc_taxi_parquet_partitioned
WHERE month = '01'


SELECT max(passenger_count) as max_passenger_count
FROM nyc_taxi_parquet
WHERE MONTH(pickup_datetime) = 1


SELECT max(passenger_count) as max_passenger_count
FROM nyc_taxi_csv
WHERE MONTH(pickup_datetime) = 1



SELECT vendor_id, AVG(trip_duration) / 60 as avg_duration_mins
FROM nyc_taxi_parquet_partitioned
WHERE month  = '01'
GROUP BY vendor_id 