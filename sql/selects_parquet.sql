SELECT count(*) as cnt
FROM nyc_taxi_parquet;



SELECT max(passenger_count) as max_passenger_count
FROM nyc_taxi_parquet




SELECT vendor_id, AVG(trip_duration) / 60 as avg_duration_mins
FROM nyc_taxi_parquet
WHERE MONTH(pickup_datetime) = 1
GROUP BY vendor_id 