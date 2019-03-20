-- Simple Selects

SELECT count(*) as cnt
FROM nyc_taxi_csv;

SELECT *
FROM nyc_taxi_csv LIMIT 10; 

SELECT vendor_id, AVG(trip_duration) / 60 as avg_duration_mins
FROM nyc_taxi_csv
WHERE MONTH(pickup_datetime) = 1
GROUP BY vendor_id 