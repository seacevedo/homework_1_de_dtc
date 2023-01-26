--Query Problem 3
SELECT COUNT(*)
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-01-15 00:00:00' AND lpep_dropoff_datetime < '2019-01-16 00:00:00';

--Query Problem 4

SELECT lpep_pickup_datetime, trip_distance
FROM green_taxi_trips
ORDER BY trip_distance DESC
LIMIT 10;

--Query Problem 5

SELECT COUNT(CASE WHEN passenger_count = 2 THEN 1
                  ELSE NULL
             END) AS num_pass_2
       ,COUNT(CASE WHEN passenger_count = 3 THEN 1
                   ELSE NULL
              END) AS num_pass_3
    FROM green_taxi_trips_01
	WHERE lpep_pickup_datetime >= '2019-01-01 00:00:00' AND lpep_dropoff_datetime <= '2019-01-01 23:59:59';

--Query Problem 6


SELECT zone_lookup."Zone", green_taxi_trips.tip_amount
FROM green_taxi_trips
LEFT JOIN zone_lookup ON green_taxi_trips."DOLocationID"=zone_lookup."LocationID"
WHERE green_taxi_trips."PULocationID" = 7
ORDER BY green_taxi_trips.tip_amount DESC;
	
