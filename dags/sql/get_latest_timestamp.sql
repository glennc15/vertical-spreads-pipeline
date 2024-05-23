-- get the latest timestamp from the spots table
SELECT spot_timestamp as timestamp
FROM spots
ORDER BY timestamp DESC
LIMIT 1;