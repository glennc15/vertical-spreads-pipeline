-- get the latest timestamp from the spots table
SELECT spot_timestamp as timestamp
FROM spots
ORDER BY spot_timestamp DESC
LIMIT 1;