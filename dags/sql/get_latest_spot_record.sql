-- get the latest timestamp from the spots table
SELECT *
FROM spots
ORDER BY spot_timestamp DESC
LIMIT 1;