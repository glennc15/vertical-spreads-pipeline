SELECT *
FROM spots
INNER JOIN expirations
ON expirations.spot_id = (SELECT id FROM spots ORDER BY spot_timestamp DESC LIMIT 1)
ORDER BY expirations.expiration ASC;