-- create expiration table
CREATE TABLE IF NOT EXISTS Expiration (
    Id SERIAL PRIMARY KEY,
    SpotTimestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    Expiration TIMESTAMP WITH TIME ZONE NOT NULL,
    Spot NUMERIC NOT NULL,
    TimeToExpiration INTEGER,
    PastExpiration BOOLEAN DEFAULT FALSE
);