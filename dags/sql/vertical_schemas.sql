
-- create spots table
CREATE TABLE IF NOT EXISTS spots (
    id uuid PRIMARY KEY,
    spot_timestamp timestamp WITH TIME ZONE NOT NULL,
    spot numeric NOT NULL
);


-- create expirations table
CREATE TABLE IF NOT EXISTS expirations (
    id uuid PRIMARY KEY,
    spot_id integer REFERENCES spots,
    spot_timestamp timestamp WITH TIME ZONE NOT NULL,
    expiration timestamp WITH TIME ZONE NOT NULL,
    time_to_expiration integer,
    past_expiration boolean DEFAULT false
);