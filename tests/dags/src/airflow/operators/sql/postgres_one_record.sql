DROP TABLE IF EXISTS options;
DROP TABLE IF EXISTS expirations;
DROP TABLE IF EXISTS spots;


CREATE TABLE IF NOT EXISTS spots (
    id uuid PRIMARY KEY,
    spot_timestamp timestamp WITH TIME ZONE NOT NULL,
    spot numeric NOT NULL
);


-- create expirations table
CREATE TABLE IF NOT EXISTS expirations (
    id uuid PRIMARY KEY,
    spot_timestamp timestamp WITH TIME ZONE NOT NULL,
    expiration timestamp WITH TIME ZONE NOT NULL,
    time_to_expiration integer,
    past_expiration boolean DEFAULT false,
    spot_id uuid,
    FOREIGN KEY (spot_id) REFERENCES spots(id)
);

-- create one record:
INSERT INTO spots (id, spot_timestamp, spot)
VALUES ('0f1a0034-d2bb-448f-8447-0d77502cd176', '2020-09-10T15:45:00.000+00:00', 340.77);
