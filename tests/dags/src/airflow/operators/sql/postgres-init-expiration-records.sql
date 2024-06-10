DROP TABLE IF EXISTS verticals;
DROP TABLE IF EXISTS expirations;
DROP TABLE IF EXISTS spots;
DROP TABLE IF EXISTS bull_calls;
DROP TABLE IF EXISTS bull_puts;
DROP TABLE IF EXISTS bear_calls;
DROP TABLE IF EXISTS bear_puts;
DROP TABLE IF EXISTS spy_ohlc;


CREATE TABLE IF NOT EXISTS spots (
    id uuid PRIMARY KEY,
    spot_timestamp timestamp WITH TIME ZONE NOT NULL,
    spot numeric NOT NULL
);


-- create expirations table
CREATE TABLE IF NOT EXISTS expirations (
    id uuid PRIMARY KEY,
    expiration timestamp WITH TIME ZONE NOT NULL,
    time_to_expiration integer,
    past_expiration boolean DEFAULT false,
    spot_id uuid,
    FOREIGN KEY (spot_id) REFERENCES spots(id)
);

CREATE TABLE IF NOT EXISTS bull_calls (
    id uuid PRIMARY KEY,
    index integer,
    short_description char(20),
    long_description char(20),
    expiration timestamp WITH TIME ZONE,
    spot_timestamp timestamp WITH TIME ZONE,
    spot decimal,
    short_strike decimal,
    long_strike decimal,
    strike_delta decimal,
    max_profit decimal,
    risk decimal,
    break_even decimal,
    delta decimal,
    long_iv decimal,
    short_iv decimal,
    expiration_close decimal,
    profit decimal,
    time_to_expiration integer,
    past_expiration boolean DEFAULT false
);

CREATE TABLE IF NOT EXISTS bear_calls (
    id uuid PRIMARY KEY,
    short_description char(20),
    long_description char(20),
    expiration timestamp WITH TIME ZONE,
    spot_timestamp timestamp WITH TIME ZONE,
    spot decimal,
    short_strike decimal,
    long_strike decimal,
    strike_delta decimal,
    max_profit decimal,
    risk decimal,
    break_even decimal,
    delta decimal,
    long_iv decimal,
    short_iv decimal,
    expiration_close decimal,
    profit decimal,
    time_to_expiration integer,
    past_expiration boolean DEFAULT false
);

CREATE TABLE IF NOT EXISTS bull_puts (
    id uuid PRIMARY KEY,
    short_description char(20),
    long_description char(20),
    expiration timestamp WITH TIME ZONE,
    spot_timestamp timestamp WITH TIME ZONE,
    spot decimal,
    short_strike decimal,
    long_strike decimal,
    strike_delta decimal,
    max_profit decimal,
    risk decimal,
    break_even decimal,
    delta decimal,
    long_iv decimal,
    short_iv decimal,
    expiration_close decimal,
    profit decimal,
    time_to_expiration integer,
    past_expiration boolean DEFAULT false
);

CREATE TABLE IF NOT EXISTS bear_puts (
    id uuid PRIMARY KEY,
    short_description char(20),
    long_description char(20),
    expiration timestamp WITH TIME ZONE,
    spot_timestamp timestamp WITH TIME ZONE,
    spot decimal,
    short_strike decimal,
    long_strike decimal,
    strike_delta decimal,
    risk decimal,
    max_profit decimal,
    break_even decimal,
    delta decimal,
    long_iv decimal,
    short_iv decimal,
    expiration_close decimal,
    profit decimal,
    time_to_expiration integer,
    past_expiration boolean DEFAULT false
);


CREATE TABLE IF NOT EXISTS spy_ohlc (
    ohlc_date date,
    open_spot decimal,
    high_spot decimal,
    low_spot decimal,
    close_spot decimal,
    adj_close decimal,
    volume integer,
    id serial PRIMARY KEY
);

-- load the spy_ohlc data:
COPY spy_ohlc(ohlc_date, open_spot, high_spot, low_spot, close_spot, adj_close, volume)
FROM '/tmp/SPY.csv'
DELIMITER ','
CSV HEADER;


-- create one spot record:
INSERT INTO spots (id, spot_timestamp, spot) VALUES
(UUID('06659cd9-d33c-7654-8000-64b5a28c6d31'), '2020-09-10T15:45:00', 340.77), (UUID('0665b529-e421-71fd-8000-844710e77cd2'), '2020-12-04T16:50:00', 369.07);


-- create expiration records:
-- INSERT INTO expirations (id, expiration, time_to_expiration, past_expiration, spot_id) VALUES (UUID('06659cc9-0823-738b-8000-08758714f231'), '2020-09-11T21:00:00+00:00', 105300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7454-8000-00cb64bfbf13'), '2020-09-14T21:00:00+00:00', 364500.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-74b9-8000-bfdbf0b2e04b'), '2020-09-16T21:00:00+00:00', 537300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-74eb-8000-3335a78565c2'), '2020-09-18T21:00:00+00:00', 710100.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-751d-8000-b4de6db1946b'), '2020-09-21T21:00:00+00:00', 969300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-753f-8000-fa5807846f08'), '2020-09-23T21:00:00+00:00', 1142100.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7571-8000-ef577614ac72'), '2020-09-25T21:00:00+00:00', 1314900.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-75d6-8000-6e20b2d9d911'), '2020-09-28T21:00:00+00:00', 1574100.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7608-8000-e27f7156469b'), '2020-09-30T21:00:00+00:00', 1746900.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-762a-8000-9c36407704d5'), '2020-10-02T21:00:00+00:00', 1919700.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-765c-8000-f16e5238476d'), '2020-10-05T21:00:00+00:00', 2178900.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-767e-8000-b9d80ec15d47'), '2020-10-07T21:00:00+00:00', 2351700.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-76b0-8000-7a0d87e12727'), '2020-10-09T21:00:00+00:00', 2524500.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7704-8000-234c5f20c495'), '2020-10-12T21:00:00+00:00', 2783700.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7736-8000-b1887f29e18b'), '2020-10-14T21:00:00+00:00', 2956500.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7758-8000-9d08277c1d46'), '2020-10-16T21:00:00+00:00', 3129300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-778a-8000-a4d0c1f8e759'), '2020-10-23T21:00:00+00:00', 3734100.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-77ac-8000-5d2cdbd4a7dc'), '2020-10-30T21:00:00+00:00', 4338900.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7832-8000-21580c2348c9'), '2020-11-20T22:00:00+00:00', 6156900.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7854-8000-81da5234731e'), '2020-12-18T22:00:00+00:00', 8576100.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-78a7-8000-c345db1519b4'), '2020-12-31T22:00:00+00:00', 9699300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-78c9-8000-035ec9f2fda6'), '2021-01-15T22:00:00+00:00', 10995300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-78fb-8000-3da587e5fcc4'), '2021-02-19T22:00:00+00:00', 14019300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-791d-8000-ee6b9b201c0f'), '2021-03-19T21:00:00+00:00', 16434900.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-794f-8000-f362933dd01c'), '2021-03-31T21:00:00+00:00', 17471700.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7982-8000-067f1841f638'), '2021-06-18T21:00:00+00:00', 24297300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-79b4-8000-b7ba879bcaed'), '2021-06-30T21:00:00+00:00', 25334100.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-79d5-8000-cb300ca58305'), '2021-09-17T21:00:00+00:00', 32159700.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-79f7-8000-6a7c46917864'), '2021-12-17T22:00:00+00:00', 40025700.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7a29-8000-41a949d625a7'), '2022-01-21T22:00:00+00:00', 43049700.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7a7d-8000-95c1478823f1'), '2022-03-18T21:00:00+00:00', 47884500.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7aaf-8000-199a6a5600f6'), '2022-06-17T21:00:00+00:00', 55746900.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7ae2-8000-5aebc4195f47'), '2022-09-16T21:00:00+00:00', 63609300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31'), (UUID('06659cc9-0823-7b25-8000-45050335474e'), '2022-12-16T22:00:00+00:00', 71475300.0, True, '06659cd9-d33c-7654-8000-64b5a28c6d31');


