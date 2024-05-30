import datetime
import pytz

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from .spreads_etl_base import SpreadsEtlBase


class BuildExpirationRecordsOperator(SpreadsEtlBase):

    def __init__(
        self,
        postgres_conn_id,
        mongo_conn_id,
        sql_path,
        **kwargs
    ):

        super().__init__(
            postgres_conn_id=postgres_conn_id,
            mongo_conn_id=mongo_conn_id,
            sql_path=sql_path,
            **kwargs
        )


    def execute(self, context):

        # initialize datebase connections:
        self._pg_hook = PostgresHook(
            postgres_conn_id=self._postgres_conn_id
        )

        self._mongo_hook = MongoHook(
            mongo_conn_id=self._mongo_conn_id
        )

        # get the most currect spot record from postgres:
        with open(self._sql_path, "r") as f:

            postgres_records = self.fetch_postgres_records(
                query=f.read(),
                expected_records=1,
                allow_zero=True
            )

        spot_id = postgres_records[0][0]
        spot_timestamp = postgres_records[0][1]

        filter = [
            {
                "$match": {
                    "timestamp": spot_timestamp
                }
            },
            {
                "$group": {
                    "_id": "$expiration"
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "expiration": "$_id",
                }
            },
            {
                "$sort": {
                    "expiration": 1
                }
            }
        ]

        mongo_client = self._mongo_hook.get_conn()
        query_results = mongo_client["OptionData"]["SPY_Options"].aggregate(filter)
        expirations = list(query_results)


        # build expiration records
        '''

        have a lot going on with the expiraiton. The expiration in the us/central
        timezone. However, all expiration/timestamps in mongo are in utc time.
        And all expirations are set to expirre at midnight.

        Need to change the expiraiton time to 16:00

        So the expiration is
        1) set to utc
        2) converted to cental
        3) hour/minutes set to 16:00
        4) converted back to utc

        converting back to utc because the spot timestamp is in utc from postgres.
        So both timezones have to match so time_to_expiration is correct.

        '''
        utc_tz = pytz.timezone("UTC")
        central_tz = pytz.timezone("US/Central")
        # today = utc_tz.localize(datetime.datetime.now())
        # today = datetime.datetime.utcnow()
        today = datetime.datetime.now(datetime.UTC)


        for record in expirations:
            record['expiration'] = utc_tz.localize(record['expiration']).astimezone(central_tz).replace(hour=16, minute=0).astimezone(utc_tz)
            record['time_to_expiration'] = (record['expiration']-spot_timestamp).total_seconds()
            record['past_expiration'] = today.date() < record['expiration'].date()

        # build expiration tuples and add the new expiration records to postgres:

        # for reference:
        # CREATE TABLE IF NOT EXISTS expirations (
        #     id uuid PRIMARY KEY,
        #     expiration timestamp WITH TIME ZONE NOT NULL,
        #     time_to_expiration integer,
        #     past_expiration boolean DEFAULT false,
        #     spot_id uuid,
        #     FOREIGN KEY (spot_id) REFERENCES spots(id)
        # );

        expiration_tuples = [(self.get_uuid7(), x.get('expiration').isoformat(), x.get('time_to_expiration'), x.get('past_expiration'), spot_id) for x in expirations]

        sql_str = "INSERT INTO expirations (id, expiration, time_to_expiration, past_expiration, spot_id) VALUES "
        for expiration_tuple in expiration_tuples:
            sql_str += f"{expiration_tuple}, "
        sql_str = sql_str[0:-2] + ";"


        self.write_postgres_records(
            query=sql_str
        )












