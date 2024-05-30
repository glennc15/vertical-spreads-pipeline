from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from .spreads_etl_base import SpreadsEtlBase


class BuildSpotRecordOperator(SpreadsEtlBase):

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
        '''
        1) get the latest spot record from postgres.
        2) get the next spot record from mongo
        3) write the new spot record to an sql file

        if no spot records exists in postgres:
            a) get the first spot record from mongo
            b) write the new spot record an sql file

        '''

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

        # Build the mongo query. If there are no postgres spot records
        # then get the first spot record in mongo. If a postgres spot record
        # exists then get the next spot record from mongo.
        if len(postgres_records) == 0:
            mongo_query = {
                "timestamp": {"$ne": None}
            }

        else:
            mongo_query = {
                "timestamp": {"$gt": postgres_records[0][0]}
            }

        mongo_records = self.get_mongo_records(
            db="OptionData",
            collection="SPY_Spots",
            query=mongo_query,
            sort=[("timestamp", 1)],
            limit=1,
            expected_records=1
        )

        # build sql insert statement and add the new spot record to postgres
        mongo_record = mongo_records[0]
        spot_tuple = (
            self.get_uuid7(),
            mongo_record['timestamp'].isoformat(),
            mongo_record['spot']
        )

        sql_str = f"INSERT INTO spots (id, spot_timestamp, spot) VALUES {spot_tuple};"

        self.write_postgres_records(
            query=sql_str
        )


    # def add_timezone(self, mongo_records, base_tz, final_tz):
    #     for record in mongo_records:
    #         record['timestamp'] = base_tz.localize(record['timestamp']).astimezone(final_tz)

    #     return mongo_records
