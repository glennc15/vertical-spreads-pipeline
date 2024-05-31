import datetime
import pytz

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from .spreads_etl_base import SpreadsEtlBase


class BuildCallSpreadsOperator(SpreadsEtlBase):

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