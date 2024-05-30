import os
import pytz

import uuid

from airflow.models import BaseOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from .spreads_etl_base import SpreadsEtlBase




class BuildSpotRecordOperator(SpreadsEtlBase):
    # template_fields = ("_start_date", "_end_date", "_insert_query")

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

        # self._postgres_conn_id = postgres_conn_id
        # self._mongo_conn_id = mongo_conn_id
        # self._sql_path = sql_path

        # self._pg_hook = None
        # self._mongo_hook = None

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

        # postgres_records = self.get_postgres_spot_record()

        # get the most currect spot record from postgres:
        with open(self._sql_path, "r") as f:
            # sql_str = f.read()

            postgres_records = self.fetch_postgres_records(
                query=f.read(),
                expected_records=1,
                allow_zero=True
            )

        # Build the mongo query. If there are no postgres spot records
        # then get the first spot record in mongo. If a postgres spot record
        # then get the next spot record from mongo.
        if len(postgres_records) == 0:
            # no records exist in postgres so get the first spot record in mongo
            mongo_query = {
                "timestamp": {"$ne": None}
            }

        else:
            # get the next sport record in mongo:
            mongo_query = {
                "timestamp": {"$gt": postgres_records[0][0]}
            }

        # print(f"mongo_query = {mongo_query}")

        mongo_records = self.get_mongo_records(
            db="OptionData",
            collection="SPY_Spots",
            query=mongo_query,
            sort=[("timestamp", 1)],
            limit=1,
            expected_records=1
        )


        # mongo_records = self.get_mongo_spot_record(query=mongo_query)

        print(mongo_records)


        # mongo_records = self.add_timezone(
        #     mongo_records=mongo_records,
        #     base_tz=pytz.timezone("UTC"),
        #     final_tz=pytz.timezone('US/Central')
        # )

        # self.write_postgres_spot_record(
        #     mongo_record=mongo_records[0]
        # )

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


    def add_timezone(self, mongo_records, base_tz, final_tz):
        for record in mongo_records:
            record['timestamp'] = base_tz.localize(record['timestamp']).astimezone(final_tz)

        return mongo_records

    def get_mongo_spot_record(self, query):
        '''


        '''

        mongo_client = self._mongo_hook.get_conn()

        query_results = mongo_client["OptionData"]["SPY_Spots"].find(
            query,
            sort=[("timestamp", 1)],
            limit=1
        )

        mongo_records = list(query_results)
        if len(mongo_records) != 1:
            raise ValueError("Problem with the query to spot records")


        return mongo_records


    def get_postgres_spot_record(self):
        '''

        gets the latest timestamp record from postgres

        '''


        with open(self._sql_path, "r") as f:
            sql_str = f.read()

        conn = self._pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql_str)
        records = cursor.fetchall()

        return records

    def write_postgres_spot_record(self, mongo_record):
        '''

        creates a spot record in postgres

        '''

        postgres_spot_record = {
            "id": uuid.uuid4(),
            "spot_timestamp": mongo_record['timestamp'],
            "spot": mongo_record['spot']
        }

        spot_tuple = (
            uuid.uuid4(),
            mongo_record['timestamp'].isoformat(),
            mongo_record['spot']
        )

        sql_str = f"INSERT INTO spots (id, spot_timestamp, spot) VALUES {spot_tuple};"

        pg_results = self._pg_hook.run(sql_str)
