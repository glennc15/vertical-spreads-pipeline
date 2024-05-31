from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

from uuid_extensions import uuid7

class SpreadsEtlBase(BaseOperator):
    '''

    Base class for building vertical spreads. Provides database hooks
    for both Mongo and Postgres that the other operators can use.

    The idea is that the other operators should only have an execute()
    and this class hold all common helper methods.

    '''

    def __init__(
        self,
        postgres_conn_id,
        mongo_conn_id,
        sql_path,
        **kwargs
    ):
        '''
        postgres_conn_id: airflow postgres database connection id
        mongo_conn_id: airflow mongo database connection id
        sql_path: path to an sql file to use for the initial postgres query

        '''

        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_conn_id
        self._mongo_conn_id = mongo_conn_id
        self._sql_path = sql_path

        self._pg_hook = None
        self._mongo_hook = None


    def execute(self, context):
        '''

        each class needs to build the mongo and postgres hooks at the start of the
        execute() method. I cannot get build the hooks in __init__() for some reason.


        '''
        # # initialize datebase connections:
        # self._pg_hook = PostgresHook(
        #     postgres_conn_id=self._postgres_conn_id
        # )

        # self._mongo_hook = MongoHook(
        #     mongo_conn_id=self._mongo_conn_id
        # )

        pass


    def get_mongo_records(self, db, collection, query, sort, limit, expected_records=None, use_find=True, use_aggregate=False):
        '''

        Run a query against the mongo database:

        db: mongo database name

        collection: mongo collection name

        query: mongo query

        sort: query sort order

        limit: query limit

        expected_records: expected records from the query. If the number of query records
            doesn't match then rasie a value error. Can be ignored by setting to None.

        '''
        mongo_client = self._mongo_hook.get_conn()
        query_results = mongo_client[db][collection].find(
            filter=query,
            sort=sort,
            limit=limit
        )

        mongo_records = list(query_results)

        if expected_records and len(mongo_records) != expected_records:
            err_msg = f"expected query results to = {expected_records}"
            err_msg += f"instead query: {query} returned {len(mongo_records)} records."
            raise ValueError(err_msg)

        return mongo_records

    def aggregate_mongo_records(self, db, collection, query):
        '''


        '''
        mongo_client = self._mongo_hook.get_conn()
        query_results = mongo_client[db][collection].aggregate(query)


        return list(query_results)


    def fetch_postgres_records(self, query, expected_records=None, allow_zero=False):
        '''

        Run a query against the postgres database:

        query: an sql query string

        expected_records: expected records from the query. If the number of query records
            doesn't match then rasie a value error. Can be ignored by setting to None.

        allow_zero: allows zero records to be returns and overrides expected_records.

        '''

        conn = self._pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        query_results = cursor.fetchall()

        if expected_records and len(query_results) != expected_records:
            if allow_zero:
                if len(query_results) != 0:
                    err_msg = f"expected query results to equal {expected_records} or zero."
                    err_msg += f"instead query: {query} returned {len(query_results)} records."
                    raise ValueError(err_msg)

            else:
                err_msg = f"expected query results to equal {expected_records}"
                err_msg += f"instead query: {query} returned {len(query_results)} records."
                raise ValueError(err_msg)

        return query_results


    def write_postgres_records(self, query):
        '''
        Writes records to the postgres database:

        query: an sql insert string.

        '''

        pg_results = self._pg_hook.run(query)

        # TODO: add some error handling


    def get_uuid7(self):
        """

        helper class for postgres id fields. Returns a uuid7

        """

        return uuid7()


