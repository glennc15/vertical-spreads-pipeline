from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator



class BuildSpotRecordOperator(BaseOperator):
    # template_fields = ("_start_date", "_end_date", "_insert_query")

    def __init__(
        self,
        postgres_conn_id,
        mongo_conn_id,
        sql_path,
        **kwargs
    ):

        super().__init__(**kwargs)
        self._postgres_conn_id = postgres_conn_id
        self._mongo_conn_id = mongo_conn_id
        self._sql_path = sql_path