from typing import Any
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os

class ReadPostgresOperator(BaseOperator):
    """

    Operator that fetches data from the PostgreSQL database.

    conn_id : str
    sql : str
        path to the sql file that defines the select statement

    """

    @apply_defaults
    def __init__(
        self,
        conn_id,
        sql,
        **kwargs,
    ):
        super(ReadPostgresOperator, self).__init__(**kwargs)

        self._conn_id = conn_id
        self._sql = sql


    def execute(self, context):

        # load the query from the .sql file:
        sql_files_path = os.path.join("/opt/airflow/dags", self._sql)

        with open(sql_files_path, "r") as f:
            query_str = f.read()

        pg_hook = PostgresHook(
            postgres_conn_id=self._conn_id
        )
        pg_conn = pg_hook.get_conn()
        pg_cursor = pg_conn.cursor()
        pg_cursor.execute(query_str)

        results = pg_cursor.fetchall()

        pg_cursor.close()
        pg_conn.close()



        if len(results) == 0:
            context["task_instance"].xcom_push(
                key="previous_spot_record",
                value=None
            )

        else:
            context["task_instance"].xcom_push(
                key="previous_spot_record",
                value=results[0]
            )
