import os
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.src.airflow.operators.build_expiration_records_operator import BuildExpirationRecordsOperator

sql_files_path = '/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql'


def test_build_expiration_records_operator(mocked_mongo_hook, mocked_postgres_hook):
    # initialize postgres with one spot records:
    pg_hook = PostgresHook()

    init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres_one_record.sql")

    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook.run(sql)

    postgres_sql_query = os.path.join(sql_files_path, "get_latest_timestamp.sql")

    task = BuildExpirationRecordsOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=postgres_sql_query
    )

    task.execute(context={})

    # get the latest expiration records:
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM spots ORDER BY spot_timestamp DESC LIMIT 1;")
    test_records = cursor.fetchall()

    # test the correct number of expiration records were generated:
    expected_records = 10
    assert len(test_records) == expected_records