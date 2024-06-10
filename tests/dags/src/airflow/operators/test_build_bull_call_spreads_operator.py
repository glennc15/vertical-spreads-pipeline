import os
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.src.airflow.operators.build_bull_call_spreads_operator import BuildBullCallSpreadsOperator

sql_files_path = '/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql'

def run_build_call_spreads_operator(mocked_mongo_hook, mocked_postgres_hook):
        # initialize postgres with one spot records:
    pg_hook = PostgresHook()

    init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres-init-expiration-records.sql")

    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook.run(sql)

    postgres_sql_query = os.path.join(sql_files_path, "get_latest_spot_record.sql")

    task = BuildBullCallSpreadsOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=postgres_sql_query
    )

    task.execute(context={})

    # get the latest expiration records:
    # sql_str = '''
    #     SELECT *
    #     FROM spots
    #     INNER JOIN expirations
    #     ON expirations.spot_id = (SELECT id FROM spots ORDER BY spot_timestamp DESC LIMIT 1)
    #     ORDER BY expirations.expiration ASC;'''

    sql_str = "SELECT * FROM bull_calls;"

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_str)

    test_records = cursor.fetchall()

    return test_records


def test_build_call_spreads_operator_correct_number_of_spread_records_created(mocked_mongo_hook, mocked_postgres_hook):
    test_records = run_build_call_spreads_operator(
        mocked_mongo_hook=mocked_mongo_hook,
        mocked_postgres_hook=mocked_postgres_hook
    )

    expected_records = 300333
    assert len(test_records) == expected_records


# def test_build_call_spreads_operator_spread1_long_description(mocked_mongo_hook, mocked_postgres_hook):
#     test_records = run_build_call_spreads_operator(
#         mocked_mongo_hook=mocked_mongo_hook,
#         mocked_postgres_hook=mocked_postgres_hook
#     )

#     assert False