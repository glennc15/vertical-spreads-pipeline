import os
# import datetime

# import pytest
# from pytest_mock import mocker

# from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.mongo.hooks.mongo import MongoHook


from dags.src.airflow.operators.build_bull_call_spreads_operator import BuildBullCallSpreadsOperator

sql_files_path = '/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql'



def run_build_call_spreads_operator(mocked_mongo_hook, mocked_postgres_hook, sql_str, keys=None):
    # initialize postgres:
    pg_hook = PostgresHook()

    init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres-init-expiration-records.sql")
    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook.run(sql)

    # run the operator to build the postgres records:
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

    # sql_str = "SELECT * FROM bull_calls;"

    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_str)

    test_records = cursor.fetchall()

    # if keys are provided then turn test_records into a dict:
    if keys:
        test_records = dict([(k, v) for k, v in zip(keys, test_records[0])])

    return test_records



def test_build_call_spreads_operator_correct_number_of_spread_records_created(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_call_spreads_operator(
        mocked_mongo_hook=mocked_mongo_hook,
        mocked_postgres_hook=mocked_postgres_hook,
        sql_str="select COUNT(*) from bull_calls;",
        keys=['count']

    )

    # sql_str = "select COUNT(*) from bull_calls;"

    # pg_hook = run_build_call_spreads_operator.get('pg_hook')
    # conn = pg_hook.get_conn()
    # cursor = conn.cursor()
    # cursor.execute(sql_str)

    # test_records = cursor.fetchall()

    # test_records = dict([(k, v) for k, v in zip(['count'], test_records[0])])
    # print("********** test_records *****************")
    # print(test_records)

    expected_records = 300333
    assert test_records.get('count') == expected_records
    # assert 0 == expected_records



# def test_build_call_spreads_operator_spread1_long_description(mocked_mongo_hook, mocked_postgres_hook):
#     test_records = run_build_call_spreads_operator(
#         mocked_mongo_hook=mocked_mongo_hook,
#         mocked_postgres_hook=mocked_postgres_hook
#     )

#     assert False