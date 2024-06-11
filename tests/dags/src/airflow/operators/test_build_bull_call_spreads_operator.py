import os
import datetime
import pytz

# import pytest
# from pytest_mock import mocker

# from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.mongo.hooks.mongo import MongoHook


from dags.src.airflow.operators.build_bull_call_spreads_operator import BuildBullCallSpreadsOperator

sql_files_path = '/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql'
utc_tz = pytz.timezone('UTC')

spread_keys = [
    "id",
    "index",
    "short_description",
    "long_description",
    "expiration",
    "spot_timestamp",
    "spot",
    "short_strike",
    "long_strike",
    "strike_delta",
    "max_profit",
    "risk",
    "break_even",
    "delta",
    "long_iv",
    "short_iv",
    "expiration_close",
    "profit",
    "time_to_expiration",
    "past_expiration"
    ]

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


    # get the test values from Postgres:
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sql_str)

    test_records = cursor.fetchall()

    # if keys are provided then turn test_records into a dict:
    if keys:
        test_records = dict([(k, v) for k, v in zip(keys, test_records[0])])

    return test_records



def test_build_call_spreads_operator_correct_number_of_spread_records_created(mocked_mongo_hook, mocked_postgres_hook):

    # test_records = run_build_call_spreads_operator(
    #     mocked_mongo_hook=mocked_mongo_hook,
    #     mocked_postgres_hook=mocked_postgres_hook,
    #     sql_str="select COUNT(*) from bull_calls;",
    #     keys=['count']

    # )

    expected_records = 300333
    # assert test_records.get('count') == expected_records
    assert 0 == expected_records




def test_185_385_20201208_bull_call_spread(mocked_mongo_hook, mocked_postgres_hook):

    sql_str = "select * from bull_calls where DATE(expiration) = '2020-12-08' and short_strike = 385.0 and long_strike = 185.0;"


    test_records = run_build_call_spreads_operator(
        mocked_mongo_hook=mocked_mongo_hook,
        mocked_postgres_hook=mocked_postgres_hook,
        sql_str=sql_str,
        keys=spread_keys
    )

    assert test_records.get("short_description") == "SPY201207C00385000"
    assert test_records.get("long_description") == "SPY201207C00185000"
    assert test_records.get("expiration") == utc_tz.localize(datetime.datetime(2020, 12, 8, 4, 59))
    assert test_records.get("spot_timestamp") == utc_tz.localize(datetime.datetime(2020, 12, 4, 16, 50))
    assert test_records.get("spot") = 340.77

