import os
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.src.airflow.operators.build_expiration_records_operator import BuildExpirationRecordsOperator

sql_files_path = '/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql'

def run_build_expiration_records_operator(mocked_mongo_hook, mocked_postgres_hook):
    # initialize postgres with one spot records:
    pg_hook = PostgresHook()

    init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres_one_record.sql")

    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook.run(sql)

    postgres_sql_query = os.path.join(sql_files_path, "get_latest_spot_record.sql")

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
    cursor.execute("SELECT * FROM spots INNER JOIN expirations ON spots.id = expirations.spot_id ORDER BY expirations.expiration;")
    test_records = cursor.fetchall()

    return test_records

def record_to_dict(test_record):
    return {
        "spot_id": test_record[0],
        "timestamp": test_record[1],
        "spot": float(test_record[2]),
        "expiration_id": test_record[3],
        "expiration": test_record[4],
        "time_to_expiration": test_record[5],
        "past_expiration": test_record[6]
    }

def test_build_expiration_records_operator_correct_number_of_expiration_records_created(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    # test the correct number of expiration records were generated:
    expected_records = 34
    assert len(test_records) == expected_records


    print(f"test_records[0] = {test_records[0]}")

def test_build_expiration_records_operator_first_expiration_record_timestamp(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    # test timestamp:
    expected_timestamp = datetime.datetime.fromisoformat("2020-09-10T15:45:00.000+00:00")
    test_timestamp = record_to_dict(test_record=test_records[0]).get("timestamp")
    assert test_timestamp == expected_timestamp


def test_build_expiration_records_operator_first_expiration_record_expiration(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    # test expiration:
    expected_expiration = datetime.datetime.fromisoformat("2020-09-11T21:00:00+00:00")
    test_expiration = record_to_dict(test_record=test_records[0]).get("expiration")
    assert test_expiration == expected_expiration


def test_build_expiration_records_operator_first_expiration_record_time_to_expiration(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    expected_time_to_expiration = 105300.0
    test_time_to_expiration = record_to_dict(test_record=test_records[0]).get("time_to_expiration")
    assert test_time_to_expiration == expected_time_to_expiration


def test_build_expiration_records_operator_first_expiration_record_past_expiration(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    expected_past_expiration = True
    test_past_expiration  = record_to_dict(test_record=test_records[0]).get("past_expiration")
    assert test_past_expiration == expected_past_expiration

# *****************************************************************************************


def test_build_expiration_records_operator_last_expiration_record_timestamp(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    expected_timestamp = datetime.datetime.fromisoformat("2020-09-10T15:45:00.000+00:00")
    test_timestamp = record_to_dict(test_record=test_records[-1]).get("timestamp")
    assert test_timestamp == expected_timestamp

def test_build_expiration_records_operator_last_expiration_record_expiration(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    expected_expiration = datetime.datetime.fromisoformat("2022-12-16T22:00:00+00:00")
    test_expiration = record_to_dict(test_record=test_records[-1]).get("expiration")
    assert test_expiration == expected_expiration


def test_build_expiration_records_operator_last_expiration_record_time_to_expiration(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    expected_time_to_expiration = 71475300.0
    test_time_to_expiration = record_to_dict(test_record=test_records[-1]).get("time_to_expiration")
    assert test_time_to_expiration == expected_time_to_expiration


def test_build_expiration_records_operator_last_expiration_record_past_expiration(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_expiration_records_operator(
        mocked_mongo_hook,
        mocked_postgres_hook
    )

    expected_past_expiration = True
    test_past_expiration = record_to_dict(test_record=test_records[-1]).get("past_expiration")
    assert test_past_expiration == expected_past_expiration