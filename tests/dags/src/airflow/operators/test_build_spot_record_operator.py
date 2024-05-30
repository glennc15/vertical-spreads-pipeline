
import os
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.src.airflow.operators.build_spot_record_operator import BuildSpotRecordOperator

sql_files_path = '/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql'


def run_build_spot_record_operator(mocked_mongo_hook, mocked_postgres_hook, postgres_init_filename):
    # initialize postgres :
    init_file = os.path.join(os.path.dirname(__file__), "sql", postgres_init_filename)
    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook = PostgresHook()
    pg_hook.run(sql)

    # run the task:
    task = BuildSpotRecordOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=os.path.join(sql_files_path, "get_latest_timestamp.sql")
    )

    task.execute(context=())

    # get the latest record from postgres:
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM spots ORDER BY spot_timestamp DESC LIMIT 1;")
    test_records = cursor.fetchall()

    return test_records


def test_build_spot_record_empty_postgres_db_record_timestamp(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_spot_record_operator(
        mocked_mongo_hook=mocked_mongo_hook,
        mocked_postgres_hook=mocked_postgres_hook,
        postgres_init_filename="postgres_no_records.sql"
    )

    # test expiration:
    expected_timestamp = datetime.datetime.fromisoformat("2020-09-10T15:45:00.000+00:00")
    test_timestamp = test_records[0][1]
    assert test_timestamp == expected_timestamp


def test_build_spot_record_empty_postgres_db_spot(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_spot_record_operator(
        mocked_mongo_hook=mocked_mongo_hook,
        mocked_postgres_hook=mocked_postgres_hook,
        postgres_init_filename="postgres_no_records.sql"
    )

    # test spot:
    expected_spot = 340.77
    test_spot = float(test_records[0][2])
    assert test_spot == expected_spot



def test_build_spot_record_one_previous_record_timestamp(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_spot_record_operator(
        mocked_mongo_hook=mocked_mongo_hook,
        mocked_postgres_hook=mocked_postgres_hook,
        postgres_init_filename="postgres_one_record.sql"
    )

    # test the timestamp:
    expected_timestamp = datetime.datetime.fromisoformat("2020-12-04T16:50:00.000+00:00")
    test_timestamp = test_records[0][1]
    assert test_timestamp == expected_timestamp



def test_build_spot_record_one_previous_record_spot(mocked_mongo_hook, mocked_postgres_hook):

    test_records = run_build_spot_record_operator(
        mocked_mongo_hook=mocked_mongo_hook,
        mocked_postgres_hook=mocked_postgres_hook,
        postgres_init_filename="postgres_one_record.sql"
    )

    # test the spot:
    expected_spot = 369.07
    test_spot = float(test_records[0][2])
    assert test_spot == expected_spot
