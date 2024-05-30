
import os
import datetime

from airflow.providers.postgres.hooks.postgres import PostgresHook

from dags.src.airflow.operators.build_spot_record_operator import BuildSpotRecordOperator

sql_files_path = '/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql'


def test_build_spot_record_empty_postgres_db(mocked_mongo_hook, mocked_postgres_hook):

    # initialize postgres with no spot records:
    pg_hook = PostgresHook()

    init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres_no_records.sql")

    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook.run(sql)

    # run the task:
    task = BuildSpotRecordOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=os.path.join(sql_files_path, "get_latest_timestamp.sql")
    )

    task.execute(context=())

    # test postgres was updated properly:
    # get the latest record from postgres:
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM spots ORDER BY spot_timestamp DESC LIMIT 1;")
    test_records = cursor.fetchall()

    # test expiration:
    expected_timestamp = datetime.datetime.fromisoformat("2020-09-10T15:45:00.000+00:00")
    test_timestamp = test_records[0][1]
    assert test_timestamp == expected_timestamp

    # test spot:
    expected_spot = 340.77
    test_spot = float(test_records[0][2])
    assert test_spot == expected_spot


# def test_build_spot_record_empty_postgres_db_spot(mocked_mongo_hook, mocked_postgres_hook):

#     # initialize postgres with no spot records:
#     pg_hook = PostgresHook()

#     init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres_no_records.sql")

#     with open(init_file, "r") as f:
#         sql = f.read()

#     pg_hook.run(sql)

#     # run the task:
#     task = BuildSpotRecordOperator(
#         task_id="test",
#         postgres_conn_id="test_postgres",
#         mongo_conn_id="test_mongo",
#         sql_path=os.path.join(sql_files_path, "get_latest_timestamp.sql")
#     )

#     task.execute(context=())

#     # test postgres was updated properly:
#     # get the latest record from postgres:
#     conn = pg_hook.get_conn()
#     cursor = conn.cursor()
#     cursor.execute("SELECT * FROM spots ORDER BY spot_timestamp DESC LIMIT 1;")
#     test_records = cursor.fetchall()

#     expected_timestamp = datetime.datetime.fromisoformat("2020-09-10T15:45:00.000+00:00")

#     test_spot = float(test_records[0][2])

#     assert test_spot == 340.77



def test_build_spot_record_one_previous_record_timestamp(mocked_mongo_hook, mocked_postgres_hook):

    # initialize postgres with one spot records:
    pg_hook = PostgresHook()

    init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres_one_record.sql")

    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook.run(sql)

    postgres_sql_query = os.path.join(sql_files_path, "get_latest_timestamp.sql")

    task = BuildSpotRecordOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=postgres_sql_query
    )

    task.execute(context=())

    # get the latest record from postgres:
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM spots ORDER BY spot_timestamp DESC LIMIT 1;")
    test_records = cursor.fetchall()

    expected_timestamp = datetime.datetime.fromisoformat("2020-12-04T16:50:00.000+00:00")
    test_timestamp = test_records[0][1]
    assert test_timestamp == expected_timestamp



def test_build_spot_record_one_previous_record_spot(mocked_mongo_hook, mocked_postgres_hook):

    # initialize postgres with one spot records:
    pg_hook = PostgresHook()

    # init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres_no_records.sql")
    init_file = os.path.join(os.path.dirname(__file__), "sql", "postgres_one_record.sql")

    with open(init_file, "r") as f:
        sql = f.read()

    pg_hook.run(sql)

    postgres_sql_query = os.path.join(sql_files_path, "get_latest_timestamp.sql")

    task = BuildSpotRecordOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=postgres_sql_query
    )

    task.execute(context=())

    # get the latest record from postgres:
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM spots ORDER BY spot_timestamp DESC LIMIT 1;")
    test_records = cursor.fetchall()

    test_spot = float(test_records[0][2])

    assert test_spot == 369.07
