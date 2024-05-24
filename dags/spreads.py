import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from src.airflow.operators.read_postgres_operator import ReadPostgresOperator
from src.airflow.operators.mongo_spot_record_operator import MongoSpotRecordOperator



import os

# import pymongo
# from pymongo import MongoClient

import datetime

# def _build_spot_record():
#     spot_record = MongoSpotRecord(
#         underlying="SPY",
#         timestamp=""
#     )
#     spot_record.get()
#     # SPY_20240113_0855_spot.sql
#     filename = f"{}_{}_{}_spot.sql"

def _get_spot_record(**context):

    print("********************")
    print("_get_spot_record")

    try:
        hook = MongoHook(mongo_conn_id="mongo-lake")
        mongo_client = hook.get_conn()

        # mongo_address = "mongodb://192.168.1.178:27017/"
        # mongo_client = MongoClient(mongo_address)

        # search_query = {
        #     "timestamp": {"$lt": datetime.datetime.now()}
        # }
        # sort_order = [
        #     ("timestamp", pymongo.DESCENDING)
        # ]


        # records = mongo_client['OptionData']['SPY_Spots'].find(search_query, sort=sort_order, limit=1)


        # print(list(records))
        print(list(mongo_client['OptionData']['SPY_Options'].find(limit=1)))

    except Exception as e:
        print(f"Error connecting to MongoDB -- {e}")



with DAG(
    dag_id="spreads_builder",
    start_date=airflow.utils.dates.days_ago(1),
    schedule_interval="@daily",
    template_searchpath="/tmp"
) as dag:

    # create_expiration_table = PostgresOperator(
    #     task_id="create_expiration_table",
    #     postgres_conn_id="postgres-spreads",
    #     sql="sql/expiration_schema.sql"
    # )

    create_vertical_tables = PostgresOperator(
        task_id="create_vertical_tables",
        postgres_conn_id="postgres-spreads",
        sql="sql/vertical_schemas.sql"
    )

    # with open("sql/get_latest_timestamp.sql", "r") as f:
    #     query = f.read()
    # /opt/airflow/dags/spreads.py]

    print(os.listdir("/opt/airflow"))

    poll_pg_timestamps = ReadPostgresOperator(
        task_id="poll_pg_timestamps",
        conn_id="postgres-spreads",
        sql="sql/get_latest_timestamp.sql"
    )

    # build_spot_record = DummyOperator(
    #     task_id="build_spot_record"
    # )

    # build_spot_record = PythonOperator(
    #     task_id="build_spot_record",
    #     python_callable=_get_spot_record,
    #     dag=dag
    # )

    get_spot_record_mongo = MongoSpotRecordOperator(
        task_id="get_spot_record_mongo",
        # timestamp="{{task_instance.xcom_pull(task_ids='build_spot_record', key='previous_spot_record')}}"
    )

    write_spot_record = DummyOperator(
        task_id="write_spot_record"
    )

    clean_up_spot_temp_files = DummyOperator(
        task_id="clean_up_spot_temp_files"
    )

    build_expiration_records = DummyOperator(
        task_id="build_expiration_records"
    )

    write_expiration_records = DummyOperator(
        task_id="write_expiration_records"
    )

    clean_up_expiration_temp_files = DummyOperator(
        task_id="clean_up_expiration_temp_files"
    )

    build_call_spreads = DummyOperator(
        task_id="build_call_spreads"
    )

    write_call_spreads = DummyOperator(
        task_id="write_call_spreads"
    )

    clean_up_call_temp_files = DummyOperator(
        task_id="clean_up_call_temp_files"
    )

    build_put_spreads = DummyOperator(
        task_id="build_put_spreads"
    )

    write_put_spreads = DummyOperator(
        task_id="write_put_spreads"
    )

    clean_up_put_temp_files = DummyOperator(
        task_id="clean_up_put_temp_files"
    )


    send_notification = DummyOperator(
        task_id="send_notification"
    )


    # create_vertical_tables >> poll_pg_timestamps >> build_spot_record >> write_spot_record >> clean_up_spot_temp_files >> build_expiration_records >> write_expiration_records >> clean_up_expiration_temp_files
    create_vertical_tables >> poll_pg_timestamps >> get_spot_record_mongo >> write_spot_record >> clean_up_spot_temp_files >> build_expiration_records >> write_expiration_records >> clean_up_expiration_temp_files


    clean_up_expiration_temp_files >> [build_call_spreads, build_put_spreads]

    build_call_spreads >> write_call_spreads >> clean_up_call_temp_files

    build_put_spreads >> write_put_spreads >> clean_up_put_temp_files

    [clean_up_call_temp_files, clean_up_put_temp_files] >> send_notification
