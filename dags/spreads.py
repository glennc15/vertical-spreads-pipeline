import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.mongo.hooks.mongo import MongoHook
import pymongo
from pymongo import MongoClient

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
        # hook = MongoHook(mongo_conn_id="mongo_connection")
        # mongo_client = hook.get_conn()

        mongo_address = "mongodb://192.168.1.178:27017/"
        mongo_client = MongoClient(mongo_address)

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
    schedule_interval="@daily"
) as dag:

    # build_spot_record = DummyOperator(
    #     task_id="build_spot_record"
    # )

    build_spot_record = PythonOperator(
        task_id="build_spot_record",
        python_callable=_get_spot_record,
        dag=dag
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


    build_spot_record >> write_spot_record >> clean_up_spot_temp_files >> build_expiration_records >> write_expiration_records >> clean_up_expiration_temp_files

    clean_up_expiration_temp_files >> [build_call_spreads, build_put_spreads]

    build_call_spreads >> write_call_spreads >> clean_up_call_temp_files

    build_put_spreads >> write_put_spreads >> clean_up_put_temp_files

    [clean_up_call_temp_files, clean_up_put_temp_files] >> send_notification
