import airflow

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator


with DAG(
    dag_id="spreads_builder",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@daily"
) as dag:

    build_spot_record = DummyOperator(
        task_id="build_spot_record"
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
