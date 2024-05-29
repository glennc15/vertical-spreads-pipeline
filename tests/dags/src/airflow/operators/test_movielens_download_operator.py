import datetime

from airflow import DAG
from airflow.models.connection import Connection

from pytest_mock import mocker

from dags.src.airflow_movielens.movielens_download_operator import MovielensDownloadOperator
from dags.src.airflow_movielens.hooks import MovielensHook

dag = DAG(
    "test_dag",
    default_args={
        "owner": "airflow",
        "start_date": datetime.datetime(2019, 1, 1)
    },
    schedule_interval="@daily"
)

def test_movielens_operator(tmp_path, mocker):
    mocker.patch.object(
        MovielensHook,
        "get_connection",
        return_value=Connection(
            conn_id="test",
            login="airflow",
            password="airflow"
        )
    )

    task = MovielensDownloadOperator(
        task_id="test",
        conn_id="testconn",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        output_path=str(tmp_path / "{{ ds }}.json"),
        dag=dag
    )

    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )



