from airflow.models.connection import Connection

from pytest_mock import mocker
from dags.src.airflow.operators.mongo_spot_record_operator import BuildSpotRecordOperator

def test_mongo_spot_record_operator(mocker):
    mocker.patch.object(
        BuildSpotRecordOperator,
        "get_connection",
        return_value=Connection(
            conn_id="test_mongo",
            login="airflow",
            password="airflow"
        )
    )

    task = BuildSpotRecordOperator(
        task_id="test",
        conn_id="test_mongo",
    )

    result = task.execute(context={})