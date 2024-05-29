import datetime

import pytest
from airflow.models import DAG, BaseOperator

@pytest.fixture
def test_dag():
    return DAG(
        "test_dag",
        default_args={
            "owner": "airflow",
            "start_date": datetime.datetime(2019, 1, 1),
        },
        schedule="@daily"
    )