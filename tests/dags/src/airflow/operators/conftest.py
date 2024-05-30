import datetime

from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook

import pytest
from pytest_mock import mocker


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

@pytest.fixture()
def mocked_mongo_hook(mocker):
    connection = Connection(
        conn_id="mongo-lake",
        conn_type="mongodb",
        host="localhost",
        port=27017
    )

    return mocker.patch.object(
        MongoHook,
        "get_connection",
        return_value=connection
    )

@pytest.fixture()
def mocked_postgres_hook(mocker):
    connection = Connection(
        conn_id="postgres",
        conn_type="postgres",
        host="localhost",
        login="root",
        password="root",
        schema="Vertical",
        port=5432
    )

    return mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=connection
    )

