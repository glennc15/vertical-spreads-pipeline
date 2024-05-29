
import os

from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook


# from pytest_docker_tools import fetch, container
# from pytest_mock import mocker

from dags.src.airflow.operators.build_spot_record_operator import BuildSpotRecordOperator
from dags.src.airflow.operators.mongo_spot_record_operator import MongoSpotRecordOperator

# test-mongo-lake
# postgres_image = fetch(respository="mongo")
# postgres_image = container(
#     image="{postgres_image.id}",
#     environment={
#         "POSTGRES_USER": "root",
#         "POSTGRES_PASSWORD": "root",
#         "POSTGRES_DB": "Verticals"
#     },
#     ports={"5432/tcp":None},
#     volumes={
#         os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
#             "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
#         },
#         "/Users/glenn/Documents/ProgrammingStuff/spreads/test-postgres-data": {
#             "bind": "/var/lib/postgresql/data:rw"
#         }
#     }
# )


# test-postgres-spreads
# postgres_image = fetch(repository="postgres:13")

# postgres_container = container(
#     image="{postgres_image.id}",
#     environment={
#         "POSTGRES_USER": "root",
#         "POSTGRES_PASSWORD": "root",
#         "POSTGRES_DB": "Verticals"
#     },
#     ports={"5432/tcp":None},
#     volumes={
#         os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
#             "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
#         },
#         "/Users/glenn/Documents/ProgrammingStuff/spreads/test-postgres-data": {
#             "bind": "/var/lib/postgresql/data:rw"
#         }
#     }
# )


def test_mongo_spot_record_operator(mocker):
    mocker.patch.object(
        MongoHook,
        "get_connection",
        return_value=Connection(
            conn_id="mongo-lake",
            conn_type="mongodb",
            host="localhost",
            port=27017
        )
    )

    task = MongoSpotRecordOperator(
        task_id="task_id",
        conn_id="test_id"
    )

    task.execute(context={})


def test_build_spot_record_empty_postgres_db_id(mocker):
    # mocker.patch.object(
    #     BuildSpotRecordOperator,
    #     "get_connection",
    #     return_value=Connection(
    #         conn_id="test",
    #         login="airflow",
    #         password="airflow"
    #     )
    # )

    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            conn_id="postgres",
            conn_type="postgres",
            host="localhost",
            login="root",
            password="root",
            port=5432
        )
    )





    task = BuildSpotRecordOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=""
    )

        #
        # {
        # "_id": {
        # "$oid": "5f5a4a7b29e0e1eee1d04d9a"
        # },
        # "ask": 340.77,
        # "bid": 340.76,
        # "spot": 340.77,
        # "timestamp": {
        # "$date": "2020-09-10T15:45:00.000Z"
        # },
        # "vol": 23547516
        # }

def test_build_spot_record_empty_postgres_db_timestamp():
    task = BuildSpotRecordOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=""
    )


def test_build_spot_record_empty_postgres_db_spot():
    task = BuildSpotRecordOperator(
        task_id="test",
        postgres_conn_id="test_postgres",
        mongo_conn_id="test_mongo",
        sql_path=""
    )


def test_build_spot_record_one_previous_record_id():
    pass


    # {
    # "_id": {
    #     "$oid": "5fca68f9f4e6f38c51a0edb4"
    # },
    # "ask": 369.08,
    # "bid": 369.06,
    # "spot": 369.07,
    # "timestamp": {
    #     "$date": "2020-12-04T16:50:00.000Z"
    # },
    # "vol": 13354692
    # }


def test_build_spot_record_one_previous_record_timestamp():
    pass


def test_build_spot_record_one_previous_record_spot():
    pass