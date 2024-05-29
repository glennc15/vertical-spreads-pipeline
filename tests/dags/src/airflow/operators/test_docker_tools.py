import os
import datetime

from airflow.models.connection import Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook

from pytest_docker_tools import fetch, container
from pytest_mock import mocker

from dags.src.airflow_movielens.movielens_to_postgres_operator import MovielensToPostgresOperator
from dags.src.airflow_movielens.hooks import MovielensHook

postgres_image = fetch(repository="postgres:11.1-alpine")

postgres = container(
    image="{postgres_image.id}",
    environment={
        "POSTGRES_USER": "testuser",
        "POSTGRES_PASSWORD": "testpass",
    },
    ports={"5432/tcp":None},
    volumes={
        os.path.join(os.path.dirname(__file__), "postgres-init.sql"): {
            "bind": "/docker-entrypoint-initdb.d/postgres-init.sql"
        }
    }
)


def test_movielens_to_postgres_operator(mocker, test_dag, postgres):
    mocker.patch.object(
        MovielensHook,
        "get_connection",
        return_value=Connection(
            conn_id="test",
            login="airflow",
            password="airflow"
        )
    )

    mocker.patch.object(
        PostgresHook,
        "get_connection",
        return_value=Connection(
            conn_id="postgres",
            conn_type="postgres",
            host="localhost",
            login="testuser",
            password="testpass",
            port=postgres.ports["5432/tcp"][0]
        )
    )

    task = MovielensToPostgresOperator(
        task_id="test",
        movielens_conn_id="movielens_id",
        start_date="{{ prev_ds }}",
        end_date="{{ ds }}",
        postgres_conn_id="postgres_id",
        insert_query=(
            "INSERT INTO movielens (movidId, rating, ratingTimestamp, userId, scrapeTime) "
            "VALUES ({0}, '{{ macros.datetime.now() }}')"
        ),
        dag=test_dag
    )

    pg_hook = PostgresHook()

    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count == 0

    task.run(
        start_date=test_dag.default_args["start_date"],
        end_date=test_dag.default_args["start_date"],

    )

    row_count = pg_hook.get_first("SELECT COUNT(*) FROM movielens")[0]
    assert row_count > 0

# def test_call_fixture(postgres_container):
#     print(
#         f"Running Postgres container named {postgres_container.name} ",
#         f"on port {postgres_container.ports['5432/tcp'][0]}."
#     )

# def test_call_fixture(postgres_image):
#     print(postgres_image.id)

# def test_misc():
#     print(f"__file__ = {__file__}")