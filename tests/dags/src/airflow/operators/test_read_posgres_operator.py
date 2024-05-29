from dags.src.airflow.operators.read_postgres_operator import ReadPostgresOperator
from airflow.models.connection import Connection


# need a few tests:
# first test: pull records from an empty spots database:
# 2nd test: pull record from a database with 1 data record -> expected result is that record
# 3rd test: pull record from a database with 3 records -> expected result is oldest record (highest timestamp)
def test_read_postgres_operator():
    task = ReadPostgresOperator(
        task_id="test",
        conn_id="test_posgres",
        sql="/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql/vertical_schemas.sql"
    )

    result = task.execute(context={})



