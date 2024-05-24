from dags.src.airflow.operators.read_postgres_operator import ReadPostgresOperator

def test_read_postgres_operator():
    task = ReadPostgresOperator(
        task_id="test",
        conn_id="test_posgres",
        sql="/Users/glenn/Documents/DataEngineering/vertical-spreads-pipeline/dags/sql/vertical_schemas.sql"
    )

    result = task.execute(context={})



