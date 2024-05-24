from dags.src.airflow.operators.mongo_spot_record_operator import MongoSpotRecordOperator

def test_mongo_spot_record_operator():
    task = MongoSpotRecordOperator(
        task_id="test",
        conn_id="test_mongo",
    )

    result = task.execute(context={})