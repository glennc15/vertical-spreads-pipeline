from typing import Any
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.providers.mongo.hooks.mongo import MongoHook
import datetime

class MongoSpotRecordOperator(BaseOperator):
    """


    """


    @apply_defaults
    def __init__(
        self,
        conn_id,
        **kwargs
    ):
        super(MongoSpotRecordOperator, self).__init__(**kwargs)

        self._conn_id = conn_id



    def execute(self, context):


        hook = MongoHook(mongo_conn_id=self._conn_id)
        mongo_client = hook.get_conn()


        timestamp = context["task_instance"].xcom_pull(
            task_ids="poll_pg_timestamps",
            key="previous_spot_record"
        )

        timestamp = datetime.datetime(2020, 8, 13, 13, 55)

        if timestamp:
            # TODO: convert the timestamp str to a datatime:
            query = {"timestamp": {"$gt": timestamp}}


        else:
            query = {"timestamp": {"$ne": None}}

        query_cursor = mongo_client['OptionData']["SPY_Spots"].find(
            query,
            sort=[("timestamp", 1)],
            limit=1
        )

        print(list(query_cursor))




