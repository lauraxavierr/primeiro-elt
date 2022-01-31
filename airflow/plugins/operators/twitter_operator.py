from airflow.models import BaseOperator, DAG, TaskInstance
from hooks.twitter_hook import TwitterHook

from datetime import datetime
import json

class TwitterOperator(BaseOperator):

    def __init__(
        self,
        query, 
        conn_id = None,
        start_time = None,
        end_time = None,
        *args, **kwargs
    ):
        super.__init__(*args, **kwargs)
        self.query = query,
        self.conn_id = conn_id,
        self.start_time = start_time,
        self.end_date = end_time

    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )
        for page in hook.run():
            print(json.dumps(page, indent=4, sort_keys=True))
            
if __name__ == "__main__":
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            query="AluraOnline", task_id="test_run"
        )
        task_inst = TaskInstance(task=to, execution_date=datetime.now())
        to.execute(task_inst.get_template_context())