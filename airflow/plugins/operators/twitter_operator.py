from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook

class TwitterOperator(BaseOperator):

    @apply_defaults
    def __init__(
        self,
        query, 
        conn_id = None,
        start_time = None, 
        end_time = None,
        *args, **kwargs
    ):
        super.__init__(*args, **kwargs)
        self.query = query
        self.conn_id = conn_id
        self.start_time = start_time
        self.end_time = end_time


    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id,
            start_time=self.start_time,
            end_time=self.end_time
        )