from airflow.models.baseoperator import BaseOperator
from Innowise_task_6.classes.snowflake_creater import SnowflakeCreater

class RecreaterItem(BaseOperator):
    def __init__(self, account, database, warehouse, conn_id, activate: bool, **kwargs):
        super().__init__(**kwargs)
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id
        self.activate = activate

    def execute(self, context):
        if self.activate:
            SnowflakeCreater(self.account, self.database, self.warehouse, self.conn_id)()