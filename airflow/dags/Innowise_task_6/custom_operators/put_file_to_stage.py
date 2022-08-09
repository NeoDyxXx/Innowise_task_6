from airflow.models.baseoperator import BaseOperator
from Innowise_task_6.classes.snowflake_inserter_files import SnowflakeInserterFiles

class PutFileToStage(BaseOperator):
    def __init__(self, account, database, warehouse, conn_id, name_of_stage: str, name_of_file: str, **kwargs):
        super().__init__(**kwargs)
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id
        self.name_of_stage = name_of_stage
        self.name_of_file = name_of_file

    def execute(self, context):
        SnowflakeInserterFiles(self.account, self.database, self.warehouse, self.conn_id)(self.name_of_file, self.name_of_stage)