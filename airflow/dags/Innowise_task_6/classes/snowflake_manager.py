import pandas as pd
from Innowise_task_6.another_lib.snowflake_hook import SnowflakeHook


class SnowflakeManager:
    def __init__(self, account, database, warehouse, conn_id) -> None:
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id
        self.hook = SnowflakeHook(conn_id=self.conn_id, account=self.account, database=self.database, warehouse=self.warehouse)
        self.connect = self.hook.get_conn()

    def execute(self, request):
        with self.connect.cursor() as cursor:
            cursor.execute(request)
            self.connect.commit()
    
    def select(self, request):
        with self.connect.cursor() as cursor:
            cursor.execute(request)
            data = cursor.fetchall()
        
        return data

    def __reconnect(self, account, database, warehouse, conn_id):
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id
        self.hook = SnowflakeHook(conn_id=self.conn_id, account=self.account, database=self.database, warehouse=self.warehouse)
        self.connect = self.hook.get_conn()