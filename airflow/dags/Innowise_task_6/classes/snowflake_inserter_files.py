from Innowise_task_6.classes.snowflake_manager import SnowflakeManager


class SnowflakeInserterFiles(SnowflakeManager):
    def __init__(self, account, database, warehouse, conn_id) -> None:
        super().__init__(account, database, warehouse, conn_id)

    def __call__(self, file_name: str, stage_name):
        super().execute('''put 'file://{0}' @{1}'''.format(file_name, stage_name))