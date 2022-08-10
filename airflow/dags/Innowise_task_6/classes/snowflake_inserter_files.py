from Innowise_task_6.classes.snowflake_manager import SnowflakeManager
import os


class SnowflakeInserterFiles(SnowflakeManager):
    def __init__(self, account, database, warehouse, conn_id) -> None:
        super().__init__(account, database, warehouse, conn_id)

    def __call__(self, directory_name: str, stage_name):
        cwd = os.getcwd()
        os.chdir(directory_name)
        count = max([int(item.split('.')[0]) for item in os.listdir()])
        super().execute('''put 'file://{0}' @{1}'''.format(os.path.join(directory_name, str(count) + '.csv'), stage_name))
        os.chdir(cwd)
        