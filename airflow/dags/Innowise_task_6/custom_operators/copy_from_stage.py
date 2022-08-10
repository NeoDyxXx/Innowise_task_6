from airflow.models.baseoperator import BaseOperator
from Innowise_task_6.classes.snowflake_manager import SnowflakeManager
import os

class CopyFromStageToRawTable(BaseOperator):
    def __init__(self, account, database, warehouse, conn_id, name_of_raw_table: str, name_of_stage: str, directory_name: str, **kwargs):
        super().__init__(**kwargs)
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id
        self.name_of_raw_table = name_of_raw_table
        self.name_of_stage = name_of_stage
        self.directory_name = directory_name

    def execute(self, context):
        cwd = os.getcwd()
        os.chdir(self.directory_name)
        count = max([int(item.split('.')[0]) for item in os.listdir()])
        SnowflakeManager(self.account, self.database, self.warehouse, self.conn_id)\
            .execute('''COPY INTO {0} from '@{1}/{2}.gz'  
                            file_format = (type=CSV COMPRESSION = GZIP FIELD_DELIMITER =',' RECORD_DELIMITER='\n' 
                                FIELD_OPTIONALLY_ENCLOSED_BY = '0x22')'''.format(self.name_of_raw_table, self.name_of_stage, str(count) + '.csv'))
        os.chdir(cwd)