from airflow.models.baseoperator import BaseOperator
from Innowise_task_6.classes.snowflake_manager import SnowflakeManager

class RawStreamInserter(BaseOperator):
    def __init__(self, account, database, warehouse, conn_id, name_of_stage_table: str, name_of_raw_stream: str, **kwargs):
        super().__init__(**kwargs)
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id
        self.name_of_stage_table = name_of_stage_table
        self.name_of_raw_stream = name_of_raw_stream

    def execute(self, context):
        SnowflakeManager(self.account, self.database, self.warehouse, self.conn_id)\
            .execute("""insert into {0}
                        select _id::varchar(24), ios_app_id::varchar(24), title::text, developer_name::text, developer_ios_id::varchar(24), ios_store_url::text,
                        seller_official_website::text, age_rating::VARCHAR(25), try_to_double(total_average_rating), try_to_double(total_number_of_ratings), try_to_double(average_rating_for_version), try_to_double(number_of_ratings_for_version), try_to_timestamp(original_release_date, 'yyyy-mm-ddThh24:mi:ssZ'), try_to_timestamp(current_version_release_date, 'yyyy-mm-ddThh24:mi:ssZ'), try_to_double(price_usd), primary_genre::varchar(50),
                        strtok_to_array(replace(replace(replace(all_genres, '[', ''), ']', ''), ' ', ''), ','),
                        strtok_to_array(replace(replace(replace(languages, '[', ''), ']', ''), ' ', ''), ','),
                        description::text
                        from {1}
                        where metadata$action = 'INSERT' """.format(self.name_of_stage_table, self.name_of_raw_stream))