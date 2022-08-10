from airflow.models.baseoperator import BaseOperator
from Innowise_task_6.classes.snowflake_manager import SnowflakeManager

class StageStreamInserter(BaseOperator):
    def __init__(self, account, database, warehouse, conn_id, **kwargs):
        super().__init__(**kwargs)
        self.account = account
        self.database = database
        self.warehouse = warehouse
        self.conn_id = conn_id

    def execute(self, context):
        manager = SnowflakeManager(self.account, self.database, self.warehouse, self.conn_id)

        manager.execute("""insert into IOS_App
            select IOS_App_Id, Developer_IOS_Id, Title, IOS_Store_Url, Age_Rating, Total_Average_Rating, Total_Number_of_Ratings, Average_Rating_For_Version, Number_of_Ratings_For_Version, Original_Release_Date, Current_Version_Release_Date, Price_USD, Primary_Genre, All_Genres, Languages, Description 
            from stage_stream_for_IOS_App
            where METADATA$ACTION = 'INSERT'""")

        manager.execute("""insert into Develop
            select Developer_IOS_Id, Developer_Name, Seller_Official_Website 
            from stage_stream_for_Develop
            where METADATA$ACTION = 'INSERT'""")