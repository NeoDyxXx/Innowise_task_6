from airflow import DAG
from airflow.utils.dates import days_ago
from Innowise_task_6.custom_operators.copy_from_stage import CopyFromStageToRawTable
from Innowise_task_6.custom_operators.parse_csv_data import CSVParser
from Innowise_task_6.custom_operators.put_file_to_stage import PutFileToStage
from Innowise_task_6.custom_operators.recreate_item_in_db import RecreaterItem
from Innowise_task_6.custom_operators.raw_stream_inserter import RawStreamInserter
from Innowise_task_6.custom_operators.stage_stream_inserter import StageStreamInserter


default_args = {
    'owner': 'airflow',
    'retries': 1
}


with DAG(
    dag_id='Innowise_task_6',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['Innowise task'],
    max_active_runs=1
) as dag:
    parser = CSVParser(
        task_id="csv_parser",
        input_file_name='/home/ndx/Innowise tasks/Innowise_task_6/airflow/dags/Innowise_task_6/data/raw_data/763K_plus_IOS_Apps_Info.csv',
        output_file_directory='/home/ndx/Innowise tasks/Innowise_task_6/airflow/dags/Innowise_task_6/data/parse_data'
    )

    creater = RecreaterItem(
        task_id='recreate_item',
        account='oi17984.eu-north-1.aws', 
        database="INNOWISE_TASK_6",
        warehouse="COMPUTE_WH", 
        conn_id='test_snowflake_connector',
        activate=False
    )

    put_file = PutFileToStage(
        task_id='put_file_to_stage',
        account='oi17984.eu-north-1.aws', 
        database="INNOWISE_TASK_6",
        warehouse="COMPUTE_WH", 
        conn_id='test_snowflake_connector',
        name_of_stage='stage_storage',
        directory_name='/home/ndx/Innowise tasks/Innowise_task_6/airflow/dags/Innowise_task_6/data/parse_data'
    )

    copy_from_stage = CopyFromStageToRawTable(
        task_id='copy_from_stage',
        account='oi17984.eu-north-1.aws', 
        database="INNOWISE_TASK_6",
        warehouse="COMPUTE_WH", 
        conn_id='test_snowflake_connector',
        name_of_raw_table='raw_table',
        name_of_stage='stage_storage',
        directory_name='/home/ndx/Innowise tasks/Innowise_task_6/airflow/dags/Innowise_task_6/data/parse_data'
    )

    raw_stream = RawStreamInserter(
        task_id='raw_stream',
        account='oi17984.eu-north-1.aws', 
        database="INNOWISE_TASK_6",
        warehouse="COMPUTE_WH", 
        conn_id='test_snowflake_connector',
        name_of_raw_stream='raw_stream',
        name_of_stage_table='stage_table'
    )

    stage_stream = StageStreamInserter(
        task_id='stage_stream',
        account='oi17984.eu-north-1.aws', 
        database="INNOWISE_TASK_6",
        warehouse="COMPUTE_WH", 
        conn_id='test_snowflake_connector'
    )

    [parser, creater] >> put_file >> copy_from_stage >> raw_stream >> stage_stream