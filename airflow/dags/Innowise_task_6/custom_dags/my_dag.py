from airflow import DAG
from airflow.utils.dates import days_ago
from Innowise_task_6.custom_operators.copy_from_stage import CopyFromStageToRawTable
from Innowise_task_6.custom_operators.parse_csv_data import CSVParser
from Innowise_task_6.custom_operators.put_file_to_stage import PutFileToStage
from airflow.operators.python import PythonOperator


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
        input_file_name='/home/ndx/Innowise tasks/Innowise_task_6/airflow/dags/Innowise_task_6/data/763K_plus_IOS_Apps_Info.csv',
        output_file_name='/home/ndx/Innowise tasks/Innowise_task_6/airflow/dags/Innowise_task_6/data/parse.csv'
    )

    put_file = PutFileToStage(
        task_id='put_file_to_stage',
        account='oi17984.eu-north-1.aws', 
        database="INNOWISE_TASK_6",
        warehouse="COMPUTE_WH", 
        conn_id='test_snowflake_connector',
        name_of_stage='stage_storage',
        name_of_file='/home/ndx/Innowise tasks/Innowise_task_6/airflow/dags/Innowise_task_6/data/parse.csv'
    )

    copy_from_stage = CopyFromStageToRawTable(
        task_id='copy_from_stage',
        account='oi17984.eu-north-1.aws', 
        database="INNOWISE_TASK_6",
        warehouse="COMPUTE_WH", 
        conn_id='test_snowflake_connector',
        name_of_raw_table='raw_table',
        name_of_stage='stage_storage',
        name_of_file='parse.csv'
    )

    parser >> put_file >> copy_from_stage