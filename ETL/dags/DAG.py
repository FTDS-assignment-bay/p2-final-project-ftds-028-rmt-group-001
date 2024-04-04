import datetime as dt

from airflow.models import DAG
from airflow.operators.python import PythonOperator

from functions.get_data import getData
from functions.concat_columns import concatColumns
from functions.data_cleaning import dataCleaning
from functions.load_data import loadData

default_args = {
    'owner': 'destri',
    'start_date': dt.datetime(2024, 3, 24, 12, 30, 0) -  dt.timedelta(hours=7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=1),
}

with DAG('ETL',
         default_args=default_args,
         schedule_interval = '30 6 * * *'
         ) as dag:

    get_data_from_database = PythonOperator(task_id='Get_Data_From_Database',
                                 python_callable=getData)
    
    concat_data_column = PythonOperator(task_id='Concat_Column',
                                 python_callable=concatColumns)

    cleaning_data = PythonOperator(task_id='Cleaning_Data',
                                 python_callable=dataCleaning)

    post_data_to_database = PythonOperator(task_id='Post_Data_To_Database',
                                 python_callable=loadData)


get_data_from_database >> concat_data_column >> cleaning_data >> post_data_to_database