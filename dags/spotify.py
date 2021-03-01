from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.mysql_operator import MySqlOperator

from dataFromSpotify import getDataFromSpotify

default_args = {
    'owner' : 'Airflow',
    'start_date': datetime(2019, 11, 30),
    'retries': 1,
    'retries_delay': timedelta(seconds=5)
}

with DAG('spotify_dag',
        default_args=default_args,
        schedule_interval='@daily',
        template_searchpath=['/usr/local/airflow/sql_files'], 
        catchup=False) as dag:

    t1 = PythonOperator(
        task_id='get_data_from_spotify', 
        python_callable=getDataFromSpotify,
        )

    t2 = MySqlOperator(
        task_id='create_mysql_table',
        mysql_conn_id="mysql_conn",
        sql="create_table.sql",
        )

    t3 = MySqlOperator(
        task_id='insert_mysql_table',
        mysql_conn_id="mysql_conn",
        sql="insert_into_table.sql"
        )

t1 >> t2 >> t3

