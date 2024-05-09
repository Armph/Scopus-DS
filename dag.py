from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 8),
    'retries': 1,
}

dag = DAG(
    'scopus_dag',
    default_args=default_args,
    description='A simple DAG to collect and preprocess data from Scopus',
    schedule_interval='@daily',
)

collect_data = BashOperator(
    task_id='collect_data',
    bash_command='python /DE/scopus_search.py',
    dag=dag,
)

preprocess_data = SparkSubmitOperator(
    task_id='preprocess_data',
    conn_id='spark_default',
    application='/DE/spark.py',
    dag=dag,
)
