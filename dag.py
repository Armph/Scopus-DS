from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from DE.scopus_search import scopus_search

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

collect_data = PythonOperator(
    task_id='scopus_search',
    python_callable=scopus_search,
    dag=dag,
)

preprocess_data = SparkSubmitOperator(
    task_id='preprocess_data',
    conn_id='spark_default',
    application='/DE/spark.py',
    total_executor_cores=1,
    executor_cores=1,
    executor_memory='2g',
    num_executors=1,
    driver_memory='2g',
    dag=dag,
)

collect_data >> preprocess_data
