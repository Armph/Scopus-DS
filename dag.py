from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from DE.scopus_search import scopus_search
from DE.spark import spark_submit
from DS.linear import analyze_data

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 8),
    'retries': 1,
}

dag = DAG(
    'scopus_dag',
    default_args=default_args,
    description='A simple DAG to collect and preprocess data from Scopus',
    schedule_interval=timedelta(days=1),
)

collect_data = PythonOperator(
    task_id='scopus_search',
    python_callable=scopus_search,
    dag=dag,
)

preprocess_data = PythonOperator(
    task_id='spark_submit',
    python_callable=spark_submit,
    dag=dag,
)

analyze_data = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    dag=dag,
)

collect_data >> preprocess_data >> analyze_data
