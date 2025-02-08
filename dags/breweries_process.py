from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag = DAG(
    dag_id='breweries_process',
    default_args={
        'owner': 'airflow',
        "start_date": datetime(2025, 2, 8),
    },
    schedule_interval='@daily',
)

start = PythonOperator(
    task_id='start',
    python_callable=lambda: print("Jobs Started"),
    dag=dag,
)
bronze_step = SparkSubmitOperator(
    task_id='bronze_step',
    conn_id='spark-conn',
    application="scripts/breweries/bronze_layer.py",
    dag=dag
)

silver_step = SparkSubmitOperator(
    task_id='silver_step',
    conn_id='spark-conn',
    application="scripts/silver_process.py",
    dag=dag
)
gold_step = SparkSubmitOperator(
    task_id='gold_step',
    conn_id='spark-conn',
    application="scripts/gold_process.py",
    dag=dag
)
end = PythonOperator(
    task_id='end',
    python_callable=lambda: print("Jobs Completed successfully"),
    dag=dag,
)
start >> bronze_step >> silver_step >> gold_step >> end
