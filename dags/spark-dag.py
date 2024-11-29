"""
created 28-11-2024
project automatic-tvp
author rohitphadtare 
"""
import sys
import os


# from datetime import date, timedelta
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Running this workflow every midnight around 12.30 for previous block date
# curr_date = date.today() - timedelta(days=1)

# as of now just using dummy block date for which we have data from Dune
curr_date = "2024-11-24"

dag = DAG(
    dag_id="TVP-Analysis-Flow",
    default_args={
        "owner": "Rohit Phadtare",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval='0 1 * * *'
)

start = PythonOperator(
    task_id="start",
    python_callable=lambda: print("Jobs started"),
    dag=dag
)

bronze_job = BashOperator(
    task_id="bronze_job",
    bash_command=f"python3 /opt/airflow/spark_scripts/bronze.py {curr_date}",
    dag=dag
)

silver_job = SparkSubmitOperator(
    task_id="silver_job",
    conn_id="spark-conn",
    application="/opt/airflow/spark_scripts/silver.py",
    application_args=[curr_date],
    dag=dag
)

gold_job = SparkSubmitOperator(
    task_id="gold_job",
    conn_id="spark-conn",
    application="/opt/airflow/spark_scripts/gold.py",
    application_args=[curr_date],
    dag=dag
)

topk_analysis_job = SparkSubmitOperator(
    task_id="topk_analysis_job",
    conn_id="spark-conn",
    application="/opt/airflow/spark_scripts/topk_analysis.py",
    application_args=[curr_date],
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> bronze_job >> silver_job >> gold_job >> topk_analysis_job >> end
