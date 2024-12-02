"""
created 25-11-2024
project automatic-tvp
author rohitphadtare 
"""

import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date

# from spark_scripts.delta_demo import main

sys.path.append("...")

curr_date = date.today() - timedelta(days=2)


def python_task_1(argv1):
    print(f"My task in airflow {argv1}!!")


def python_task_2():
    print("My second task in airflow !!")


# Define the DAG
with DAG(
        dag_id="d1",
        description="DAG for TVP process calculation per vertical and protocol'",
        start_date=datetime(2024, 11, 25, hour=18, minute=50, second=00),
        schedule=timedelta(minutes=10.0),
        catchup=False,
) as dag:
    # python operators task
    bronze_task = PythonOperator(
        task_id="bronze",
        python_callable=python_task_1,
        op_args=[curr_date]
    )

    silver_task = PythonOperator(
        task_id="silver",
        python_callable=python_task_1,
        op_args=[curr_date]
    )

    gold_task = PythonOperator(
        task_id="gold",
        python_callable=python_task_1,
        op_args=[curr_date]
    )

    topk_analysis_task = PythonOperator(
        task_id="topk_analysis",
        python_callable=python_task_1,
        op_args=[curr_date]
    )

    # set dependency between task
    bronze_task >> silver_task >> gold_task >> topk_analysis_task

