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
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
# from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Running this workflow every midnight around 12.30 for previous block date
# curr_date = date.today() - timedelta(days=1)

# as of now just using dummy block date for which we have data from Dune
curr_date = "2024-11-23"
master = "spark://spark-master:7077"

# To run dag every midnight 1 AM UTC
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

#bronze_job1 = SparkSubmitOperator(
#    task_id="bronze_job1",
#    conn_id="spark-conn",
#    application="/opt/airflow/spark_scripts/bronze.py",
#    application_args=[curr_date, master],
#    packages="io.delta:delta-spark_2.12:3.2.1",
#    dag=dag
#)

bronze_job = BashOperator(
    task_id='bronze_job',
    bash_command=f'python /opt/airflow/spark_scripts/bronze.py {curr_date} {master}',
    dag=dag
)


silver_job = BashOperator(
    task_id='silver_job',
    bash_command=f'python /opt/airflow/spark_scripts/silver.py {curr_date} {master}',
    dag=dag
)

gold_job = BashOperator(
    task_id='gold_job',
    bash_command=f'python /opt/airflow/spark_scripts/gold.py {curr_date} {master}',
    dag=dag
)

topk_analysis_job = BashOperator(
    task_id='topk_analysis_job',
    bash_command=f'python /opt/airflow/spark_scripts/topk_analysis.py {curr_date} {master}',
    dag=dag
)

end = PythonOperator(
    task_id="end",
    python_callable=lambda: print("Jobs completed successfully"),
    dag=dag
)

start >> bronze_job >> silver_job >> gold_job >> topk_analysis_job >> end


##
#
# Future reference -
#
# bronze_job = BashOperator(
#    task_id="bronze_job",
#    bash_command=f"docker exec -it spark-master bash && python3 /spark_scripts/bronze.py {curr_date}",
#    dag=dag
# )

# bronze_job = SSHOperator(
#     task_id="bronze_job",
#     ssh_conn_id="spark-ssh",
#     command=f"/opt/spark/spark_scripts/bronze.py ${curr_date}",
#     dag=dag
# )
#
