To run Apache airflow locally -
  Checkout this project and execute following commands -
    1. install apache airflow -> pip install apache-airflow
    2. cd etl-workflow
    3. initialise airflow db -> airflow db init
    4. finally run command -> airflow standalone
        This will launch airflow locally.
        Login to airflow by using credentials from logs
    5. Check for dag with id = d1.(Definition present under - etl-workflow/dags/dag.py)
    6. Every night job will run around 12.30 AM with following jobs
        bronze -> silver -> gold -> analysis