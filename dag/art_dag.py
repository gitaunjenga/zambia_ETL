from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Add the parent directory to the Python path so the ETL script can be imported
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'etl_pipeline')))

# Import the `main` function from the ETL script
from etl import main

# Define default arguments for the DAG
default_args = {
    'owner': 'john',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}


dag = DAG(
    'ETL_Pipeline',
    default_args=default_args,
    description='ETL pipeline DAG',
    schedule_interval='@daily',  # Run the DAG daily
)

run_etl = PythonOperator(
    task_id='run_etl_script',
    python_callable=main,  
    dag=dag,
)

run_etl
