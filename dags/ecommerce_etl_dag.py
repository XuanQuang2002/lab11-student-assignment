from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import sys

# Add src to path so airflow can import it
sys.path.append('/opt/airflow/')

from src.extract import extract_to_bronze
from src.transform import clean_to_silver, check_quality
from src.load import load_to_gold

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data_team@example.com'],
}

def print_execution_report(**kwargs):
    """
    Generate a summary report of the pipeline execution (Monitoring step).
    """
    print("="*50)
    print(" 🚀 E-COMMERCE ETL PIPELINE EXECUTION REPORT 🚀")
    print("="*50)
    print(f"DAG Run ID: {kwargs['dag_run'].run_id}")
    print(f"Logical Date: {kwargs['logical_date']}")
    print("Status: SUCCESS ✅")
    print("Pipeline has successfully extracted Data Lake files,")
    print("passed Data Contracts (Great Expectations),")
    print("and loaded into PostgreSQL Star Schema Data Mart.")
    print("="*50)

with DAG(
    'ecommerce_end_to_end_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='End-to-End ETL for E-commerce Data using Medallion Architecture'
) as dag:

    start = EmptyOperator(task_id='start')
    t_extract = PythonOperator(
        task_id='extract_to_bronze',
        python_callable=extract_to_bronze,
    )
    t_transform = PythonOperator(
        task_id='transform_to_silver',
        python_callable=clean_to_silver,
    )
    t_branch = BranchPythonOperator(
        task_id='check_quality', 
        python_callable=check_quality
    )
    t_load = PythonOperator(
        task_id='load_to_gold', 
        python_callable=load_to_gold
    )
    t_report = PythonOperator(
        task_id='execution_report', 
        python_callable=print_execution_report
    )
    t_alert = EmptyOperator(
        task_id='alert_quality'
    )
    end = EmptyOperator(
        task_id='end', trigger_rule='none_failed'
    )
    
    start >> t_extract >> t_transform >> t_branch # type: ignore
    t_branch >> t_load >> t_report >> end # type: ignore
    t_branch >> t_alert >> end # type: ignore
