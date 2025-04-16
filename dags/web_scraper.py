import os
import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.state import State
from airflow.models import XCom
import logging
import sys # <--- ADDED import sys

# --- START: Re-added sys.path modification ---
scraper_path = '/opt/airflow/scraper'
if scraper_path not in sys.path:
    print(f"Appending {scraper_path} to sys.path") # Optional: log message
    sys.path.append(scraper_path)
# --- END: Re-added sys.path modification ---


# --- Import the CORRECT function ---
try:
    from scraper import run_configured_scrapers # Now hopefully works due to sys.path
except ImportError as e:
    logging.error(f"Failed to import run_configured_scrapers (sys.path={sys.path}): {e}", exc_info=True) # Log sys.path on failure
    # Fallback or error handling if the scraper module isn't found
    def run_configured_scrapers(**kwargs):
        print("CRITICAL WARNING: 'scraper.run_configured_scrapers' could not be imported! Returning dummy data.")
        return [{'title': 'Dummy Job (Import Failed)', 'company_name': 'Dummy Inc.', 'location': 'Anywhere', 'description_snippet': 'Test job.', 'job_url': 'http://example.com', 'visa_sponsored': True}]

# Configuration (keep as is)
SCRAPER_CONFIG_PATH = '/opt/airflow/config/scraper_config.yaml'
SCRAPED_DATA_XCOM_KEY = "scraped_data_key"
STORE_DAG_ID = "store_scraped_data"
NOTIFY_DAG_ID = "notify_data_added_dag"

# Default args (keep as is)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# run_scrapers_and_push_data (keep as is)
def run_scrapers_and_push_data(**context):
    """Calls the configured scraper function and pushes results to XCom."""
    print(f"Running scraper with config: {SCRAPER_CONFIG_PATH}")
    scraped_data = run_configured_scrapers(config_path=SCRAPER_CONFIG_PATH, **context)
    print(f"Scraped {len(scraped_data)} jobs.")
    if scraped_data:
        context['ti'].xcom_push(key=SCRAPED_DATA_XCOM_KEY, value=scraped_data)
        return True
    else:
        print("No new jobs found.")
        return False

# decide_which_path (keep as is)
def decide_which_path(**context):
    data_found = context['ti'].xcom_pull(task_ids='run_configured_scrapers_task')
    if data_found:
        print("Data found, triggering downstream tasks.")
        return ['trigger_store_scraped_data_task', 'trigger_notify_dag']
    else:
        print("No data found, skipping downstream tasks.")
        return 'skip_downstream_tasks'

# DAG definition and tasks (keep as is)
with DAG(
    dag_id='web_scraper_dag',
    default_args=default_args,
    description='Scrapes H1B jobs based on YAML config and triggers downstream DAGs',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['scraper', 'h1b', 'configurable'],
) as dag:
    run_scrapers = PythonOperator(
        task_id='run_configured_scrapers_task',
        python_callable=run_scrapers_and_push_data,
    )
    decide_trigger = BranchPythonOperator(
        task_id='decide_trigger_task',
        python_callable=decide_which_path,
    )
    trigger_store_scraped_data = TriggerDagRunOperator(
        task_id='trigger_store_scraped_data_task',
        trigger_dag_id=STORE_DAG_ID,
        conf={
            "scraped_data_key": SCRAPED_DATA_XCOM_KEY,
            "triggering_run_id": "{{ run_id }}"
        },
        wait_for_completion=False,
    )
    # In dags/web_scraper.py

    trigger_notify = TriggerDagRunOperator(
        task_id='trigger_notify_dag',
        trigger_dag_id=NOTIFY_DAG_ID,
        conf={
            # ADD THE | tojson FILTER HERE:
            "scraped_data": "{{ ti.xcom_pull(task_ids='run_configured_scrapers_task', key='" + SCRAPED_DATA_XCOM_KEY + "') | tojson }}",
            "triggering_run_id": "{{ run_id }}"
        },
        wait_for_completion=False,
    )
    skip_downstream = DummyOperator(
        task_id='skip_downstream_tasks',
    )
    # Dependencies (keep as is)
    run_scrapers >> decide_trigger
    decide_trigger >> [trigger_store_scraped_data, trigger_notify]
    decide_trigger >> skip_downstream