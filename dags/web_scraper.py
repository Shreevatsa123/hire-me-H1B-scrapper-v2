# dags/web_scraper.py
import json # Keep json import for potential future use, though not strictly needed now
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import sys
import os 
import logging # Import logging

# Configure logging for the DAG file itself
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

scraper_path = '/opt/airflow/scraper' 
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

# Import the new wrapper function that reads the config
try:
    # This function now handles reading the config file internally
    from scraper import run_configured_scrapers 
except ImportError as e:
     logging.error(f"Error importing run_configured_scrapers from {scraper_path}. Error: {e}", exc_info=True)
     # Define a dummy function
     def run_configured_scrapers(**kwargs):
         logging.error("ERROR: run_configured_scrapers function could not be imported!")
         raise ImportError("Dummy function called because import failed.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 23), 
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True 
}

def decide_trigger(**kwargs):
    """Decide whether to trigger store_scraped_data DAG based on job availability."""
    ti = kwargs["ti"]
    # Task ID matches the PythonOperator below
    scraped_data = ti.xcom_pull(task_ids="run_scrapers_task") 
    
    if scraped_data and isinstance(scraped_data, list) and len(scraped_data) > 0:
        logging.info(f"Found {len(scraped_data)} jobs. Triggering store_scraped_data DAG.")
        return "trigger_store_scraped_data_task" 
    else:
        logging.info("⚠️ No new jobs found by scrapers. Skipping store DAG.")
        return "skip_store_scraped_data_task" 

with DAG(
    "web_scraper_dag", 
    default_args=default_args,
    schedule_interval="@daily", 
    catchup=False,
    tags=['scraping', 'configurable'], 
) as dag:
    
    run_scrapers_task = PythonOperator(
        # Changed task_id
        task_id="run_scrapers_task", 
        # Call the wrapper function that reads the config
        python_callable=run_configured_scrapers, 
        # op_kwargs could be used to override the default config path if needed:
        # op_kwargs={'config_path': '/opt/airflow/custom_config/my_config.json'},
        do_xcom_push=True  
    )

    branch_task = BranchPythonOperator(
        task_id="decide_trigger_task", 
        python_callable=decide_trigger,
    )

    trigger_store_data = TriggerDagRunOperator(
        task_id="trigger_store_scraped_data_task",
        trigger_dag_id="store_scraped_data", 
        # Pull XCom from the correct task
        conf={"scraped_data": "{{ ti.xcom_pull(task_ids='run_scrapers_task') | tojson }}"},
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS 
    )

    skip_store_scraped_data = PythonOperator(
        task_id="skip_store_scraped_data_task",
        python_callable=lambda: logging.info("Task executed: Skipping store DAG trigger."),
        trigger_rule=TriggerRule.ALL_SUCCESS 
    )

    # Define task dependencies
    run_scrapers_task >> branch_task >> [trigger_store_data, skip_store_scraped_data]