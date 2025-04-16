# dags/web_scraper.py
import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import sys
import os # Import os

# Ensure the scraper directory is in the Python path
# Adjust the path based on your Airflow execution environment if needed
scraper_path = '/opt/airflow/scraper' 
if scraper_path not in sys.path:
    sys.path.append(scraper_path)

# Import the new scraper function
try:
    from scraper import scrape_google_careers 
except ImportError as e:
     # Add more informative error logging if import fails
     print(f"Error importing scrape_google_careers from {scraper_path}. Ensure scraper.py exists and path is correct. Error: {e}")
     # Define a dummy function to prevent DAG parsing errors if import fails during parsing
     def scrape_google_careers(**kwargs):
         print("ERROR: scrape_google_careers function could not be imported!")
         raise ImportError("Dummy function called because import failed.")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 23), # Adjust start date if needed
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "provide_context": True # Ensure context is passed to python callables
}

def decide_trigger(**kwargs):
    """Decide whether to trigger store_scraped_data DAG based on job availability."""
    ti = kwargs["ti"]
    # Ensure the task_id matches the PythonOperator task_id below
    scraped_data = ti.xcom_pull(task_ids="scrape_google_jobs_task") 
    
    # Check if scraped_data is not None and is a non-empty list
    if scraped_data and isinstance(scraped_data, list) and len(scraped_data) > 0:
        print(f"Found {len(scraped_data)} jobs. Triggering store_scraped_data DAG.")
        return "trigger_store_scraped_data_task" # Task ID of TriggerDagRunOperator
    else:
        print("⚠️ No new jobs found or scraping failed. Skipping store DAG.")
        return "skip_store_scraped_data_task" # Task ID of the skip task

with DAG(
    "web_scraper_dag", # DAG ID remains the same
    default_args=default_args,
    schedule_interval="@daily", # Or your desired schedule
    catchup=False,
    tags=['scraping', 'google-careers'], # Add tags for better organization
) as dag:
    
    scrape_task = PythonOperator(
        # Changed task_id for clarity
        task_id="scrape_google_jobs_task", 
        # Use the new function
        python_callable=scrape_google_careers, 
        # op_kwargs can be used to pass specific keywords or max_pages if needed
        # op_kwargs={"keywords": ["data scientist"], "max_pages": 2}, 
        # Keep pushing results to XCom
        do_xcom_push=True  
    )

    branch_task = BranchPythonOperator(
        # Changed task_id for clarity
        task_id="decide_trigger_task", 
        python_callable=decide_trigger,
        # No provide_context=True needed here, handled by default_args
        # depends_on_past is False by default_args
    )

    trigger_store_data = TriggerDagRunOperator(
        # Changed task_id for clarity
        task_id="trigger_store_scraped_data_task",
        trigger_dag_id="store_scraped_data", # The DAG ID of the downstream DAG
        # Ensure the task_ids here matches the one pulling from XCom
        conf={"scraped_data": "{{ ti.xcom_pull(task_ids='scrape_google_jobs_task') | tojson }}"},
        wait_for_completion=False, # Usually False for triggering other DAGs
        # This task only runs if the branch decides to go this way
        trigger_rule=TriggerRule.ALL_SUCCESS 
    )

    skip_store_scraped_data = PythonOperator(
        # Changed task_id for clarity
        task_id="skip_store_scraped_data_task",
        python_callable=lambda: print("Task executed: Skipping store DAG trigger."),
        # This task only runs if the branch decides to go this way
        trigger_rule=TriggerRule.ALL_SUCCESS 
    )

    # Define task dependencies
    scrape_task >> branch_task 
    branch_task >> trigger_store_data
    branch_task >> skip_store_scraped_data