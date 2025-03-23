import json
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/scraper')

from scraper import scrape_jobs

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 23),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def pass_xcom_to_store(**kwargs):
    """Decide whether to trigger store_scraped_data DAG based on job availability."""
    ti = kwargs["ti"]
    scraped_data = ti.xcom_pull(task_ids="scrape_jobs")
    if scraped_data:
        return "trigger_store_scraped_data"
    return "skip_store_scraped_data"

with DAG(
    "web_scraper_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    scrape_task = PythonOperator(
        task_id="scrape_jobs",
        python_callable=scrape_jobs,
        op_kwargs={"url": "https://careers.google.com"},
        do_xcom_push=True  # Push data to XCom
    )

    pass_xcom_task = BranchPythonOperator(
        task_id="pass_xcom_to_store",
        python_callable=pass_xcom_to_store,
        dag=dag
    )

    trigger_store_data = TriggerDagRunOperator(
        task_id="trigger_store_scraped_data",
        trigger_dag_id="store_scraped_data",
        conf={"scraped_data": "{{ ti.xcom_pull(task_ids='scrape_jobs') | tojson }}"},
        wait_for_completion=False,
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    skip_store_scraped_data = PythonOperator(
        task_id="skip_store_scraped_data",
        python_callable=lambda: print("⚠️ Skipping store DAG as no jobs found."),
        dag=dag,
    )

    scrape_task >> pass_xcom_task >> [trigger_store_data, skip_store_scraped_data]
