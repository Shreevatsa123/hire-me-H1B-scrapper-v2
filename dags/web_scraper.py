from airflow import DAG
from airflow.operators.python import PythonOperator
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
    )

    scrape_task
