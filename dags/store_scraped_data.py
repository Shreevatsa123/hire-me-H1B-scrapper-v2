import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3

DB_PATH = "/opt/airflow/database/jobs.db"

def init_db():
    """Initialize the SQLite database if it doesn't exist."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS job_postings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_posting TEXT,
            date_scrapped TEXT,
            job_posting_name TEXT,
            description TEXT,
            link TEXT
        )
    ''')
    conn.commit()
    conn.close()

def store_scraped_data(**kwargs):
    """Retrieve XCom data from dag_run.conf and store it in SQLite."""
    scraped_data_json = kwargs["dag_run"].conf.get("scraped_data", "[]")

    try:
        scraped_data = json.loads(scraped_data_json)  # Convert JSON string to list
    except json.JSONDecodeError:
        print("❌ Failed to parse scraped_data JSON.")
        return

    print("✅ Scraped Data from conf (parsed):", scraped_data)  # Debugging

    if not scraped_data or not isinstance(scraped_data, list):
        print("⚠️ No valid scraped data found.")
        return

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    for job in scraped_data:
        if not all(k in job for k in ["date_posting", "job_posting_name", "description", "link"]):
            print(f"⚠️ Skipping invalid job entry: {job}")
            continue

        cursor.execute('''
            INSERT OR IGNORE INTO job_postings (date_posting, date_scrapped, job_posting_name, description, link)
            VALUES (?, ?, ?, ?, ?)
        ''', (job['date_posting'], datetime.now().strftime('%Y-%m-%d'), job['job_posting_name'], job['description'], job['link']))

    conn.commit()
    conn.close()
    print("✅ Data successfully written to SQLite!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 23),
    'retries': 1,
}

dag = DAG(
    'store_scraped_data',
    default_args=default_args,
    schedule_interval=None,  # Triggered by another DAG
    catchup=False
)

init_db_task = PythonOperator(
    task_id='init_db',
    python_callable=init_db,
    dag=dag
)

store_data_task = PythonOperator(
    task_id='store_scraped_data',
    python_callable=store_scraped_data,
    dag=dag
)

init_db_task >> store_data_task
