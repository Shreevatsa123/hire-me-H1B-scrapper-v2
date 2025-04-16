# dags/store_scraped_data.py
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sqlite3
import logging # Import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Define database path (ensure this directory exists as per docker-compose volume mount)
DB_PATH = "/opt/airflow/database/jobs.db" 

def init_db():
    """Initialize the SQLite database and table if they don't exist."""
    try:
        # Ensure the directory exists (though volume mount should handle it)
        # os.makedirs(os.path.dirname(DB_PATH), exist_ok=True) # Might need os import
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        # Create table with an added UNIQUE constraint on the link column
        # This provides database-level uniqueness guarantee as well
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS job_postings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_posting TEXT,
                date_scrapped TEXT,
                job_posting_name TEXT,
                description TEXT,
                link TEXT UNIQUE NOT NULL 
            )
        ''')
        # Optional: Add an index for faster link lookups
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_job_postings_link ON job_postings (link);
        ''')
        conn.commit()
        conn.close()
        logging.info(f"Database {DB_PATH} initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"Database initialization error: {e}")
        raise # Re-raise the exception to fail the task if DB init fails

def store_scraped_data(**kwargs):
    """
    Retrieve XCom data, check for duplicates based on link, 
    and store only new job postings in SQLite.
    """
    scraped_data_json = kwargs["dag_run"].conf.get("scraped_data", "[]")
    new_jobs_added = 0
    jobs_skipped = 0

    try:
        scraped_data = json.loads(scraped_data_json)
    except json.JSONDecodeError as e:
        logging.error(f"❌ Failed to parse scraped_data JSON: {e}")
        # Fail the task if data cannot be parsed
        raise ValueError("Invalid JSON data received in dag_run.conf") 

    logging.info(f"Received {len(scraped_data)} potential job entries from conf.")
    # Log first few items for debugging if needed
    # logging.debug(f"Sample data: {scraped_data[:3]}") 

    if not scraped_data or not isinstance(scraped_data, list):
        logging.warning("⚠️ No valid scraped data list found in dag_run.conf.")
        return # Exit gracefully if no data

    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        for job in scraped_data:
            # Validate essential keys are present
            required_keys = ["date_posting", "job_posting_name", "description", "link"]
            if not all(k in job and job[k] is not None for k in required_keys):
                logging.warning(f"⚠️ Skipping invalid job entry (missing keys or null values): {job}")
                continue

            job_link = job['link']

            # --- Deduplication Check ---
            cursor.execute("SELECT 1 FROM job_postings WHERE link = ? LIMIT 1", (job_link,))
            exists = cursor.fetchone()
            # --- End Deduplication Check ---

            if exists:
                logging.info(f"Job link already exists, skipping: {job_link}")
                jobs_skipped += 1
            else:
                logging.info(f"Adding new job: {job.get('job_posting_name', 'N/A')} ({job_link})")
                try:
                    cursor.execute('''
                        INSERT INTO job_postings (date_posting, date_scrapped, job_posting_name, description, link)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        job['date_posting'], 
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), # Store timestamp too
                        job['job_posting_name'], 
                        job['description'], 
                        job_link
                        ))
                    new_jobs_added += 1
                except sqlite3.IntegrityError:
                    # Catch potential race condition if UNIQUE constraint is violated
                    logging.warning(f"IntegrityError: Job link likely added concurrently, skipping: {job_link}")
                    jobs_skipped += 1
                except sqlite3.Error as insert_err:
                     logging.error(f"Error inserting job {job_link}: {insert_err}")
                     # Decide if you want to skip or fail the task on insert error

        conn.commit()
        conn.close()
        logging.info(f"✅ Data processing complete. Added: {new_jobs_added} new jobs. Skipped: {jobs_skipped} duplicates.")

    except sqlite3.Error as e:
        logging.error(f"Database connection or operation error: {e}")
        # Fail the task if database operations fail
        raise 

# --- DAG Definition ---
# (Default args and DAG definition remain the same as your previous version)
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 3, 23), # Adjust start date if needed
    'retries': 1,
}

with DAG(
    'store_scraped_data',
    default_args=default_args,
    schedule_interval=None,  # Triggered by another DAG
    catchup=False,
    tags=['storage', 'sqlite', 'deduplication'] # Added tags
) as dag:

    init_db_task = PythonOperator(
        task_id='init_db',
        python_callable=init_db,
        # No changes needed here
    )

    store_data_task = PythonOperator(
        task_id='store_scraped_data',
        python_callable=store_scraped_data,
        # No changes needed here - function handles logic internally
    )

    init_db_task >> store_data_task
