# dags/notify_data_added.py

import os
from datetime import datetime, timedelta
import json # <-- Import json library
import logging # <-- Import logging library

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
# Note: Importing send_email from airflow.utils.email is often unnecessary
# if you are only using the EmailOperator, but keep it if you use it elsewhere.
from airflow.utils.email import send_email

# Configure logging (Add this section if not already present)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__) # Use a named logger for consistency

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@example.com'], # Default FROM email if not set
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Configuration
RECIPIENT_EMAIL = os.getenv('SCRAPER_NOTIFICATION_EMAIL', 'recipient@example.com') # Ensure this env var is set or default is okay

def format_email_content(**context):
    """
    Retrieves scraped data from DAG run config, decodes JSON,
    formats it for email, and pushes content to XCom.
    """
    dag_run_conf = {}
    # Safely get dag_run configuration
    if context.get('dag_run') and hasattr(context['dag_run'], 'conf'):
        dag_run_conf = context['dag_run'].conf or {} # Ensure it's a dict

    raw_scraped_data = dag_run_conf.get('scraped_data', None) # Get string or None
    run_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')

    scraped_data = [] # Default to empty list
    if isinstance(raw_scraped_data, str): # Check if it's a string
        log.info("Received scraped_data string from conf.")
        try:
            # --- CORRECTED JSON LOADING ---
            # Load directly, expecting valid JSON now (due to | tojson filter in sender)
            scraped_data = json.loads(raw_scraped_data)
            log.info(f"Successfully decoded JSON string into list with {len(scraped_data)} items.")

        except json.JSONDecodeError as e:
            # Log the error and the problematic string
            log.error(f"JSONDecodeError: Could not decode scraped_data string. Error: {e}", exc_info=True)
            log.error(f"Received string: {raw_scraped_data}")
            scraped_data = [] # Fallback to empty list on error
        except Exception as e:
             log.error(f"An unexpected error occurred processing scraped_data from conf: {e}", exc_info=True)
             log.error(f"Data string was: {raw_scraped_data}")
             scraped_data = [] # Reset to empty list on error
    elif isinstance(raw_scraped_data, list):
        # Fallback if data somehow wasn't JSON encoded (shouldn't happen with tojson)
        log.warning("Received data as list directly, not JSON string.")
        scraped_data = raw_scraped_data
    else:
         # Handle None or other unexpected types
         log.warning(f"Received unexpected data type or None for scraped_data: {type(raw_scraped_data)}")
         scraped_data = []

    # --- Proceed with formatting using the 'scraped_data' list ---
    if not scraped_data:
        subject = f"H1B Scraper: No New Jobs Found ({run_timestamp})"
        html_content = f"<p>No new job data found in this run (at {run_timestamp}), or data could not be processed.</p>"
        log.info("Setting email body to indicate no data.")
    else:
        # --- Format the actual data ---
        subject = f"H1B Scraper: {len(scraped_data)} New Jobs Found ({run_timestamp})"
        log.info(f"Formatting email body for {len(scraped_data)} jobs.")

        # Build HTML table
        html_content = f"""
        <h3>H1B Job Scraper Run at {run_timestamp}</h3>
        <p>Found {len(scraped_data)} new job postings:</p>
        <table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse; border: 1px solid #ccc;'>
            <thead style='background-color: #f2f2f2;'>
                <tr>
                    <th>Title</th>
                    <th>Company</th>
                    <th>Location</th>
                    <th>URL</th>
                </tr>
            </thead>
            <tbody>
        """

        for job in scraped_data:
            # Add extra check if job is actually a dictionary
            if isinstance(job, dict):
                 # Use .get() for safety, provide default values
                 title = job.get('job_posting_name', 'N/A')
                 company = job.get('company', 'N/A')
                 location = job.get('location', 'N/A')
                 # description = job.get('description_snippet', 'N/A') # Uncomment if using
                 link = job.get('link', '#')
                 # visa = job.get('visa_sponsored', 'N/A') # Uncomment if using

                 # Basic HTML escaping for safety
                 title = title.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                 company = company.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                 location = location.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                 # description = description.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;') # Uncomment if using
                 link_display = link if len(link) < 70 else link[:67] + '...' # Shorten long links for display
                 link_display = link_display.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
                 link = link.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')

                 html_content += f"""
                    <tr>
                        <td>{title}</td>
                        <td>{company}</td>
                        <td>{location}</td>
                        <td><a href="{link}" target="_blank">{link_display}</a></td>
                    </tr>
                """
            else:
                 log.warning(f"Encountered invalid job data item (not a dict): {job}")
                 html_content += f"""
                    <tr>
                        <td colspan="4">Error: Encountered invalid job data item.</td>
                    </tr>
                """

        html_content += """
            </tbody>
        </table>
        <br/>
        """
        # Add note about storage only if the store DAG is reliably triggered
        # html_content += "<p>Note: This data has been sent for storage in the database.</p>"

    # Push the subject and content to XCom for the EmailOperator
    log.info("Pushing formatted email content to XCom keys 'email_subject' and 'email_html_content'")
    context['ti'].xcom_push(key='email_subject', value=subject)
    context['ti'].xcom_push(key='email_html_content', value=html_content)

    return None # Return value not needed if EmailOperator pulls from XCom


with DAG(
    dag_id='notify_data_added_dag',
    default_args=default_args,
    description='Sends email notification with newly scraped job data.',
    schedule_interval=None,  # Triggered externally
    start_date=datetime(2024, 4, 16), # Adjust start date as needed
    catchup=False,
    tags=['scraper', 'notification'],
) as dag:

    format_email_task = PythonOperator(
        task_id='format_email_content',
        python_callable=format_email_content,
        # provide_context=True is default and deprecated, not needed explicitly
    )

    send_email_notification = EmailOperator(
        task_id='send_email_notification',
        to=RECIPIENT_EMAIL,
        subject="{{ ti.xcom_pull(task_ids='format_email_content', key='email_subject') | default('H1B Job Alert: Subject Error') }}",
        html_content="{{ ti.xcom_pull(task_ids='format_email_content', key='email_html_content') | default('<p>Error retrieving email content.</p>') }}",
    )

    format_email_task >> send_email_notification