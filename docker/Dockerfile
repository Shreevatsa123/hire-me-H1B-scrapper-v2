FROM apache/airflow:2.6.0

# Switch to root user to install dependencies
USER root
RUN apt-get update && apt-get install -y curl sqlite3 && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user
USER airflow

# Copy project files (relative to build context)
COPY dags /opt/airflow/dags
COPY scraper /opt/airflow/scraper
COPY scraper/requirements.txt /requirements.txt

# Install dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure the SQLite database directory exists and is writable
USER root
RUN mkdir -p /opt/airflow/database && chown -R airflow /opt/airflow/database

# Switch back to airflow user
USER airflow

# Start Airflow services
ENTRYPOINT ["bash", "-c", "airflow db init && airflow webserver & airflow scheduler"]
