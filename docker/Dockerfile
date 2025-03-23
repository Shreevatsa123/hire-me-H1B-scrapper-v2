FROM apache/airflow:2.6.0

USER root
RUN apt-get update && apt-get install -y curl

USER airflow
# Copy the project root files into the container
COPY ../scraper /opt/airflow/scraper
COPY ../dags /opt/airflow/dags
COPY ../scraper/requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

ENTRYPOINT ["/bin/bash", "-c", "airflow db init && airflow webserver & airflow scheduler"]
