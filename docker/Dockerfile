# docker/Dockerfile

FROM apache/airflow:2.6.0

# Switch to root user to install dependencies
USER root

# Install system dependencies for Selenium and Chrome
RUN apt-get update && apt-get install -y \
    curl \
    sqlite3 \
    wget \
    unzip \
    # Add missing dependencies for Google Chrome
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libgbm1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libvulkan1 \
    libx11-6 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Install Google Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt-get install -y ./google-chrome-stable_current_amd64.deb
RUN rm google-chrome-stable_current_amd64.deb

# Switch back to airflow user
USER airflow

# Copy project files
COPY dags /opt/airflow/dags
COPY scraper /opt/airflow/scraper
COPY scraper/requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Ensure the SQLite database directory exists and is writable
USER root
RUN mkdir -p /opt/airflow/database && chown -R airflow /opt/airflow/database

# Switch back to airflow user
USER airflow

# Start Airflow services
ENTRYPOINT ["bash", "-c", "airflow db init && airflow webserver & airflow scheduler"]