FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    openjdk-11-jdk \
    postgresql-client \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Create necessary directories
RUN mkdir -p /home/airflow/data/{raw,bronze,silver,gold} \
    /home/airflow/config/great_expectations/{expectations,checkpoints,uncommitted/validations,uncommitted/data_docs/local_site,profilers,plugins} \
    /home/airflow/pyspark \
    /home/airflow/scripts \
    /home/airflow/logs
