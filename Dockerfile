FROM apache/airflow:2.7.3-python3.11

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    openjdk-17-jdk \
    postgresql-client \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set Java home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

# Copy requirements
COPY requirements.txt /requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt

# Create necessary directories
RUN mkdir -p /home/airflow/data/{raw,bronze,silver,gold} \
    /home/airflow/config \
    /home/airflow/pyspark \
    /home/airflow/scripts \
    /home/airflow/logs
