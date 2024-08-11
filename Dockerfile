FROM apache/airflow:2.3.4

# Install additional Python packages
# COPY requirements.txt /requirements.txt
RUN pip install logging
# Install additional Python packages
RUN pip install --no-cache-dir  requests pandas sqlalchemy psycopg2-binary psycopg2

# Install additional system packages
USER root
RUN apt-get update && apt-get install -y wget
USER airflow
