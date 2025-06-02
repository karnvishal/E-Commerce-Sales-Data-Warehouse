FROM apache/airflow:2.5.1-python3.10 


USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    git \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    dbt-core==1.5.0 \
    dbt-bigquery==1.5.0 \
    && dbt --version