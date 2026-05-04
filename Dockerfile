FROM apache/airflow:2.8.0

USER root
RUN apt-get update && apt-get install -y git && apt-get clean

USER airflow
RUN pip install dbt-bigquery==1.7.0