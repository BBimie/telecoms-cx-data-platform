# ✅ MATCH THE VERSION
FROM apache/airflow:3.0.6

USER root
RUN apt-get update && \
    apt-get install -y git && \
    apt-get clean

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt