FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11

USER airflow
RUN pip install --no-cache-dir "boto3" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

USER root
COPY entrypoint.sh /opt/app/entrypoint.sh
COPY dags /opt/airflow/dags
RUN chmod 755 /opt/app/entrypoint.sh \
    && chown -R airflow:root /opt/airflow/dags /opt/app/entrypoint.sh

USER airflow
ENTRYPOINT ["/opt/app/entrypoint.sh"]
