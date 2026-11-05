# Our own Airflow image = the official image + the 2 libraries our pipeline needs.
FROM apache/airflow:3.3.1-python3.12

COPY requirements-airflow.txt /requirements-airflow.txt

# Pin Airflow's own version so pip never upgrades or downgrades it by accident
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements-airflow.txt
