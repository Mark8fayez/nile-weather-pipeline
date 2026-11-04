-- Airflow keeps its own bookkeeping (DAG runs, task states, logs metadata)
-- in a separate database, so it never mixes with our business data.
CREATE DATABASE airflow;