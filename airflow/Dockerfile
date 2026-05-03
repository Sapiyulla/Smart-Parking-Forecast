FROM apache/airflow:slim-3.2.1-python3.11

USER root
# Устанавливаем libpq — она нужна для работы с Postgres даже в slim
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Устанавливаем готовые бинарные сборки (binary), чтобы не мучиться с gcc
RUN pip install --no-cache-dir \
    pyyaml \
    psycopg2-binary \
    asyncpg \
    apache-airflow-providers-fab \
    astronomer-cosmos \
    dbt-postgres \
    apache-airflow-providers-postgres
