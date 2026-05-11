FROM apache/airflow:slim-3.2.1-python3.11

USER root

# Системные зависимости для LightGBM
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libgomp1 \
    build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Создаём все нужные директории
RUN mkdir -p /opt/airflow/ml/{config,model,data,logs} \
    && mkdir -p /opt/airflow/ml/model/{metrics,artifacts} \
    && chown -R airflow:root /opt/airflow/ml

USER airflow

# Установка Python-пакетов (добавлен asyncpg)
RUN pip install --no-cache-dir \
    apache-airflow \
    asyncpg \
    lightgbm \
    pandas \
    numpy \
    pyyaml \
    scikit-learn \
    sqlalchemy \
    psycopg2-binary \
    apache-airflow-providers-fab \
    astronomer-cosmos \
    dbt-postgres \
    apache-airflow-providers-postgres

# Копируем ваши файлы (если они на хосте)
COPY --chown=airflow:root ml /opt/airflow/ml/
COPY --chown=airflow:root airflow/dags /opt/airflow/dags/